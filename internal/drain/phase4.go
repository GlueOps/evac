package drain

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"

	"github.com/GlueOps/evac/internal/exitcode"
	"github.com/GlueOps/evac/internal/output"
)

// phase4 waits for the node to empty.
//
// Job and CronJob pods are never evicted — they finish on their own, and
// cordoning means no new ones land. "On their own" could be minutes or hours,
// so the wait is bounded.
func (e *Engine) phase4(ctx context.Context, node string, drainStart time.Time) error {
	deadline := time.Now().Add(e.opts.JobDeadline)

	for {
		remaining, jobs, err := e.pollNode(ctx, node)
		if err != nil {
			return exitcode.Wrap(exitcode.Error, err)
		}
		if len(remaining) == 0 && len(jobs) == 0 {
			e.rec.Infof("4", node, "no evictable or Job pods remain")
			return nil
		}

		if time.Now().After(deadline) {
			return e.jobDeadlineDiagnostic(node, jobs, remaining, drainStart)
		}

		if len(jobs) > 0 {
			e.rec.Progress(fmt.Sprintf("  waiting on %d Job pod(s) on %s  (%s remaining)",
				len(jobs), node, time.Until(deadline).Round(time.Second)))
		} else {
			e.rec.Progress(fmt.Sprintf("  waiting for %d pod(s) to clear %s", len(remaining), node))
		}

		select {
		case <-ctx.Done():
			return exitcode.Wrap(exitcode.Interrupted, ctx.Err())
		case <-time.After(e.opts.PollInterval):
		}
	}
}

// pollNode re-reads what is actually on the node right now.
//
// This is a poll rather than a single evaluation for a specific reason.
// Deleting a local-path PVC spawns a short-lived helper pod on the node to
// remove the directory, and the provisioner sets spec.nodeName directly, so
// cordon does not block it. Those pods appear mid-drain and exit within
// seconds; a one-shot check would fail spuriously on them, while a poll absorbs
// them naturally. They are excluded from the blocker count outright since they
// are storage cleanup rather than workload.
//
// This is the one place a per-node field selector is used, and it is
// deliberate: it is a bounded poll of a single node during execution, not the
// inventory sweep §3's rule is about.
func (e *Engine) pollNode(ctx context.Context, node string) (blockers []*corev1.Pod, jobs []*corev1.Pod, err error) {
	list, err := e.client.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{
		FieldSelector: fields.OneTermEqualSelector("spec.nodeName", node).String(),
		Limit:         500,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("listing pods on %s: %w", node, err)
	}

	for i := range list.Items {
		p := &list.Items[i]
		if p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
			continue
		}
		if isDaemonSetPod(p) || isMirrorPod(p) || isStorageHelperPod(p) {
			continue
		}
		if isJobPod(p) {
			jobs = append(jobs, p)
			continue
		}
		blockers = append(blockers, p)
	}
	return blockers, jobs, nil
}

func isDaemonSetPod(p *corev1.Pod) bool { return ownerKind(p) == "DaemonSet" }
func isJobPod(p *corev1.Pod) bool       { return ownerKind(p) == "Job" }

func isMirrorPod(p *corev1.Pod) bool {
	_, ok := p.Annotations[corev1.MirrorPodAnnotationKey]
	return ok
}

// isStorageHelperPod mirrors the classifier's rule, kept in step with
// classify.isStorageHelper.
func isStorageHelperPod(p *corev1.Pod) bool {
	return strings.HasPrefix(p.Name, "helper-pod-") &&
		ownerKind(p) == "" &&
		p.Spec.RestartPolicy == corev1.RestartPolicyNever
}

func ownerKind(p *corev1.Pod) string {
	for i := range p.OwnerReferences {
		ref := &p.OwnerReferences[i]
		if ref.Controller != nil && *ref.Controller {
			return ref.Kind
		}
	}
	return ""
}

// jobDeadlineDiagnostic renders the §5 phase 4 expiry block.
func (e *Engine) jobDeadlineDiagnostic(node string, jobs, blockers []*corev1.Pod, drainStart time.Time) error {
	elapsed := time.Since(drainStart).Round(time.Second)

	detail := []string{
		fmt.Sprintf("%d Job pod(s) still running on %s:", len(jobs), node),
		"",
	}
	// Per-pod age is what tells the operator whether something is nearly done
	// or wedged.
	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].CreationTimestamp.Before(&jobs[j].CreationTimestamp)
	})
	namespaces := map[string]bool{}
	for _, p := range jobs {
		age := time.Since(p.CreationTimestamp.Time).Round(time.Minute)
		detail = append(detail, fmt.Sprintf("  %-14s %-32s %-22s %s", p.Namespace, p.Name, node, age))
		namespaces[p.Namespace] = true
	}
	if len(blockers) > 0 {
		detail = append(detail, "", fmt.Sprintf("%d non-Job pod(s) also remain:", len(blockers)))
		for _, p := range blockers {
			detail = append(detail, fmt.Sprintf("  %s/%s", p.Namespace, p.Name))
		}
	}
	detail = append(detail,
		"",
		"Namespaces affected: "+strings.Join(sortedKeys(namespaces), ", "),
		"All other pods drained successfully. Nodes remain cordoned.",
	)

	e.rec.Diagnostic(output.Diagnostic{
		Node:     node,
		Headline: fmt.Sprintf("job wait deadline exceeded after %s (drain elapsed %s)", e.opts.JobDeadline, elapsed),
		Detail:   detail,
		Rerun:    e.rerun,
	})
	return exitcode.Wrap(exitcode.JobTimeout,
		fmt.Errorf("job wait deadline exceeded on %s", node))
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
