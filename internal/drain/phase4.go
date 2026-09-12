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

// pollPageSize bounds each page of the per-node poll.
const pollPageSize = 500

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
			// Which code this is depends on what is actually left.
			//
			// Exit 3 is the only code §9 tells a wrapper it may retry on a
			// timer, and that is only honest for Job pods, which finish on
			// their own. Anything else on the node will still be there on the
			// next attempt — a pod with spec.nodeName set bypasses the cordon,
			// and an unmanaged pod created mid-drain is nobody's to reap — so
			// reporting 3 would send a wrapper into a loop that burns the full
			// deadline each time and never converges.
			if len(remaining) > 0 {
				return e.blockedDiagnostic(node, remaining, jobs, drainStart)
			}
			return e.jobDeadlineDiagnostic(node, jobs, drainStart)
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
	// Paginated. A single Limit-bounded page is not the same as "the pods on
	// this node": a node that has accumulated Succeeded Job pods — common
	// wherever nothing sets a TTL on them — can exceed one page, and if the
	// first page happens to filter out entirely, this would report an empty
	// node and declare the drain complete with workload still running. §9's
	// whole point is that the tool is honest about an incomplete drain.
	var items []corev1.Pod
	opts := metav1.ListOptions{
		FieldSelector: fields.OneTermEqualSelector("spec.nodeName", node).String(),
		Limit:         pollPageSize,
	}
	for {
		list, err := e.client.CoreV1().Pods(metav1.NamespaceAll).List(ctx, opts)
		if err != nil {
			return nil, nil, fmt.Errorf("listing pods on %s: %w", node, err)
		}
		items = append(items, list.Items...)
		if list.Continue == "" {
			break
		}
		opts.Continue = list.Continue
	}

	for i := range items {
		p := &items[i]
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
func (e *Engine) jobDeadlineDiagnostic(node string, jobs []*corev1.Pod, drainStart time.Time) error {
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

// blockedDiagnostic reports pods that are neither evictable nor Job pods and
// are still on the node at the deadline.
//
// Separate from the Job path because the remedy is different and so is the exit
// code. These pods did not arrive through the scheduler — cordon does not stop
// a pod that names its node directly — so waiting longer achieves nothing and
// a re-run finds them exactly where they were. This needs a human.
func (e *Engine) blockedDiagnostic(node string, blockers, jobs []*corev1.Pod, drainStart time.Time) error {
	elapsed := time.Since(drainStart).Round(time.Second)

	detail := []string{
		fmt.Sprintf("%d pod(s) remain on %s that the drain cannot remove:", len(blockers), node),
		"",
	}
	sort.Slice(blockers, func(i, j int) bool { return blockers[i].Name < blockers[j].Name })
	namespaces := map[string]bool{}
	for _, p := range blockers {
		age := time.Since(p.CreationTimestamp.Time).Round(time.Second)
		detail = append(detail, fmt.Sprintf("  %-14s %-34s age %s", p.Namespace, p.Name, age))
		namespaces[p.Namespace] = true
	}
	detail = append(detail,
		"",
		"These were not in scope when the drain started, so they were never",
		"evicted. A pod that sets spec.nodeName bypasses the cordon entirely, and",
		"nothing recreates an unmanaged pod once it is gone.",
		"",
		"Re-running will not help: they will still be here. Remove or reschedule",
		"them, then re-run.",
		"",
		"Namespaces affected: "+strings.Join(sortedKeys(namespaces), ", "),
		"The node remains cordoned.",
	)
	if len(jobs) > 0 {
		detail = append(detail, "",
			fmt.Sprintf("(%d Job pod(s) are also still running; those would have finished on their own.)", len(jobs)))
	}

	suggested := make([]string, 0, len(blockers))
	for _, p := range blockers {
		suggested = append(suggested, fmt.Sprintf("kubectl describe pod -n %s %s", p.Namespace, p.Name))
		if len(suggested) == 3 {
			break
		}
	}

	e.rec.Diagnostic(output.Diagnostic{
		Node:      node,
		Headline:  fmt.Sprintf("%d pod(s) cannot be drained from %s (elapsed %s)", len(blockers), node, elapsed),
		Detail:    detail,
		Suggested: suggested,
	})
	// Deliberately not JobTimeout: a wrapper must not retry this on a timer.
	return exitcode.Wrap(exitcode.Error,
		fmt.Errorf("%d pod(s) on %s cannot be drained", len(blockers), node))
}
