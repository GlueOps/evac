package drain

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/GlueOps/evac/internal/exitcode"
	"github.com/GlueOps/evac/internal/output"
)

// pdbStallDiagnostic reports an eviction blocked past its timeout.
//
// The blocking PDB is named with its current numbers. Without those the
// operator sees "stuck" and has to go find it by hand, which is the runbook
// this tool exists to replace.
func (e *Engine) pdbStallDiagnostic(ctx context.Context, node string, pod *corev1.Pod) error {
	facts := map[string]string{}
	detail := []string{fmt.Sprintf("%s/%s on %s", pod.Namespace, pod.Name, node)}

	if pdb := e.blockingPDB(ctx, pod); pdb != nil {
		detail = append(detail, fmt.Sprintf("  blocked by PDB %s/%s", pdb.Namespace, pdb.Name))
		facts["disruptionsAllowed"] = fmt.Sprint(pdb.Status.DisruptionsAllowed)
		facts["currentHealthy"] = fmt.Sprint(pdb.Status.CurrentHealthy)
		facts["desiredHealthy"] = fmt.Sprint(pdb.Status.DesiredHealthy)

		// The three numbers alone describe two opposite situations that call
		// for opposite responses, and rendering them identically leaves the
		// reading to an operator at 2am. currentHealthy below desiredHealthy
		// means replicas are still coming back and waiting genuinely works;
		// at or above it with no disruptions allowed, the budget cannot be
		// satisfied at this replica count and will never clear on its own —
		// so the re-run this block suggests would stall for another full
		// timeout and fail in exactly the same way.
		suggested := []string{
			fmt.Sprintf("kubectl describe pdb -n %s %s", pdb.Namespace, pdb.Name),
			fmt.Sprintf("kubectl get pods -n %s -o wide", pod.Namespace),
		}
		if pdb.Status.DisruptionsAllowed == 0 && pdb.Status.CurrentHealthy >= pdb.Status.DesiredHealthy {
			detail = append(detail,
				"  this budget cannot allow a disruption at the current replica count",
				fmt.Sprintf("  waiting will not clear it, and a re-run stalls for another %s", e.opts.EvictionTimeout),
				"  raise replicas or relax the budget first, then re-run")
			suggested = append(suggested,
				fmt.Sprintf("kubectl patch pdb -n %s %s --type=merge -p '{\"spec\":{\"minAvailable\":%d}}'",
					pdb.Namespace, pdb.Name, maxInt(int(pdb.Status.DesiredHealthy)-1, 0)))
		}
		detail = append(detail, fmt.Sprintf("  %s remains cordoned", node))

		e.rec.Diagnostic(output.Diagnostic{
			Node:      node,
			Headline:  fmt.Sprintf("eviction timeout after %s", e.opts.EvictionTimeout),
			Detail:    detail,
			Facts:     facts,
			Suggested: suggested,
			Rerun:     e.rerun,
		})
	} else {
		e.rec.Diagnostic(output.Diagnostic{
			Node:     node,
			Headline: fmt.Sprintf("eviction timeout after %s", e.opts.EvictionTimeout),
			Detail: append(detail,
				"  the eviction API kept returning 429 but no matching PDB could be read back",
				fmt.Sprintf("  %s remains cordoned", node)),
			Suggested: []string{
				fmt.Sprintf("kubectl get pdb -n %s", pod.Namespace),
				fmt.Sprintf("kubectl describe pod -n %s %s", pod.Namespace, pod.Name),
			},
			Rerun: e.rerun,
		})
	}

	return exitcode.Wrap(exitcode.EvictionTimeout,
		fmt.Errorf("eviction of %s/%s timed out after %s", pod.Namespace, pod.Name, e.opts.EvictionTimeout))
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// blockingPDB finds the PDB refusing this pod, re-read live so the numbers in
// the message are current rather than from the opening snapshot.
func (e *Engine) blockingPDB(ctx context.Context, pod *corev1.Pod) *policyv1.PodDisruptionBudget {
	list, err := e.client.PolicyV1().PodDisruptionBudgets(pod.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil
	}
	for i := range list.Items {
		pdb := &list.Items[i]
		sel, err := metav1.LabelSelectorAsSelector(pdb.Spec.Selector)
		if err != nil || !sel.Matches(labels.Set(pod.Labels)) {
			continue
		}
		if pdb.Status.DisruptionsAllowed == 0 {
			return pdb
		}
	}
	return nil
}

// terminationTimeoutDiagnostic reports a pod that was evicted but never went
// away.
//
// On a NotReady node this is expected: the kubelet has stopped reporting, so
// eviction sets a deletion timestamp but nothing terminates the container. The
// tool must not force-delete these automatically. Force-delete removes the pod
// object without the kubelet confirming the container is dead — fine if the
// node is genuinely down, catastrophic if it is network-partitioned but still
// running, because the controller immediately starts a second copy. The tool
// cannot distinguish "dead" from "unreachable", so it names both options and
// lets the operator choose.
func (e *Engine) terminationTimeoutDiagnostic(ctx context.Context, node string, pod *corev1.Pod) error {
	notReady := e.nodeNotReady(ctx, node)

	headline := fmt.Sprintf("eviction timeout after %s", e.opts.EvictionTimeout)
	if notReady {
		headline += fmt.Sprintf(" — %s is NotReady", node)
	}

	detail := []string{
		fmt.Sprintf("%s/%s is still present after being evicted.", pod.Namespace, pod.Name),
	}
	var suggested []string

	if notReady {
		detail = append(detail,
			"",
			"The kubelet is not confirming termination.",
			"",
			"Confirm whether the node is actually down, then either:",
			"",
			"  # node is dead — force-remove the pod object",
			fmt.Sprintf("  kubectl delete pod -n %s %s --force --grace-period=0", pod.Namespace, pod.Name),
			"",
			"  # node is not coming back — delete the Node object and let pod GC clean up",
			fmt.Sprintf("  kubectl delete node %s", node),
			"",
			"Do NOT force-delete if the node may still be running: the container keeps",
			"running and the controller will start a second copy.",
		)
	} else {
		suggested = []string{
			fmt.Sprintf("kubectl describe pod -n %s %s", pod.Namespace, pod.Name),
			fmt.Sprintf("kubectl get events -n %s --field-selector involvedObject.name=%s", pod.Namespace, pod.Name),
		}
	}

	e.rec.Diagnostic(output.Diagnostic{
		Node:      node,
		Headline:  headline,
		Detail:    append(detail, "", fmt.Sprintf("%s remains cordoned.", node)),
		Suggested: suggested,
		Rerun:     e.rerun,
	})
	return exitcode.Wrap(exitcode.EvictionTimeout,
		fmt.Errorf("pod %s/%s did not terminate within %s", pod.Namespace, pod.Name, e.opts.EvictionTimeout))
}

func (e *Engine) nodeNotReady(ctx context.Context, name string) bool {
	n, err := e.client.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return false
	}
	for i := range n.Status.Conditions {
		c := &n.Status.Conditions[i]
		if c.Type == corev1.NodeReady {
			return c.Status != corev1.ConditionTrue
		}
	}
	return false
}
