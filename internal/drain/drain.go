// Package drain executes §5's phases.
//
// The eviction call here is the policy/v1 subresource directly, not
// k8s.io/kubectl/pkg/drain. That library's unit of work is a node-scoped batch
// — it spawns a goroutine per pod, owns its own retry loop and then waits
// across the whole set — which cannot express phase 2's requirement to delete a
// PVC, evict one pod, and wait for that pod object to disappear before touching
// the next. See §10.
package drain

import (
	"context"
	"errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	"github.com/GlueOps/evac/internal/classify"
	"github.com/GlueOps/evac/internal/exitcode"
	"github.com/GlueOps/evac/internal/output"
	"github.com/GlueOps/evac/internal/scope"
)

// Options configure a drain run.
type Options struct {
	// EvictionTimeout bounds phase 2 per pod, from the first eviction attempt
	// to the pod object actually being gone.
	EvictionTimeout time.Duration
	// JobDeadline bounds phase 4 per node.
	JobDeadline time.Duration
	// PVCTimeout bounds how long a PVC may stay Terminating.
	PVCTimeout time.Duration
	// Parallel is how many nodes to drain concurrently. Zero or one means
	// serial, which is the default: the first node reveals whether the capacity
	// math was right, and a mistake stops after one node instead of all of them.
	Parallel int
	// PollInterval is how often waits re-check.
	PollInterval time.Duration
}

// Defaults returns the spec's values.
func Defaults() Options {
	return Options{
		EvictionTimeout: 10 * time.Minute,
		JobDeadline:     30 * time.Minute,
		PVCTimeout:      5 * time.Minute,
		Parallel:        1,
		PollInterval:    2 * time.Second,
	}
}

// Engine runs a drain.
type Engine struct {
	client kubernetes.Interface
	rec    *output.Recorder
	opts   Options
	scope  *scope.Scope
	// rerun is the command to suggest in failure diagnostics.
	rerun string
}

// New builds an Engine.
func New(client kubernetes.Interface, rec *output.Recorder, sc *scope.Scope, opts Options, rerun string) *Engine {
	return &Engine{client: client, rec: rec, opts: opts, scope: sc, rerun: rerun}
}

// NodeResult is the outcome for one node.
type NodeResult struct {
	Node string
	Code exitcode.Code
	Err  error
}

// Result is the aggregate outcome.
type Result struct {
	Nodes []NodeResult
}

// Code combines the per-node codes using §9's precedence.
func (r Result) Code() exitcode.Code {
	codes := make([]exitcode.Code, 0, len(r.Nodes))
	for _, n := range r.Nodes {
		codes = append(codes, n.Code)
	}
	return exitcode.Combine(codes...)
}

// Failed reports whether any node failed.
func (r Result) Failed() bool { return r.Code() != exitcode.OK }

// Run executes phases 1 through 4.
func (e *Engine) Run(ctx context.Context) (Result, error) {
	// Phase 1 covers every selected node before any eviction, regardless of
	// parallelism. Cordoning node by node would let pods evicted from the first
	// node land on a selected-but-not-yet-cordoned one and be moved twice.
	if err := e.cordonAll(ctx); err != nil {
		return Result{}, err
	}

	if e.opts.Parallel <= 1 {
		return e.runSerial(ctx), nil
	}
	return e.runParallel(ctx), nil
}

// --- phase 1 ---------------------------------------------------------------

// cordonAll marks every selected node unschedulable.
//
// This runs before classification is used, not after. Cordoning is a safe
// mutation and it freezes the picture: with all targets unschedulable, the pod
// set on those nodes stops changing, so the classification is accurate when it
// is acted on rather than stale by the time eviction starts.
func (e *Engine) cordonAll(ctx context.Context) error {
	for i := range e.scope.Nodes {
		n := &e.scope.Nodes[i]
		if !n.Schedulable {
			e.rec.Infof("1", n.Name, "already cordoned")
			continue
		}
		if err := e.cordon(ctx, n.Name); err != nil {
			return exitcode.Wrap(exitcode.Error, fmt.Errorf("cordoning %s: %w", n.Name, err))
		}
		e.rec.Infof("1", n.Name, "cordoned")
	}
	return nil
}

// cordon patches spec.unschedulable. A merge patch touches only that field, so
// it cannot clobber a concurrent change to anything else on the node.
func (e *Engine) cordon(ctx context.Context, name string) error {
	patch := []byte(`{"spec":{"unschedulable":true}}`)
	_, err := e.client.CoreV1().Nodes().Patch(ctx, name, types.StrategicMergePatchType, patch, metav1.PatchOptions{})
	return err
}

// --- orchestration ---------------------------------------------------------

func (e *Engine) runSerial(ctx context.Context) Result {
	var res Result
	for i := range e.scope.Nodes {
		name := e.scope.Nodes[i].Name
		err := e.drainNode(ctx, name)
		res.Nodes = append(res.Nodes, NodeResult{Node: name, Code: exitcode.Of(err), Err: err})
		if err != nil {
			// Serial is the safe default precisely so a mistake stops after one
			// node. Nodes stay cordoned, so a re-run picks up where this left off.
			break
		}
	}
	return res
}

func (e *Engine) runParallel(ctx context.Context) Result {
	workers := e.opts.Parallel
	if workers > len(e.scope.Nodes) {
		workers = len(e.scope.Nodes)
	}

	type job struct{ name string }
	jobs := make(chan job)
	results := make(chan NodeResult, len(e.scope.Nodes))

	for range workers {
		go func() {
			for j := range jobs {
				err := e.drainNode(ctx, j.name)
				results <- NodeResult{Node: j.name, Code: exitcode.Of(err), Err: err}
			}
		}()
	}
	go func() {
		for i := range e.scope.Nodes {
			jobs <- job{name: e.scope.Nodes[i].Name}
		}
		close(jobs)
	}()

	// One worker failing must not abandon in-flight work on other nodes: let
	// them finish the node they are on, then report the aggregate.
	var res Result
	for range e.scope.Nodes {
		res.Nodes = append(res.Nodes, <-results)
	}
	return res
}

// drainNode runs phases 2 through 4 for one node.
func (e *Engine) drainNode(ctx context.Context, node string) error {
	start := time.Now()

	if err := e.phase2(ctx, node); err != nil {
		return err
	}
	if err := e.phase3(ctx, node); err != nil {
		return err
	}
	if err := e.phase4(ctx, node, start); err != nil {
		return err
	}

	e.rec.Event(output.Event{
		Phase: "4", Node: node, Msg: "node drained", Duration: time.Since(start),
	})
	return nil
}

// podsOn returns the in-scope pods for one node in a given class.
func (e *Engine) podsOn(node string, class classify.Class) []classify.Result {
	var out []classify.Result
	for _, r := range e.scope.Pods {
		if r.Pod.Spec.NodeName == node && r.Class == class {
			out = append(out, r)
		}
	}
	return out
}

// pvcsFor returns the in-scope PVC targets belonging to one pod.
func (e *Engine) pvcsFor(pod *corev1.Pod) []scope.PVCTarget {
	var out []scope.PVCTarget
	for _, t := range e.scope.PVCs {
		if t.Pod.Namespace == pod.Namespace && t.Pod.Name == pod.Name {
			out = append(out, t)
		}
	}
	return out
}

// --- phase 2: normal pods --------------------------------------------------

func (e *Engine) phase2(ctx context.Context, node string) error {
	pods := e.podsOn(node, classify.Normal)
	if len(pods) == 0 {
		return nil
	}
	e.rec.Infof("2", node, "evicting %d normal pod(s)", len(pods))

	for _, r := range pods {
		if err := e.movePod(ctx, "2", node, r, false); err != nil {
			return err
		}
	}
	return nil
}

// --- phase 3: fragile pods -------------------------------------------------

func (e *Engine) phase3(ctx context.Context, node string) error {
	pods := e.podsOn(node, classify.Fragile)
	if len(pods) == 0 {
		return nil
	}
	e.rec.Infof("3", node, "deleting %d fragile pod(s) directly (eviction would be refused)", len(pods))

	for _, r := range pods {
		if err := e.movePod(ctx, "3", node, r, true); err != nil {
			return err
		}
	}
	return nil
}

// movePod implements the §5 ordering for one pod.
//
//  1. Delete PVC          → sets deletionTimestamp; pvc-protection holds it
//  2. Evict (or delete) the pod
//  3. Wait: pod object fully GONE, not merely Terminating
//  4. Verify the PVC actually went away
//  5. Wait: the workload is back to its desired replica count
//
// The order is the whole point. Deleting a PVC does not remove it — it marks
// it, and the pvc-protection finalizer holds it while any pod still references
// it. With the intuitive order (evict, then delete the PVC) the controller can
// recreate the pod before the PVC is gone; the new pod binds to a healthy PVC
// whose PV has nodeAffinity to the node just cordoned, goes Pending forever,
// and now holds the finalizer on the PVC being deleted. Marking the PVC first
// means the deletionTimestamp is already set when the replacement appears, so
// it cannot cleanly bind and the controller waits instead of wedging.
func (e *Engine) movePod(ctx context.Context, phase, node string, r classify.Result, direct bool) error {
	pod := r.Pod
	targets := e.pvcsFor(pod)

	// Step 1.
	for _, t := range targets {
		if err := e.deletePVC(ctx, phase, node, t); err != nil {
			return err
		}
	}

	// Step 2.
	start := time.Now()
	if direct {
		e.rec.Event(output.Event{
			Phase: phase, Node: node, Namespace: pod.Namespace,
			Kind: "pod", Name: pod.Name, Msg: "deleting",
		})
		if err := e.deletePod(ctx, pod); err != nil {
			return exitcode.Wrap(exitcode.Error, fmt.Errorf("deleting %s/%s: %w", pod.Namespace, pod.Name, err))
		}
	} else {
		e.rec.Event(output.Event{
			Phase: phase, Node: node, Namespace: pod.Namespace,
			Kind: "pod", Name: pod.Name, Msg: "evicting",
		})
		if err := e.evictWithRetry(ctx, phase, node, pod); err != nil {
			return err
		}
	}

	// Step 3.
	if err := e.waitPodGone(ctx, phase, node, pod, start); err != nil {
		return err
	}
	e.rec.Event(output.Event{
		Phase: phase, Node: node, Namespace: pod.Namespace,
		Msg: "pod terminated", Duration: time.Since(start),
	})

	// Step 4.
	for _, t := range targets {
		if !t.Deletable(e.scope.Provider.Disabled) {
			continue
		}
		if err := e.waitPVCGone(ctx, phase, node, t); err != nil {
			return err
		}
	}

	// Step 5.
	return e.waitWorkloadReady(ctx, phase, node, r)
}

// deletePVC marks a claim for deletion, honouring both guards.
func (e *Engine) deletePVC(ctx context.Context, phase, node string, t scope.PVCTarget) error {
	if e.scope.Provider.Disabled {
		return nil // reported once, in the plan
	}
	if !t.Decision.Allowed {
		e.rec.Event(output.Event{
			Phase: phase, Node: node, Namespace: t.PVC.Namespace,
			Kind: "pvc", Name: t.PVC.Name, Msg: "keeping",
			Attrs: map[string]string{"reason": t.Decision.Reason},
		})
		return nil
	}
	if t.AlreadyDeleting {
		// §7: convergent. A PVC already carrying a deletionTimestamp is
		// unfinished work from an earlier run; re-issuing the delete achieves
		// nothing.
		e.rec.Event(output.Event{
			Phase: phase, Node: node, Namespace: t.PVC.Namespace,
			Kind: "pvc", Name: t.PVC.Name, Msg: "already marked for deletion, skipping",
		})
		return nil
	}

	e.rec.Event(output.Event{
		Phase: phase, Node: node, Namespace: t.PVC.Namespace,
		Kind: "pvc", Name: t.PVC.Name, Msg: "deleting",
		Attrs: map[string]string{"source": t.Decision.Source},
	})

	// Deleted by UID, not by name.
	//
	// The guard's verdict (§5) was computed against the object in the snapshot,
	// and the snapshot is taken before the plan is rendered — so an operator
	// reading the plan, plus the drain itself, can put minutes between the
	// decision and the deletion. In that window a claim can be deleted and
	// recreated under the same name backed by something entirely different.
	// Deleting by name would then destroy a volume the allowlist was never
	// shown, which is the one outcome the allowlist exists to prevent.
	//
	// UID alone, deliberately: resourceVersion changes on every status or
	// annotation write, so pinning it would produce constant spurious
	// conflicts. UID is stable for an object's lifetime and changes only on
	// delete-and-recreate — exactly the event that voids the verdict.
	err := e.client.CoreV1().PersistentVolumeClaims(t.PVC.Namespace).
		Delete(ctx, t.PVC.Name, metav1.DeleteOptions{
			Preconditions: &metav1.Preconditions{UID: &t.PVC.UID},
		})
	switch {
	case err == nil, apierrors.IsNotFound(err):
		// Already gone is success: §7 requires every operation be convergent.
		return nil
	case apierrors.IsConflict(err):
		// The claim was replaced since the plan. Its verdict is void, so stop
		// rather than evicting the pod out from under an unexamined volume.
		return exitcode.Wrap(exitcode.Error, fmt.Errorf(
			"pvc %s/%s was replaced since the plan was computed; its volume source has not been checked.\n"+
				"       Re-run to re-derive scope from current state", t.PVC.Namespace, t.PVC.Name))
	default:
		return exitcode.Wrap(exitcode.Error,
			fmt.Errorf("deleting pvc %s/%s: %w", t.PVC.Namespace, t.PVC.Name, err))
	}
}

func (e *Engine) deletePod(ctx context.Context, pod *corev1.Pod) error {
	// By UID, for the same reason as the claim above: between the snapshot and
	// here the controller may have replaced this pod, and a StatefulSet's
	// replacement carries the same namespace and name. Deleting by name would
	// kill the replacement — possibly already running on a node that was never
	// selected.
	err := e.client.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{
		Preconditions: &metav1.Preconditions{UID: &pod.UID},
	})
	switch {
	case err == nil, apierrors.IsNotFound(err), apierrors.IsConflict(err):
		// NotFound and Conflict both mean "the pod we meant is gone", which is
		// the outcome this call wanted. §7: convergent.
		return nil
	default:
		return err
	}
}

// evictWithRetry submits an eviction, retrying 429s.
//
// A 429 means evicting would violate a PodDisruptionBudget. That is usually
// transient — the workload's other replicas come back and the retry succeeds.
// When it is permanent, the timeout expires and the diagnostic names the
// blocking PDB with its numbers, because "stuck" without them sends the
// operator hunting by hand, which is the runbook this tool replaces.
func (e *Engine) evictWithRetry(ctx context.Context, phase, node string, pod *corev1.Pod) error {
	deadline := time.Now().Add(e.opts.EvictionTimeout)
	backoff := time.Second
	const maxBackoff = 15 * time.Second

	for attempt := 0; ; attempt++ {
		err := e.evict(ctx, pod)
		switch {
		case err == nil, apierrors.IsNotFound(err), apierrors.IsConflict(err):
			// Conflict means the UID moved on: the pod this call was about no
			// longer exists, which is what eviction was trying to achieve.
			return nil
		case !apierrors.IsTooManyRequests(err):
			return exitcode.Wrap(exitcode.Error,
				fmt.Errorf("evicting %s/%s: %w", pod.Namespace, pod.Name, err))
		}

		if time.Now().After(deadline) {
			return e.pdbStallDiagnostic(ctx, node, pod)
		}
		if attempt == 0 {
			e.rec.Event(output.Event{
				Phase: phase, Node: node, Namespace: pod.Namespace, Kind: "pod", Name: pod.Name,
				Msg: "eviction refused by a PodDisruptionBudget, retrying", Level: output.LevelWarn,
			})
		}
		e.rec.Progress(fmt.Sprintf("  waiting on PDB for %s/%s  (%s remaining)",
			pod.Namespace, pod.Name, time.Until(deadline).Round(time.Second)))

		select {
		case <-ctx.Done():
			return exitcode.Wrap(exitcode.Interrupted, ctx.Err())
		case <-time.After(backoff):
		}
		if backoff < maxBackoff {
			backoff *= 2
		}
	}
}

func (e *Engine) evict(ctx context.Context, pod *corev1.Pod) error {
	// The eviction subresource honours DeleteOptions, so the same UID
	// precondition applies: evicting by name could evict a replacement that
	// happens to share it.
	return e.client.PolicyV1().Evictions(pod.Namespace).Evict(ctx, &policyv1.Eviction{
		ObjectMeta: metav1.ObjectMeta{Namespace: pod.Namespace, Name: pod.Name},
		DeleteOptions: &metav1.DeleteOptions{
			Preconditions: &metav1.Preconditions{UID: &pod.UID},
		},
	})
}

// waitPodGone waits for the pod object to disappear entirely.
//
// Not merely Terminating: the pvc-protection finalizer is not released while
// any pod object references the claim, including a terminating one.
//
// "Gone" means NotFound *or a changed UID*. A StatefulSet recreates its pod
// under the same namespace and name, so a name-only check can see the
// replacement and conclude the original is gone before it actually is.
func (e *Engine) waitPodGone(ctx context.Context, phase, node string, pod *corev1.Pod, start time.Time) error {
	deadline := start.Add(e.opts.EvictionTimeout)

	for {
		got, err := e.client.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return exitcode.Wrap(exitcode.Error, err)
		}
		if got.UID != pod.UID {
			return nil // already replaced, so the original is gone
		}

		if time.Now().After(deadline) {
			return e.terminationTimeoutDiagnostic(ctx, node, pod)
		}
		e.rec.Progress(fmt.Sprintf("  waiting for %s/%s to terminate  (%s)",
			pod.Namespace, pod.Name, time.Since(start).Round(time.Second)))

		select {
		case <-ctx.Done():
			return exitcode.Wrap(exitcode.Interrupted, ctx.Err())
		case <-time.After(e.opts.PollInterval):
		}
	}
}

// waitPVCGone confirms the claim actually went away once its last consumer did.
func (e *Engine) waitPVCGone(ctx context.Context, phase, node string, t scope.PVCTarget) error {
	deadline := time.Now().Add(e.opts.PVCTimeout)

	for {
		got, err := e.client.CoreV1().PersistentVolumeClaims(t.PVC.Namespace).
			Get(ctx, t.PVC.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			e.rec.Event(output.Event{
				Phase: phase, Node: node, Namespace: t.PVC.Namespace,
				Kind: "pvc", Name: t.PVC.Name, Msg: "deleted",
			})
			return nil
		}
		if err != nil {
			return exitcode.Wrap(exitcode.Error, err)
		}
		if got.UID != t.PVC.UID {
			// The original is gone and the controller has already created a
			// fresh claim from the volumeClaimTemplate. That is the successful
			// outcome, so say so rather than returning silently.
			e.rec.Event(output.Event{
				Phase: phase, Node: node, Namespace: t.PVC.Namespace,
				Kind: "pvc", Name: t.PVC.Name, Msg: "deleted and recreated by the controller",
			})
			return nil
		}

		if time.Now().After(deadline) {
			// There is no force flag for PVCs and the tool must not patch
			// finalizers off: stripping pvc-protection while a VolumeAttachment
			// still exists is exactly how a volume ends up attached to a node
			// with no Kubernetes object tracking it.
			e.rec.Diagnostic(output.Diagnostic{
				Node:     node,
				Headline: fmt.Sprintf("PVC stuck in Terminating after %s", e.opts.PVCTimeout),
				Detail: []string{
					fmt.Sprintf("%s/%s   finalizers: %v", got.Namespace, got.Name, got.Finalizers),
					"",
					"Check for lingering pods and VolumeAttachments on the selected nodes.",
				},
				Suggested: []string{
					fmt.Sprintf("kubectl describe pvc -n %s %s", got.Namespace, got.Name),
					"kubectl get volumeattachment",
				},
				Rerun: e.rerun,
			})
			return exitcode.Wrap(exitcode.PVCStuck,
				fmt.Errorf("pvc %s/%s stuck terminating", got.Namespace, got.Name))
		}
		e.rec.Progress(fmt.Sprintf("  waiting for pvc/%s to be removed", t.PVC.Name))

		select {
		case <-ctx.Done():
			return exitcode.Wrap(exitcode.Interrupted, ctx.Err())
		case <-time.After(e.opts.PollInterval):
		}
	}
}

// waitWorkloadReady waits for the workload to be whole again before moving on.
//
// §5 says to wait on the replacement pod reaching Running, never on the PVC
// reaching Bound: the PVC binds as a consequence of scheduling, so waiting on
// it inverts the dependency and reports false failures on every run. Local
// volumes use WaitForFirstConsumer, so a freshly created PVC staying Pending is
// the designed behaviour, not a stall.
//
// Readiness is measured on the controller rather than by guessing the
// replacement pod's name, which is unknowable for a Deployment.
func (e *Engine) waitWorkloadReady(ctx context.Context, phase, node string, r classify.Result) error {
	if r.Replicas == nil || r.ControllerKind == "" {
		return nil // unmanaged or unresolvable: nothing will come back
	}
	want := *r.Replicas
	if want == 0 {
		return nil
	}

	deadline := time.Now().Add(e.opts.EvictionTimeout)
	ns := r.Pod.Namespace

	for {
		ready, err := e.readyReplicas(ctx, ns, r.ControllerKind, r.ControllerName)
		if err != nil {
			return exitcode.Wrap(exitcode.Error, err)
		}
		if ready >= want {
			e.rec.Event(output.Event{
				Phase: phase, Node: node, Namespace: ns,
				Msg: fmt.Sprintf("%s/%s back to %d/%d ready", r.ControllerKind, r.ControllerName, ready, want),
			})
			return nil
		}

		if time.Now().After(deadline) {
			e.rec.Diagnostic(output.Diagnostic{
				Node:     node,
				Headline: fmt.Sprintf("replacement did not become ready within %s", e.opts.EvictionTimeout),
				Detail: []string{
					fmt.Sprintf("%s %s/%s is at %d/%d ready", r.ControllerKind, ns, r.ControllerName, ready, want),
					"",
					"A local volume binds only once its pod is scheduled, so a Pending PVC here is",
					"normal. A Pending *pod* is not — check whether the scheduler has anywhere to",
					"put it.",
				},
				Suggested: []string{
					fmt.Sprintf("kubectl get pods -n %s -o wide", ns),
					fmt.Sprintf("kubectl describe %s -n %s %s", r.ControllerKind, ns, r.ControllerName),
				},
				Rerun: e.rerun,
			})
			return exitcode.Wrap(exitcode.EvictionTimeout,
				fmt.Errorf("%s %s/%s did not return to %d ready", r.ControllerKind, ns, r.ControllerName, want))
		}
		e.rec.Progress(fmt.Sprintf("  waiting for %s/%s  (%d/%d ready)", ns, r.ControllerName, ready, want))

		select {
		case <-ctx.Done():
			return exitcode.Wrap(exitcode.Interrupted, ctx.Err())
		case <-time.After(e.opts.PollInterval):
		}
	}
}

func (e *Engine) readyReplicas(ctx context.Context, ns, kind, name string) (int32, error) {
	switch kind {
	case "Deployment":
		d, err := e.client.AppsV1().Deployments(ns).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return 0, ignoreNotFound(err)
		}
		return d.Status.ReadyReplicas, nil
	case "StatefulSet":
		s, err := e.client.AppsV1().StatefulSets(ns).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return 0, ignoreNotFound(err)
		}
		return s.Status.ReadyReplicas, nil
	case "ReplicaSet":
		rs, err := e.client.AppsV1().ReplicaSets(ns).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return 0, ignoreNotFound(err)
		}
		return rs.Status.ReadyReplicas, nil
	case "ReplicationController":
		rc, err := e.client.CoreV1().ReplicationControllers(ns).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return 0, ignoreNotFound(err)
		}
		return rc.Status.ReadyReplicas, nil
	default:
		return 0, errUnknownController
	}
}

var errUnknownController = errors.New("unknown controller kind")

// ignoreNotFound treats a deleted controller as "nothing to wait for" rather
// than an error: the workload was removed mid-drain, which is not this tool's
// problem to solve.
func ignoreNotFound(err error) error {
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}
