// Package classify implements phase 1b: sorting the pods on the selected
// nodes into the three buckets that decide how each one is removed.
//
// This is pure logic over a snapshot. It issues no API calls, which is what
// lets it be tested exhaustively against hand-built fixtures — and given that
// misclassifying a pod here is what decides whether a workload is evicted
// politely or deleted outright, exhaustive testing is the point.
package classify

import (
	"fmt"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/GlueOps/evac/internal/kube"
)

// Inputs are the cluster objects classification reads. Taking these directly
// rather than a snapshot keeps the logic free of any API dependency, so the
// tests build fixtures as plain structs.
type Inputs struct {
	PDBs                   []policyv1.PodDisruptionBudget
	ReplicaSets            []appsv1.ReplicaSet
	Deployments            []appsv1.Deployment
	StatefulSets           []appsv1.StatefulSet
	ReplicationControllers []corev1.ReplicationController
}

// Class is the bucket a pod falls into.
type Class string

const (
	// Normal pods go through the eviction API (phase 2).
	Normal Class = "normal"
	// Fragile pods bypass eviction and are deleted directly (phase 3),
	// accepting downtime, because the eviction API would refuse them forever.
	Fragile Class = "fragile"
	// Excluded pods are not touched by the drain at all.
	Excluded Class = "excluded"
)

// ExclusionReason says why a pod is out of scope.
type ExclusionReason string

const (
	ExcludedDaemonSet ExclusionReason = "daemonset"
	ExcludedJob       ExclusionReason = "job"
	ExcludedMirror    ExclusionReason = "mirror-pod"
	// ExcludedStorageHelper is the local-path provisioner's short-lived
	// directory-cleanup pod. See isStorageHelper for why this exists.
	ExcludedStorageHelper ExclusionReason = "storage-helper"
)

// FragileReason says why a pod cannot go through eviction. A pod may carry
// more than one.
type FragileReason string

const (
	// FragileSingleReplica is a workload whose top controller wants exactly one
	// replica. With a restrictive PDB the eviction API refuses it permanently.
	FragileSingleReplica FragileReason = "single-replica"
	// FragileNoPDB is a pod no PodDisruptionBudget selects.
	FragileNoPDB FragileReason = "no-pdb"
	// FragileUnmanaged is a pod with no controller owner: nothing recreates it.
	FragileUnmanaged FragileReason = "unmanaged"
)

// Result is the verdict for one pod.
type Result struct {
	Pod   *corev1.Pod
	Class Class

	// Exclusion is set when Class is Excluded.
	Exclusion ExclusionReason
	// Fragile lists every reason the pod is fragile, in a stable order.
	// Non-empty exactly when Class is Fragile.
	FragileReasons []FragileReason

	// ControllerKind and ControllerName name the *top* controller — the
	// Deployment behind a ReplicaSet, not the ReplicaSet. Empty when unmanaged.
	ControllerKind string
	ControllerName string

	// Replicas is the top controller's desired replica count, or nil when it
	// could not be determined (an unmanaged pod, or a controller kind this tool
	// does not know how to resolve, such as a custom operator's CRD).
	Replicas *int32

	// MatchingPDBs names every PDB in the pod's namespace whose selector
	// matches it, in a stable order.
	MatchingPDBs []string
}

// Unmanaged reports whether nothing will recreate this pod. These are listed
// separately in plan and run output, labelled as permanent losses.
func (r Result) Unmanaged() bool {
	return r.hasFragileReason(FragileUnmanaged)
}

// BearsDowntime distinguishes the two populations inside the fragile bucket.
// A pod that is fragile only because no PDB selects it would have evicted
// cleanly; a single-replica workload held by a restrictive PDB is the one that
// actually goes down. That second group is surfaced separately.
func (r Result) BearsDowntime() bool {
	return r.Class == Fragile && r.hasFragileReason(FragileSingleReplica) && len(r.MatchingPDBs) > 0
}

func (r Result) hasFragileReason(want FragileReason) bool {
	for _, got := range r.FragileReasons {
		if got == want {
			return true
		}
	}
	return false
}

// Classifier holds the indexes built once from a snapshot.
type Classifier struct {
	pdbsByNamespace map[string][]policyv1.PodDisruptionBudget
	replicaSets     map[string]*appsv1.ReplicaSet
	deployments     map[string]*appsv1.Deployment
	statefulSets    map[string]*appsv1.StatefulSet
	replicaCtrls    map[string]*corev1.ReplicationController
}

func key(namespace, name string) string { return namespace + "/" + name }

// FromSnapshot builds a Classifier from a full snapshot. It requires the
// workload owner lists, so an inventory-only snapshot is rejected rather than
// silently producing wrong replica counts.
func FromSnapshot(snap *kube.Snapshot) (*Classifier, error) {
	if !snap.Full() {
		return nil, fmt.Errorf("classify: needs a full snapshot (got inventory only)")
	}
	return New(Inputs{
		PDBs:                   snap.PDBs,
		ReplicaSets:            snap.ReplicaSets,
		Deployments:            snap.Deployments,
		StatefulSets:           snap.StatefulSets,
		ReplicationControllers: snap.ReplicationControllers,
	}), nil
}

// New builds a Classifier from raw object lists.
func New(snap Inputs) *Classifier {
	c := &Classifier{
		pdbsByNamespace: make(map[string][]policyv1.PodDisruptionBudget),
		replicaSets:     make(map[string]*appsv1.ReplicaSet, len(snap.ReplicaSets)),
		deployments:     make(map[string]*appsv1.Deployment, len(snap.Deployments)),
		statefulSets:    make(map[string]*appsv1.StatefulSet, len(snap.StatefulSets)),
		replicaCtrls:    make(map[string]*corev1.ReplicationController, len(snap.ReplicationControllers)),
	}
	for i := range snap.PDBs {
		pdb := snap.PDBs[i]
		c.pdbsByNamespace[pdb.Namespace] = append(c.pdbsByNamespace[pdb.Namespace], pdb)
	}
	for i := range snap.ReplicaSets {
		rs := &snap.ReplicaSets[i]
		c.replicaSets[key(rs.Namespace, rs.Name)] = rs
	}
	for i := range snap.Deployments {
		d := &snap.Deployments[i]
		c.deployments[key(d.Namespace, d.Name)] = d
	}
	for i := range snap.StatefulSets {
		sts := &snap.StatefulSets[i]
		c.statefulSets[key(sts.Namespace, sts.Name)] = sts
	}
	for i := range snap.ReplicationControllers {
		rc := &snap.ReplicationControllers[i]
		c.replicaCtrls[key(rc.Namespace, rc.Name)] = rc
	}
	return c
}

// ClassifyAll classifies a set of pods, preserving input order.
func (c *Classifier) ClassifyAll(pods []*corev1.Pod) []Result {
	out := make([]Result, 0, len(pods))
	for _, p := range pods {
		out = append(out, c.Classify(p))
	}
	return out
}

// Classify returns the verdict for one pod.
//
// Order matters: exclusions are checked first, because an excluded pod is never
// evaluated for fragility. A DaemonSet pod has one replica per node and no PDB,
// so it would otherwise look maximally fragile and be deleted — exactly wrong.
func (c *Classifier) Classify(pod *corev1.Pod) Result {
	r := Result{Pod: pod}

	if reason, excluded := c.exclusionFor(pod); excluded {
		r.Class = Excluded
		r.Exclusion = reason
		return r
	}

	ctrl := c.resolveController(pod)
	r.ControllerKind = ctrl.kind
	r.ControllerName = ctrl.name
	r.Replicas = ctrl.replicas
	r.MatchingPDBs = c.matchingPDBs(pod)

	// Fragile if the owner wants one replica, or nothing selects it with a
	// PDB, or it has no controller at all. These are independent tests and a
	// pod can trip several; all are recorded so the operator sees why.
	if !ctrl.found {
		r.FragileReasons = append(r.FragileReasons, FragileUnmanaged)
	}
	if ctrl.replicas != nil && *ctrl.replicas == 1 {
		r.FragileReasons = append(r.FragileReasons, FragileSingleReplica)
	}
	if len(r.MatchingPDBs) == 0 {
		r.FragileReasons = append(r.FragileReasons, FragileNoPDB)
	}

	if len(r.FragileReasons) > 0 {
		r.Class = Fragile
	} else {
		r.Class = Normal
	}
	return r
}

// exclusionFor implements the first bucket: pods the drain never touches.
func (c *Classifier) exclusionFor(pod *corev1.Pod) (ExclusionReason, bool) {
	// Mirror pods are static pods owned by the kubelet. Nothing in the API can
	// evict them and the kubelet recreates them regardless.
	if _, ok := pod.Annotations[corev1.MirrorPodAnnotationKey]; ok {
		return ExcludedMirror, true
	}
	if ref := controllerRef(pod.OwnerReferences); ref != nil {
		switch ref.Kind {
		case "DaemonSet":
			// Evicted DaemonSet pods return immediately: the controller
			// tolerates the unschedulable taint. Ignoring them is unconditional.
			return ExcludedDaemonSet, true
		case "Job":
			// Job and CronJob pods finish on their own; cordon stops new ones
			// landing. Phase 4 waits for them rather than evicting.
			return ExcludedJob, true
		}
	}
	if isStorageHelper(pod) {
		return ExcludedStorageHelper, true
	}
	return "", false
}

// isStorageHelper recognises the local-path provisioner's cleanup pod.
//
// Deleting a local-path PVC spawns a short-lived pod on the node to remove the
// directory. It has no controller owner and sets spec.nodeName directly, which
// makes it look exactly like an unmanaged pod — so on a re-run, where
// classification happens fresh against live state, one of these caught
// mid-flight would be reported to the operator as a permanent pod loss and
// then deleted in phase 3.
//
// The provisioner does not label these, so the name prefix is the only durable
// signal; it is required to coincide with having no controller and a Never
// restart policy so an ordinary user pod called "helper-pod-foo" is not caught.
func isStorageHelper(pod *corev1.Pod) bool {
	return strings.HasPrefix(pod.Name, "helper-pod-") &&
		controllerRef(pod.OwnerReferences) == nil &&
		pod.Spec.RestartPolicy == corev1.RestartPolicyNever
}

// matchingPDBs implements the "has no PDB" test.
//
// This is not a lookup by name: every PDB in the pod's *own* namespace has its
// selector evaluated against the pod's labels. A PDB in another namespace never
// applies, and an empty selector matches every pod in its namespace — which
// reads like "matches nothing" and is the trap worth naming.
func (c *Classifier) matchingPDBs(pod *corev1.Pod) []string {
	var out []string
	podLabels := labels.Set(pod.Labels)
	for _, pdb := range c.pdbsByNamespace[pod.Namespace] {
		sel, err := metav1.LabelSelectorAsSelector(pdb.Spec.Selector)
		if err != nil {
			// An unparseable selector cannot be shown to match. Treating it as
			// a match would let a malformed PDB silently reclassify a pod as
			// Normal and send it into an eviction that stalls.
			continue
		}
		if sel.Matches(podLabels) {
			out = append(out, pdb.Name)
		}
	}
	return out
}

// controller is the outcome of walking ownerReferences to the top.
type controller struct {
	kind     string
	name     string
	replicas *int32
	// found is false when the pod has no controller owner at all.
	found bool
}

// resolveController walks to the top controller and reads its desired replica
// count. That is one hop for some kinds and two for others.
//
// The count is always spec.replicas, never status.readyReplicas: a 3-replica
// workload with two pods temporarily unhealthy is not a single-replica
// workload, and treating it as one would delete it outright instead of evicting
// it politely.
func (c *Classifier) resolveController(pod *corev1.Pod) controller {
	ref := controllerRef(pod.OwnerReferences)
	if ref == nil {
		return controller{}
	}

	switch ref.Kind {
	case "StatefulSet":
		if sts, ok := c.statefulSets[key(pod.Namespace, ref.Name)]; ok {
			return controller{kind: "StatefulSet", name: sts.Name, replicas: sts.Spec.Replicas, found: true}
		}
		return controller{kind: "StatefulSet", name: ref.Name, found: true}

	case "ReplicationController":
		if rc, ok := c.replicaCtrls[key(pod.Namespace, ref.Name)]; ok {
			return controller{kind: "ReplicationController", name: rc.Name, replicas: rc.Spec.Replicas, found: true}
		}
		return controller{kind: "ReplicationController", name: ref.Name, found: true}

	case "ReplicaSet":
		// Two hops. Reading spec.replicas off the ReplicaSet is the easy
		// mistake: it usually equals the Deployment's, but diverges mid-rollout
		// when two ReplicaSets are live — the old one scaled to 1 while the new
		// one scales up. That is precisely when a drain is riskiest, and it
		// would misclassify a healthy 3-replica Deployment as fragile.
		rs, ok := c.replicaSets[key(pod.Namespace, ref.Name)]
		if !ok {
			return controller{kind: "ReplicaSet", name: ref.Name, found: true}
		}
		if owner := controllerRef(rs.OwnerReferences); owner != nil && owner.Kind == "Deployment" {
			if dep, ok := c.deployments[key(rs.Namespace, owner.Name)]; ok {
				return controller{kind: "Deployment", name: dep.Name, replicas: dep.Spec.Replicas, found: true}
			}
			return controller{kind: "Deployment", name: owner.Name, found: true}
		}
		// A bare ReplicaSet with no Deployment above it is a legitimate, if
		// unusual, top-level controller.
		return controller{kind: "ReplicaSet", name: rs.Name, replicas: rs.Spec.Replicas, found: true}

	default:
		// Some other controller — a custom operator's CRD, most likely. It has
		// an owner, so it is managed and something will recreate the pod, but
		// the replica count is unknowable without knowing the kind. Leaving
		// Replicas nil means the single-replica test does not fire, so the pod
		// goes through eviction rather than direct deletion. That is the
		// conservative direction: eviction respects PDBs and a stall is caught
		// by the phase 2 timeout, whereas a wrong Fragile verdict deletes it.
		return controller{kind: ref.Kind, name: ref.Name, found: true}
	}
}

// controllerRef returns the owner reference marked controller:true, or nil.
// A pod may carry several ownerReferences but at most one is the controller,
// and only that one means "something will recreate this".
func controllerRef(refs []metav1.OwnerReference) *metav1.OwnerReference {
	for i := range refs {
		if refs[i].Controller != nil && *refs[i].Controller {
			return &refs[i]
		}
	}
	return nil
}
