package drain

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/GlueOps/evac/internal/exitcode"
	"github.com/GlueOps/evac/internal/inventory"
	"github.com/GlueOps/evac/internal/kube"
	"github.com/GlueOps/evac/internal/output"
	"github.com/GlueOps/evac/internal/scope"
)

// These tests use the fake clientset for plumbing only — which calls were made,
// whether a 429 was retried — and never for semantics. The fake runs no
// controllers, does not honour finalizers, and does not enforce field
// selectors, so a fake-based test of the phase 2 ordering would be green while
// the real thing deadlocked. That behaviour is covered by the integration
// suite instead.

func node(name string) corev1.Node {
	return corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{corev1.LabelHostname: name}},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
		},
	}
}

func pod(ns, name, nodeName string, claims ...string) corev1.Pod {
	p := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, UID: types.UID("uid-" + name)},
		Spec:       corev1.PodSpec{NodeName: nodeName},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
	for _, c := range claims {
		p.Spec.Volumes = append(p.Spec.Volumes, corev1.Volume{
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: c},
			},
		})
	}
	return p
}

func localPair(ns, claim, volume string) (corev1.PersistentVolumeClaim, corev1.PersistentVolume) {
	pvc := corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: claim, UID: types.UID("uid-" + claim)},
		Spec:       corev1.PersistentVolumeClaimSpec{VolumeName: volume},
	}
	pv := corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: volume},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				Local: &corev1.LocalVolumeSource{Path: "/var/lib/rancher/k3s/storage/" + volume},
			},
		},
	}
	return pvc, pv
}

// harness wires a scope, a recorder and a fake client together.
type harness struct {
	engine *Engine
	client *fake.Clientset
	out    *bytes.Buffer
	rec    *output.Recorder
	scope  *scope.Scope
}

func newHarness(t *testing.T, fx kube.SnapshotFixture, targets ...string) *harness {
	t.Helper()
	snap := kube.NewSnapshotForTest(fx)

	byName := inventory.ByName(inventory.Build(snap))
	var sel []inventory.Node
	for _, n := range targets {
		got, ok := byName[n]
		if !ok {
			t.Fatalf("node %s not in fixture", n)
		}
		sel = append(sel, *got)
	}
	sc, err := scope.Build(snap, sel, "test")
	if err != nil {
		t.Fatal(err)
	}

	objs := []runtime.Object{}
	for i := range fx.Nodes {
		objs = append(objs, &fx.Nodes[i])
	}
	for i := range fx.Pods {
		objs = append(objs, &fx.Pods[i])
	}
	for i := range fx.PVCs {
		objs = append(objs, &fx.PVCs[i])
	}
	client := fake.NewSimpleClientset(objs...)

	buf := &bytes.Buffer{}
	rec, err := output.New(output.Options{Stdout: buf, NoLogFile: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rec.Close() })

	opts := Defaults()
	opts.PollInterval = 10 * time.Millisecond
	opts.EvictionTimeout = 2 * time.Second
	opts.JobDeadline = time.Second
	opts.PVCTimeout = time.Second

	return &harness{
		engine: New(client, rec, sc, opts, "evac drain"),
		client: client, out: buf, rec: rec, scope: sc,
	}
}

func (h *harness) output() string {
	_ = h.rec.Close()
	return h.out.String()
}

// actions returns the recorded verb/resource pairs, e.g. "delete persistentvolumeclaims".
func (h *harness) actions() []string {
	var out []string
	for _, a := range h.client.Actions() {
		out = append(out, a.GetVerb()+" "+a.GetResource().Resource)
	}
	return out
}

func (h *harness) didAction(verb, resource string) bool {
	for _, a := range h.client.Actions() {
		if a.GetVerb() == verb && a.GetResource().Resource == resource {
			return true
		}
	}
	return false
}

// --- phase 1: cordon -------------------------------------------------------

func TestCordonSkipsNodesAlreadyUnschedulable(t *testing.T) {
	t.Parallel()
	n1, n2 := node("n1"), node("n2")
	n2.Spec.Unschedulable = true

	h := newHarness(t, kube.SnapshotFixture{Nodes: []corev1.Node{n1, n2}}, "n1", "n2")
	if err := h.engine.cordonAll(context.Background()); err != nil {
		t.Fatal(err)
	}

	var patches int
	for _, a := range h.client.Actions() {
		if a.GetVerb() == "patch" && a.GetResource().Resource == "nodes" {
			patches++
		}
	}
	if patches != 1 {
		t.Errorf("issued %d node patches, want 1 — cordoning an already-cordoned node is a no-op (§7)", patches)
	}
	if out := h.output(); !strings.Contains(out, "already cordoned") {
		t.Errorf("the skip was not reported to the operator:\n%s", out)
	}
}

// --- PVC deletion gating ---------------------------------------------------

func TestPVCIsNotDeletedWhenTheProviderGuardIsTripped(t *testing.T) {
	t.Parallel()
	pvc, pv := localPair("app", "data-0", "pv-a")
	fx := kube.SnapshotFixture{
		Nodes: []corev1.Node{node("n1")},
		Pods:  []corev1.Pod{pod("app", "web-0", "n1", "data-0")},
		PVCs:  []corev1.PersistentVolumeClaim{pvc},
		PVs:   []corev1.PersistentVolume{pv},
	}
	h := newHarness(t, fx, "n1")
	// Simulate the cluster-wide kill switch.
	h.scope.Provider.Disabled = true
	h.scope.Provider.Signal = "node providerID prefix aws://"

	if err := h.engine.deletePVC(context.Background(), "2", "n1", h.scope.PVCs[0]); err != nil {
		t.Fatal(err)
	}
	if h.didAction("delete", "persistentvolumeclaims") {
		t.Error("a PVC was deleted with the provider guard tripped")
	}
}

func TestRefusedPVCIsKeptAndTheReasonIsReported(t *testing.T) {
	t.Parallel()
	pvc, pv := localPair("app", "data-0", "pv-a")
	// Make it CSI-backed so the volume-source guard refuses it.
	pv.Spec.Local = nil
	pv.Spec.CSI = &corev1.CSIPersistentVolumeSource{Driver: "nfs.csi.k8s.io"}

	h := newHarness(t, kube.SnapshotFixture{
		Nodes: []corev1.Node{node("n1")},
		Pods:  []corev1.Pod{pod("app", "web-0", "n1", "data-0")},
		PVCs:  []corev1.PersistentVolumeClaim{pvc},
		PVs:   []corev1.PersistentVolume{pv},
	}, "n1")

	if err := h.engine.deletePVC(context.Background(), "2", "n1", h.scope.PVCs[0]); err != nil {
		t.Fatal(err)
	}
	if h.didAction("delete", "persistentvolumeclaims") {
		t.Error("a non-local PVC was deleted")
	}
	out := h.output()
	if !strings.Contains(out, "keeping") {
		t.Errorf("the keep was not reported:\n%s", out)
	}
	if !strings.Contains(out, "nfs.csi.k8s.io") {
		t.Errorf("the reason does not name the driver:\n%s", out)
	}
}

// §7: convergent. Re-issuing a delete against a claim already marked achieves
// nothing, so a re-run skips straight to eviction.
func TestPVCAlreadyMarkedForDeletionIsNotDeletedAgain(t *testing.T) {
	t.Parallel()
	pvc, pv := localPair("app", "data-0", "pv-a")
	now := metav1.Now()
	pvc.DeletionTimestamp = &now
	pvc.Finalizers = []string{"kubernetes.io/pvc-protection"}

	h := newHarness(t, kube.SnapshotFixture{
		Nodes: []corev1.Node{node("n1")},
		Pods:  []corev1.Pod{pod("app", "web-0", "n1", "data-0")},
		PVCs:  []corev1.PersistentVolumeClaim{pvc},
		PVs:   []corev1.PersistentVolume{pv},
	}, "n1")

	if err := h.engine.deletePVC(context.Background(), "2", "n1", h.scope.PVCs[0]); err != nil {
		t.Fatal(err)
	}
	if h.didAction("delete", "persistentvolumeclaims") {
		t.Error("a re-run re-issued a delete against an already-marked claim")
	}
	if out := h.output(); !strings.Contains(out, "already marked for deletion") {
		t.Errorf("the skip was not reported to the operator:\n%s", out)
	}
}

func TestLocalPVCIsDeleted(t *testing.T) {
	t.Parallel()
	pvc, pv := localPair("app", "data-0", "pv-a")
	h := newHarness(t, kube.SnapshotFixture{
		Nodes: []corev1.Node{node("n1")},
		Pods:  []corev1.Pod{pod("app", "web-0", "n1", "data-0")},
		PVCs:  []corev1.PersistentVolumeClaim{pvc},
		PVs:   []corev1.PersistentVolume{pv},
	}, "n1")

	if err := h.engine.deletePVC(context.Background(), "2", "n1", h.scope.PVCs[0]); err != nil {
		t.Fatal(err)
	}
	if !h.didAction("delete", "persistentvolumeclaims") {
		t.Errorf("a local PVC was not deleted; actions were %v", h.actions())
	}
}

// A claim that vanished between the snapshot and the delete is not an error:
// every operation is meant to be convergent (§7).
func TestDeletingAnAlreadyGonePVCIsNotAnError(t *testing.T) {
	t.Parallel()
	pvc, pv := localPair("app", "data-0", "pv-a")
	h := newHarness(t, kube.SnapshotFixture{
		Nodes: []corev1.Node{node("n1")},
		Pods:  []corev1.Pod{pod("app", "web-0", "n1", "data-0")},
		PVCs:  []corev1.PersistentVolumeClaim{pvc},
		PVs:   []corev1.PersistentVolume{pv},
	}, "n1")
	h.client.PrependReactor("delete", "persistentvolumeclaims",
		func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewNotFound(schema.GroupResource{Resource: "persistentvolumeclaims"}, "data-0")
		})

	if err := h.engine.deletePVC(context.Background(), "2", "n1", h.scope.PVCs[0]); err != nil {
		t.Errorf("deleting an already-gone PVC returned %v", err)
	}
}

// --- eviction --------------------------------------------------------------

// A 429 means evicting would violate a PDB. It is usually transient, so it is
// retried rather than treated as a failure.
func TestEvictionRetriesOn429(t *testing.T) {
	t.Parallel()
	h := newHarness(t, kube.SnapshotFixture{
		Nodes: []corev1.Node{node("n1")},
		Pods:  []corev1.Pod{pod("app", "web-0", "n1")},
	}, "n1")

	attempts := 0
	h.client.PrependReactor("create", "pods", func(a k8stesting.Action) (bool, runtime.Object, error) {
		if a.GetSubresource() != "eviction" {
			return false, nil, nil
		}
		attempts++
		if attempts == 1 {
			return true, nil, apierrors.NewTooManyRequests("disruption budget", 1)
		}
		return true, nil, nil
	})

	p := h.scope.Pods[0].Pod
	if err := h.engine.evictWithRetry(context.Background(), "2", "n1", p); err != nil {
		t.Fatalf("eviction failed instead of retrying: %v", err)
	}
	if attempts < 2 {
		t.Errorf("made %d eviction attempts, want a retry after the 429", attempts)
	}
	if out := h.output(); !strings.Contains(out, "PodDisruptionBudget") {
		t.Errorf("the first 429 was not reported to the operator:\n%s", out)
	}
}

// A permanent 429 must time out with exit code 4, not retry forever.
func TestPermanent429TimesOutWithEvictionTimeoutCode(t *testing.T) {
	t.Parallel()
	h := newHarness(t, kube.SnapshotFixture{
		Nodes: []corev1.Node{node("n1")},
		Pods:  []corev1.Pod{pod("app", "web-0", "n1")},
	}, "n1")
	h.client.PrependReactor("create", "pods", func(a k8stesting.Action) (bool, runtime.Object, error) {
		if a.GetSubresource() != "eviction" {
			return false, nil, nil
		}
		return true, nil, apierrors.NewTooManyRequests("disruption budget", 1)
	})

	err := h.engine.evictWithRetry(context.Background(), "2", "n1", h.scope.Pods[0].Pod)
	if err == nil {
		t.Fatal("a permanent 429 eventually succeeded")
	}
	if got := exitcode.Of(err); got != exitcode.EvictionTimeout {
		t.Errorf("exit code = %d, want %d", got, exitcode.EvictionTimeout)
	}
}

// Anything that is not a 429 is a real failure and must surface immediately
// rather than being retried until the timeout.
func TestNon429EvictionErrorSurfacesImmediately(t *testing.T) {
	t.Parallel()
	h := newHarness(t, kube.SnapshotFixture{
		Nodes: []corev1.Node{node("n1")},
		Pods:  []corev1.Pod{pod("app", "web-0", "n1")},
	}, "n1")
	attempts := 0
	h.client.PrependReactor("create", "pods", func(a k8stesting.Action) (bool, runtime.Object, error) {
		if a.GetSubresource() != "eviction" {
			return false, nil, nil
		}
		attempts++
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "web-0", nil)
	})

	err := h.engine.evictWithRetry(context.Background(), "2", "n1", h.scope.Pods[0].Pod)
	if err == nil {
		t.Fatal("a Forbidden eviction reported success")
	}
	if got := exitcode.Of(err); got != exitcode.Error {
		t.Errorf("exit code = %d, want %d", got, exitcode.Error)
	}
	// Counting attempts rather than timing: a wall-clock assertion under -race
	// on a loaded two-core runner measures scheduling noise, not intent.
	if attempts != 1 {
		t.Errorf("made %d eviction attempts, want 1 — only a 429 is retried", attempts)
	}
}

// --- waiting for the pod object to be gone ---------------------------------

// "Gone" means NotFound *or a changed UID*. A StatefulSet recreates its pod
// under the same namespace and name, so a name-only check would see the
// replacement and conclude the original had gone before it had.
func TestPodIsConsideredGoneWhenItsUIDChanges(t *testing.T) {
	t.Parallel()
	original := pod("app", "web-0", "n1")
	h := newHarness(t, kube.SnapshotFixture{
		Nodes: []corev1.Node{node("n1")},
		Pods:  []corev1.Pod{original},
	}, "n1")

	replacement := original.DeepCopy()
	replacement.UID = "a-brand-new-uid"
	h.client.PrependReactor("get", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, replacement, nil
	})

	if err := h.engine.waitPodGone(context.Background(), "2", "n1", &original, time.Now()); err != nil {
		t.Errorf("waitPodGone did not treat a changed UID as gone: %v", err)
	}
}

func TestPodIsConsideredGoneWhenNotFound(t *testing.T) {
	t.Parallel()
	original := pod("app", "web-0", "n1")
	h := newHarness(t, kube.SnapshotFixture{Nodes: []corev1.Node{node("n1")}}, "n1")

	if err := h.engine.waitPodGone(context.Background(), "2", "n1", &original, time.Now()); err != nil {
		t.Errorf("waitPodGone errored on an absent pod: %v", err)
	}
}

// A pod that never goes away times out with exit code 4 rather than hanging.
func TestPodThatNeverTerminatesTimesOut(t *testing.T) {
	t.Parallel()
	original := pod("app", "web-0", "n1")
	h := newHarness(t, kube.SnapshotFixture{
		Nodes: []corev1.Node{node("n1")},
		Pods:  []corev1.Pod{original},
	}, "n1")

	err := h.engine.waitPodGone(context.Background(), "2", "n1", &original, time.Now())
	if err == nil {
		t.Fatal("waiting on a pod that never terminates returned success")
	}
	if got := exitcode.Of(err); got != exitcode.EvictionTimeout {
		t.Errorf("exit code = %d, want %d", got, exitcode.EvictionTimeout)
	}
}

// --- result aggregation ----------------------------------------------------

func TestResultCodeUsesSeverityPrecedence(t *testing.T) {
	t.Parallel()
	res := Result{Nodes: []NodeResult{
		{Node: "a", Code: exitcode.JobTimeout},
		{Node: "b", Code: exitcode.EvictionTimeout},
		{Node: "c", Code: exitcode.OK},
	}}
	if got := res.Code(); got != exitcode.EvictionTimeout {
		t.Errorf("Code = %d, want %d — a retry-on-timer code must not mask one needing a human", got, exitcode.EvictionTimeout)
	}
	if !res.Failed() {
		t.Error("Failed = false despite a failing node")
	}

	all := Result{Nodes: []NodeResult{{Node: "a", Code: exitcode.OK}, {Node: "b", Code: exitcode.OK}}}
	if all.Failed() {
		t.Error("Failed = true when every node succeeded")
	}
}

// --- phase 2 / phase 3 call contracts --------------------------------------

// evictionDeletesPod makes the fake behave enough like a real cluster for
// movePod to progress: the fake's eviction subresource does not remove the pod
// object, so without this the wait never completes.
//
// This models the API's *call* contract, not its semantics. Nothing below
// asserts anything that depends on finalizers or scheduling.
func evictionDeletesPod(t *testing.T, h *harness, ns, name string) {
	t.Helper()
	evicted := false
	h.client.PrependReactor("create", "pods", func(a k8stesting.Action) (bool, runtime.Object, error) {
		if a.GetSubresource() != "eviction" {
			return false, nil, nil
		}
		evicted = true
		return true, nil, nil
	})
	h.client.PrependReactor("get", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		if evicted {
			return true, nil, apierrors.NewNotFound(schema.GroupResource{Resource: "pods"}, name)
		}
		return false, nil, nil
	})
}

// §5 phase 2: delete the PVC FIRST, then evict.
//
// This pins the call contract only — it does NOT prove the ordering avoids the
// deadlock, which is TestPhase2OrderingMovesLocalVolumeToAnotherNode's job
// against a real cluster. The two are complementary: this one is deterministic
// and catches a reversal, the integration test proves a reversal would matter.
func TestPhase2DeletesThePVCBeforeEvicting(t *testing.T) {
	t.Parallel()
	pvc, pv := localPair("app", "data-0", "pv-a")
	h := newHarness(t, kube.SnapshotFixture{
		Nodes: []corev1.Node{node("n1")},
		Pods:  []corev1.Pod{pod("app", "web-0", "n1", "data-0")},
		PVCs:  []corev1.PersistentVolumeClaim{pvc},
		PVs:   []corev1.PersistentVolume{pv},
	}, "n1")
	evictionDeletesPod(t, h, "app", "web-0")

	if err := h.engine.movePod(context.Background(), "2", "n1", h.scope.Pods[0], false); err != nil {
		t.Fatalf("movePod: %v\n%s", err, h.out.String())
	}

	pvcDelete, eviction := -1, -1
	for i, a := range h.client.Actions() {
		switch {
		case a.GetVerb() == "delete" && a.GetResource().Resource == "persistentvolumeclaims":
			if pvcDelete < 0 {
				pvcDelete = i
			}
		case a.GetVerb() == "create" && a.GetResource().Resource == "pods" && a.GetSubresource() == "eviction":
			if eviction < 0 {
				eviction = i
			}
		}
	}

	if pvcDelete < 0 {
		t.Fatalf("no PVC delete was issued; actions were %v", h.actions())
	}
	if eviction < 0 {
		t.Fatalf("no eviction was issued; actions were %v", h.actions())
	}
	if pvcDelete > eviction {
		t.Errorf("evicted at action %d before deleting the PVC at %d — with that order the controller can recreate the pod while the claim is still healthy, which wedges it Pending forever", eviction, pvcDelete)
	}
}

// §5 phase 3: fragile pods are DELETED, never evicted. A single-replica
// workload with minAvailable:1 allows zero disruptions, so the eviction API
// would refuse it forever — not slowly, never.
func TestPhase3DeletesFragilePodsRatherThanEvicting(t *testing.T) {
	t.Parallel()
	pvc, pv := localPair("db", "data-vault-0", "pv-a")
	h := newHarness(t, kube.SnapshotFixture{
		Nodes: []corev1.Node{node("n1")},
		Pods:  []corev1.Pod{pod("db", "vault-0", "n1", "data-vault-0")},
		PVCs:  []corev1.PersistentVolumeClaim{pvc},
		PVs:   []corev1.PersistentVolume{pv},
	}, "n1")

	if err := h.engine.movePod(context.Background(), "3", "n1", h.scope.Pods[0], true); err != nil {
		t.Fatalf("movePod: %v\n%s", err, h.out.String())
	}

	var sawDelete, sawEviction bool
	pvcDelete, podDelete := -1, -1
	for i, a := range h.client.Actions() {
		if a.GetVerb() == "create" && a.GetSubresource() == "eviction" {
			sawEviction = true
		}
		if a.GetVerb() == "delete" && a.GetResource().Resource == "pods" {
			sawDelete = true
			if podDelete < 0 {
				podDelete = i
			}
		}
		if a.GetVerb() == "delete" && a.GetResource().Resource == "persistentvolumeclaims" && pvcDelete < 0 {
			pvcDelete = i
		}
	}

	if sawEviction {
		t.Error("a fragile pod went through the eviction API; a restrictive PDB would refuse it forever")
	}
	if !sawDelete {
		t.Fatalf("no pod delete was issued; actions were %v", h.actions())
	}
	// §5: the PVC-first ordering matters MORE here, not less — a direct delete
	// removes the pod object immediately, so the controller recreates faster
	// and the race window is tighter.
	if pvcDelete < 0 || pvcDelete > podDelete {
		t.Errorf("PVC deleted at %d, pod at %d — the claim must be marked first", pvcDelete, podDelete)
	}
}

// §5: the tool must never patch finalizers off a PVC. Stripping pvc-protection
// while a VolumeAttachment still exists is how a volume ends up attached to a
// node with no Kubernetes object tracking it.
func TestNoPatchOrUpdateIsEverIssuedAgainstAPVC(t *testing.T) {
	t.Parallel()
	pvc, pv := localPair("app", "data-0", "pv-a")
	h := newHarness(t, kube.SnapshotFixture{
		Nodes: []corev1.Node{node("n1")},
		Pods:  []corev1.Pod{pod("app", "web-0", "n1", "data-0")},
		PVCs:  []corev1.PersistentVolumeClaim{pvc},
		PVs:   []corev1.PersistentVolume{pv},
	}, "n1")
	evictionDeletesPod(t, h, "app", "web-0")

	_ = h.engine.movePod(context.Background(), "2", "n1", h.scope.Pods[0], false)

	for _, a := range h.client.Actions() {
		if a.GetResource().Resource != "persistentvolumeclaims" {
			continue
		}
		switch a.GetVerb() {
		case "patch", "update":
			t.Errorf("issued a %s against a PVC; finalizers must never be stripped", a.GetVerb())
		}
	}
}

// --- deletion identity (UID preconditions) ---------------------------------

// The guard's verdict is computed against the object in the snapshot, and
// minutes can pass before the delete. If the claim was replaced in that window
// the verdict is void, and deleting by name would destroy a volume the
// allowlist never examined.
func TestPVCReplacedSinceThePlanIsNotDeleted(t *testing.T) {
	t.Parallel()
	pvc, pv := localPair("app", "data-0", "pv-a")
	h := newHarness(t, kube.SnapshotFixture{
		Nodes: []corev1.Node{node("n1")},
		Pods:  []corev1.Pod{pod("app", "web-0", "n1", "data-0")},
		PVCs:  []corev1.PersistentVolumeClaim{pvc},
		PVs:   []corev1.PersistentVolume{pv},
	}, "n1")

	// The API server rejects a delete whose UID precondition no longer matches.
	h.client.PrependReactor("delete", "persistentvolumeclaims",
		func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewConflict(
				schema.GroupResource{Resource: "persistentvolumeclaims"}, "data-0",
				errors.New("UID precondition not met"))
		})

	err := h.engine.deletePVC(context.Background(), "2", "n1", h.scope.PVCs[0])
	if err == nil {
		t.Fatal("a replaced PVC was treated as deleted; the drain would then evict the pod out from under an unexamined volume")
	}
	if !strings.Contains(err.Error(), "replaced since the plan") {
		t.Errorf("error = %q, want it to explain that the claim changed identity", err)
	}
	if !strings.Contains(err.Error(), "Re-run") {
		t.Errorf("error = %q, want it to tell the operator how to recover", err)
	}
}

// A conflict on the POD delete is the opposite case: the pod this call was
// about is already gone, which is what the call wanted. §7 convergence.
func TestPodReplacedSinceThePlanIsTreatedAsAlreadyGone(t *testing.T) {
	t.Parallel()
	h := newHarness(t, kube.SnapshotFixture{
		Nodes: []corev1.Node{node("n1")},
		Pods:  []corev1.Pod{pod("app", "web-0", "n1")},
	}, "n1")
	h.client.PrependReactor("delete", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewConflict(
			schema.GroupResource{Resource: "pods"}, "web-0", errors.New("UID precondition not met"))
	})

	if err := h.engine.deletePod(context.Background(), h.scope.Pods[0].Pod); err != nil {
		t.Errorf("a replaced pod was reported as an error: %v", err)
	}
}

// --- phase 4 terminal check pagination -------------------------------------

// A node can hold more pods than one page — Succeeded Job pods accumulate
// wherever nothing sets a TTL. Reading only the first page and finding it
// filtered out would report an empty node and declare a drain complete with
// workload still running.
func TestPollNodeReadsEveryPage(t *testing.T) {
	t.Parallel()
	h := newHarness(t, kube.SnapshotFixture{Nodes: []corev1.Node{node("n1")}}, "n1")

	// Page 1: all DaemonSet pods, every one filtered out. Page 2: a real
	// workload pod. A single-page read sees an empty node.
	dsPod := pod("kube-system", "cilium-0", "n1")
	dsPod.OwnerReferences = []metav1.OwnerReference{{
		Kind: "DaemonSet", Name: "cilium", Controller: func() *bool { b := true; return &b }(),
	}}
	blocker := pod("app", "straggler", "n1")

	calls := 0
	h.client.PrependReactor("list", "pods", func(a k8stesting.Action) (bool, runtime.Object, error) {
		calls++
		la := a.(k8stesting.ListAction)
		if la.GetListRestrictions().Fields != nil && calls == 1 {
			return true, &corev1.PodList{
				ListMeta: metav1.ListMeta{Continue: "page-2"},
				Items:    []corev1.Pod{dsPod},
			}, nil
		}
		return true, &corev1.PodList{Items: []corev1.Pod{blocker}}, nil
	})

	blockers, jobs, err := h.engine.pollNode(context.Background(), "n1")
	if err != nil {
		t.Fatal(err)
	}
	if calls < 2 {
		t.Errorf("made %d list call(s); the continue token was not followed", calls)
	}
	if len(blockers) != 1 {
		t.Fatalf("found %d blocker(s), want 1 — the node would have been reported drained with a pod still on it", len(blockers))
	}
	if len(jobs) != 0 {
		t.Errorf("jobs = %d, want 0", len(jobs))
	}
}

// --- skipped nodes ---------------------------------------------------------

// Phase 1 cordons every selected node before any eviction, so a serial run that
// stops at node 2 of 5 has still taken all five out of service. Cordon is
// one-way (§1), so the untouched remainder has to be reported or the operator
// does not know what is still unschedulable.
func TestNodesCordonedButNeverAttemptedAreReported(t *testing.T) {
	t.Parallel()
	res := Result{Nodes: []NodeResult{
		{Node: "n1", Code: exitcode.OK},
		{Node: "n2", Code: exitcode.PVCStuck},
		{Node: "n3", Skipped: true},
		{Node: "n4", Skipped: true},
	}}

	skipped := res.Skipped()
	if len(skipped) != 2 {
		t.Fatalf("Skipped = %v, want two nodes", skipped)
	}
	// A skipped node was never attempted, so it must not contribute an outcome
	// — otherwise its zero Code would read as a success.
	if got := res.Code(); got != exitcode.PVCStuck {
		t.Errorf("Code = %d, want %d; skipped entries must not affect the aggregate", got, exitcode.PVCStuck)
	}
	if !res.Failed() {
		t.Error("Failed = false despite a failing node")
	}
}

// The serial loop must record the remainder rather than truncating the result,
// which is what made a failure on node 1 of several print nothing at all.
func TestSerialFailureRecordsTheUntouchedRemainder(t *testing.T) {
	t.Parallel()
	h := newHarness(t, kube.SnapshotFixture{
		Nodes: []corev1.Node{node("n1"), node("n2"), node("n3")},
	}, "n1", "n2", "n3")

	// Fail the very first node's cordon-independent work by making every pod
	// list fail, so drainNode returns an error on n1.
	h.client.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewServiceUnavailable("boom")
	})

	res := h.engine.runSerial(context.Background())

	if len(res.Nodes) != 3 {
		t.Fatalf("recorded %d node(s), want 3 — the untouched remainder was dropped", len(res.Nodes))
	}
	if res.Nodes[0].Skipped {
		t.Error("the attempted node was marked skipped")
	}
	if got := res.Skipped(); len(got) != 2 {
		t.Errorf("Skipped = %v, want n2 and n3", got)
	}
}

// --- phase 4 exit codes ----------------------------------------------------

// Exit 3 is the only code §9 tells a wrapper it may retry on a timer, and that
// is honest only for Job pods, which finish by themselves. A pod that arrived
// after the snapshot — one naming its node directly, which the cordon does not
// stop — will still be there on the next attempt, so reporting 3 sends a
// wrapper into a loop that burns the full deadline each time forever.
func TestNonJobBlockerDoesNotReportTheRetryOnATimerCode(t *testing.T) {
	t.Parallel()
	h := newHarness(t, kube.SnapshotFixture{Nodes: []corev1.Node{node("n1")}}, "n1")

	// Not in the snapshot: it appeared after scope was computed.
	blocker := pod("default", "pinned-debug", "n1")
	h.client.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, &corev1.PodList{Items: []corev1.Pod{blocker}}, nil
	})

	err := h.engine.phase4(context.Background(), "n1", time.Now())
	if err == nil {
		t.Fatal("phase 4 reported success with a pod still on the node")
	}
	if got := exitcode.Of(err); got == exitcode.JobTimeout {
		t.Errorf("exit code = %d (JobTimeout); a wrapper would retry this forever on something that needs a human", got)
	}
	if got := exitcode.Of(err); got != exitcode.Error {
		t.Errorf("exit code = %d, want %d", got, exitcode.Error)
	}

	out := h.output()
	for _, want := range []string{"cannot be drained", "pinned-debug", "Re-running will not help"} {
		if !strings.Contains(out, want) {
			t.Errorf("diagnostic missing %q:\n%s", want, out)
		}
	}
}

// A Job pod on its own still reports 3: those do finish, so a timed retry is
// the correct advice.
func TestOnlyJobPodsStillReportsTheJobDeadlineCode(t *testing.T) {
	t.Parallel()
	h := newHarness(t, kube.SnapshotFixture{Nodes: []corev1.Node{node("n1")}}, "n1")

	jobPod := pod("analytics", "nightly-rollup", "n1")
	ctrl := true
	jobPod.OwnerReferences = []metav1.OwnerReference{{Kind: "Job", Name: "nightly", Controller: &ctrl}}
	h.client.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, &corev1.PodList{Items: []corev1.Pod{jobPod}}, nil
	})

	err := h.engine.phase4(context.Background(), "n1", time.Now())
	if got := exitcode.Of(err); got != exitcode.JobTimeout {
		t.Errorf("exit code = %d, want %d — a Job pod does finish on its own", got, exitcode.JobTimeout)
	}
}

// --- shared claims ---------------------------------------------------------

// A ReadWriteOnce local volume may be mounted by more than one pod on the same
// node. Verifying the claim has gone while another in-scope pod still holds
// pvc-protection is waiting for something that correctly is not happening yet —
// it burned the PVC timeout and failed the node for doing the right thing.
func TestSharedClaimIsVerifiedOnlyAfterTheLastPodMoves(t *testing.T) {
	t.Parallel()
	pvc, pv := localPair("app", "shared", "pv-a")
	h := newHarness(t, kube.SnapshotFixture{
		Nodes: []corev1.Node{node("n1")},
		Pods: []corev1.Pod{
			pod("app", "reader", "n1", "shared"),
			pod("app", "writer", "n1", "shared"),
		},
		PVCs: []corev1.PersistentVolumeClaim{pvc},
		PVs:  []corev1.PersistentVolume{pv},
	}, "n1")

	if len(h.scope.PVCs) != 1 {
		t.Fatalf("scope holds %d claim(s), want 1", len(h.scope.PVCs))
	}
	target := h.scope.PVCs[0]
	if len(target.Pods) != 2 {
		t.Fatalf("claim records %d consumer(s), want 2 — the second pod was dropped", len(target.Pods))
	}

	first, second := h.scope.Pods[0].Pod, h.scope.Pods[1].Pod

	// Nothing moved yet: neither pod is the last.
	if h.engine.lastConsumer(target, first) {
		t.Error("the first pod was treated as the last consumer; its verification would block on the other pod's finalizer")
	}
	// Once the first has moved, the second is.
	h.engine.markMoved(first)
	if !h.engine.lastConsumer(target, second) {
		t.Error("the second pod was not treated as the last consumer; the claim would never be verified")
	}

	// Both pods must still see the claim in step 1, so the PVC-first ordering
	// holds for each of them.
	for _, p := range []*corev1.Pod{first, second} {
		if got := h.engine.pvcsFor(p); len(got) != 1 {
			t.Errorf("pvcsFor(%s) returned %d target(s), want 1", p.Name, len(got))
		}
	}
}

// --- controller removed mid-drain ------------------------------------------

// Returning (0, nil) for a NotFound made this poll to the eviction timeout and
// then report that a replacement never became ready — ten minutes and a false
// exit 4 because someone deleted a Deployment.
func TestDeletedControllerEndsTheWaitImmediately(t *testing.T) {
	t.Parallel()
	h := newHarness(t, kube.SnapshotFixture{
		Nodes:        []corev1.Node{node("n1")},
		Pods:         []corev1.Pod{pod("app", "web-0", "n1")},
		StatefulSets: []appsv1.StatefulSet{{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "web"}}},
	}, "n1")
	h.client.PrependReactor("get", "statefulsets", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewNotFound(schema.GroupResource{Resource: "statefulsets"}, "web")
	})

	want := int32(3)
	r := h.scope.Pods[0]
	r.ControllerKind, r.ControllerName, r.Replicas = "StatefulSet", "web", &want

	start := time.Now()
	if err := h.engine.waitWorkloadReady(context.Background(), "2", "n1", r); err != nil {
		t.Fatalf("a deleted controller was reported as a failure: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("took %s; the wait should end as soon as the controller is known to be gone", elapsed)
	}
	if !strings.Contains(h.output(), "no longer exists") {
		t.Errorf("the operator was not told why the wait ended:\n%s", h.output())
	}
}
