package drain

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

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
	if !strings.Contains(h.output(), "already cordoned") {
		t.Error("the skip was not reported")
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
	if !strings.Contains(h.output(), "already marked for deletion") {
		t.Error("the skip was not reported")
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
	if !strings.Contains(h.output(), "PodDisruptionBudget") {
		t.Error("the first 429 was not reported to the operator")
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
	h.client.PrependReactor("create", "pods", func(a k8stesting.Action) (bool, runtime.Object, error) {
		if a.GetSubresource() != "eviction" {
			return false, nil, nil
		}
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "web-0", nil)
	})

	start := time.Now()
	err := h.engine.evictWithRetry(context.Background(), "2", "n1", h.scope.Pods[0].Pod)
	if err == nil {
		t.Fatal("a Forbidden eviction reported success")
	}
	if got := exitcode.Of(err); got != exitcode.Error {
		t.Errorf("exit code = %d, want %d", got, exitcode.Error)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("took %s; a non-429 must not be retried until the timeout", elapsed)
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
