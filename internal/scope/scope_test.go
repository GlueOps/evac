package scope

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/GlueOps/evac/internal/inventory"
	"github.com/GlueOps/evac/internal/kube"
)

func ptr[T any](v T) *T { return &v }

// fullSnapshot builds a snapshot with the owner lists populated, which
// classification requires.
func fullSnapshot(t *testing.T, pods []corev1.Pod, pvcs []corev1.PersistentVolumeClaim, pvs []corev1.PersistentVolume) *kube.Snapshot {
	t.Helper()
	nodes := []corev1.Node{
		{ObjectMeta: metav1.ObjectMeta{Name: "n1"}, Status: readyStatus()},
		{ObjectMeta: metav1.ObjectMeta{Name: "n2"}, Status: readyStatus()},
	}
	return kube.NewSnapshotForTest(kube.SnapshotFixture{
		TakenAt: time.Now(),
		Context: "test",
		Nodes:   nodes,
		Pods:    pods,
		PVCs:    pvcs,
		PVs:     pvs,
	})
}

func readyStatus() corev1.NodeStatus {
	return corev1.NodeStatus{
		Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
	}
}

func pod(ns, name, node string, phase corev1.PodPhase, claims ...string) corev1.Pod {
	p := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec:       corev1.PodSpec{NodeName: node},
		Status:     corev1.PodStatus{Phase: phase},
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

func jobPod(ns, name, node string, phase corev1.PodPhase) corev1.Pod {
	p := pod(ns, name, node, phase)
	p.OwnerReferences = []metav1.OwnerReference{{Kind: "Job", Name: "installer", Controller: ptr(true)}}
	return p
}

func localPVC(ns, name, volume string) (corev1.PersistentVolumeClaim, corev1.PersistentVolume) {
	pvc := corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
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

func nodes(names ...string) []inventory.Node {
	snap := kube.NewSnapshotForTest(kube.SnapshotFixture{TakenAt: time.Now()})
	for _, n := range names {
		snap.Nodes = append(snap.Nodes, corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: n}, Status: readyStatus(),
		})
	}
	return inventory.Build(snap)
}

// Every k3s cluster carries a Succeeded helm-install Job pod. Waiting on it
// would run phase 4 to its deadline on the first node of every drain.
func TestCompletedJobPodsAreNotWaitedOn(t *testing.T) {
	t.Parallel()
	snap := fullSnapshot(t,
		[]corev1.Pod{
			jobPod("kube-system", "helm-install-traefik", "n1", corev1.PodSucceeded),
			jobPod("analytics", "nightly-rollup", "n1", corev1.PodRunning),
			pod("app", "failed-once", "n1", corev1.PodFailed),
		}, nil, nil)

	s, err := Build(snap, nodes("n1"), "test")
	if err != nil {
		t.Fatal(err)
	}

	waiting := s.JobPods()
	if len(waiting) != 1 {
		t.Fatalf("waiting on %d job pod(s), want 1 — a Succeeded pod has already finished", len(waiting))
	}
	if waiting[0].Pod.Name != "nightly-rollup" {
		t.Errorf("waiting on %q, want the running Job pod", waiting[0].Pod.Name)
	}
	for _, r := range s.Pods {
		if r.Pod.Name == "failed-once" {
			t.Error("a Failed pod is in scope; it holds nothing and needs no eviction")
		}
	}
}

// §5: discovery is scoped to the target pod set, never swept by node. Sweeping
// by node picks up claims belonging to pods that are never evicted, and those
// pods keep running with a PVC stuck Terminating underneath them.
func TestPVCDiscoveryIgnoresClaimsOfExcludedPods(t *testing.T) {
	t.Parallel()
	dsPod := pod("kube-system", "cilium-abc", "n1", corev1.PodRunning, "ds-state")
	dsPod.OwnerReferences = []metav1.OwnerReference{{Kind: "DaemonSet", Name: "cilium", Controller: ptr(true)}}

	pvcA, pvA := localPVC("app", "data-web-0", "pv-a")
	pvcB, pvB := localPVC("kube-system", "ds-state", "pv-b")

	snap := fullSnapshot(t,
		[]corev1.Pod{pod("app", "web-0", "n1", corev1.PodRunning, "data-web-0"), dsPod},
		[]corev1.PersistentVolumeClaim{pvcA, pvcB},
		[]corev1.PersistentVolume{pvA, pvB},
	)

	s, err := Build(snap, nodes("n1"), "test")
	if err != nil {
		t.Fatal(err)
	}

	got := s.DeletablePVCs()
	if len(got) != 1 {
		t.Fatalf("deletable PVCs = %d, want 1", len(got))
	}
	if got[0].PVC.Name != "data-web-0" {
		t.Errorf("selected %q; the DaemonSet pod's claim must not be in scope — that pod is never evicted", got[0].PVC.Name)
	}
}

// Pods on nodes that are not selected are simply not in scope.
func TestScopeIsLimitedToSelectedNodes(t *testing.T) {
	t.Parallel()
	snap := fullSnapshot(t, []corev1.Pod{
		pod("app", "here", "n1", corev1.PodRunning),
		pod("app", "elsewhere", "n2", corev1.PodRunning),
	}, nil, nil)

	s, err := Build(snap, nodes("n1"), "test")
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Pods) != 1 || s.Pods[0].Pod.Name != "here" {
		t.Errorf("scope = %d pods, want just the one on n1", len(s.Pods))
	}
}

// §7: a PVC already carrying a deletionTimestamp is unfinished work from a
// previous run, and the delete is skipped rather than repeated.
func TestPVCAlreadyDeletingIsMarkedAsResuming(t *testing.T) {
	t.Parallel()
	pvc, pv := localPVC("app", "data-web-0", "pv-a")
	now := metav1.Now()
	pvc.DeletionTimestamp = &now
	pvc.Finalizers = []string{"kubernetes.io/pvc-protection"}

	snap := fullSnapshot(t,
		[]corev1.Pod{pod("app", "web-0", "n1", corev1.PodRunning, "data-web-0")},
		[]corev1.PersistentVolumeClaim{pvc},
		[]corev1.PersistentVolume{pv},
	)

	s, err := Build(snap, nodes("n1"), "test")
	if err != nil {
		t.Fatal(err)
	}
	if len(s.PVCs) != 1 {
		t.Fatalf("PVCs = %d, want 1", len(s.PVCs))
	}
	if !s.PVCs[0].AlreadyDeleting {
		t.Error("AlreadyDeleting = false — a deletionTimestamp is self-evidently unfinished work from an earlier run")
	}
}
