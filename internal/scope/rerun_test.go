package scope

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/GlueOps/evac/internal/kube"
)

func localPV(name, node string, claimNS, claimName string, phase corev1.PersistentVolumePhase) corev1.PersistentVolume {
	pv := corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				Local: &corev1.LocalVolumeSource{Path: "/var/lib/rancher/k3s/storage/" + name},
			},
			NodeAffinity: &corev1.VolumeNodeAffinity{
				Required: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{{
						MatchExpressions: []corev1.NodeSelectorRequirement{{
							Key:      corev1.LabelHostname,
							Operator: corev1.NodeSelectorOpIn,
							Values:   []string{node},
						}},
					}},
				},
			},
		},
		Status: corev1.PersistentVolumeStatus{Phase: phase},
	}
	if claimName != "" {
		pv.Spec.ClaimRef = &corev1.ObjectReference{Namespace: claimNS, Name: claimName}
	}
	return pv
}

func scopeWith(t *testing.T, pods []corev1.Pod, pvcs []corev1.PersistentVolumeClaim, pvs []corev1.PersistentVolume) *Scope {
	t.Helper()
	snap := kube.NewSnapshotForTest(kube.SnapshotFixture{
		TakenAt: time.Now(),
		Nodes: []corev1.Node{
			{ObjectMeta: metav1.ObjectMeta{Name: "n1"}, Status: readyStatus()},
			{ObjectMeta: metav1.ObjectMeta{Name: "n2"}, Status: readyStatus()},
		},
		Pods: pods, PVCs: pvcs, PVs: pvs,
	})
	s, err := Build(snap, nodes("n1"), "test")
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// The gap §7 describes: a run died between classification and the PVC delete,
// so the claim is marked but no pod is left to discover it through.
func TestLeftoverFindsMarkedPVCWithNoPod(t *testing.T) {
	t.Parallel()
	now := metav1.Now()
	pvc := corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "app", Name: "data-web-0",
			DeletionTimestamp: &now,
			Finalizers:        []string{"kubernetes.io/pvc-protection"},
		},
		Spec: corev1.PersistentVolumeClaimSpec{VolumeName: "pv-a"},
	}
	s := scopeWith(t, nil,
		[]corev1.PersistentVolumeClaim{pvc},
		[]corev1.PersistentVolume{localPV("pv-a", "n1", "app", "data-web-0", corev1.VolumeBound)},
	)

	got := s.Leftovers()
	if len(got) != 1 {
		t.Fatalf("found %d leftover(s), want 1 — this is discoverable only from the PV side", len(got))
	}
	if got[0].PVC == nil || got[0].PVC.Name != "data-web-0" {
		t.Errorf("leftover did not resolve back to the claim: %+v", got[0])
	}
	if got[0].Node != "n1" {
		t.Errorf("node = %q, want n1 from the PV's nodeAffinity", got[0].Node)
	}
}

func TestLeftoverReportsReleasedPVs(t *testing.T) {
	t.Parallel()
	s := scopeWith(t, nil, nil,
		[]corev1.PersistentVolume{localPV("pv-old", "n1", "app", "gone", corev1.VolumeReleased)})

	got := s.Leftovers()
	if len(got) != 1 {
		t.Fatalf("found %d leftover(s), want 1", len(got))
	}
	if got[0].Reason == "" {
		t.Error("leftover has no reason")
	}
}

// Volumes pinned to nodes that were not selected are not this run's business.
func TestLeftoverIgnoresPVsOnUnselectedNodes(t *testing.T) {
	t.Parallel()
	s := scopeWith(t, nil, nil,
		[]corev1.PersistentVolume{localPV("pv-elsewhere", "n2", "app", "x", corev1.VolumeReleased)})

	if got := s.Leftovers(); len(got) != 0 {
		t.Errorf("found %d leftover(s) on an unselected node, want 0", len(got))
	}
}

// Only local volumes are ever this tool's business, so a stranded NFS or CSI
// volume must not be reported as leftover work.
func TestLeftoverIgnoresNonLocalVolumes(t *testing.T) {
	t.Parallel()
	pv := localPV("pv-nfs", "n1", "app", "x", corev1.VolumeReleased)
	pv.Spec.Local = nil
	pv.Spec.NFS = &corev1.NFSVolumeSource{Server: "nas", Path: "/export"}

	s := scopeWith(t, nil, nil, []corev1.PersistentVolume{pv})

	if got := s.Leftovers(); len(got) != 0 {
		t.Errorf("reported %d non-local volume(s) as leftover work, want 0", len(got))
	}
}

// A claim already found through its pod is not also reported as a leftover:
// running both discovery paths and diffing them is the point.
func TestLeftoverDoesNotDuplicatePodDerivedScope(t *testing.T) {
	t.Parallel()
	now := metav1.Now()
	pvc := corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "app", Name: "data-web-0", DeletionTimestamp: &now,
		},
		Spec: corev1.PersistentVolumeClaimSpec{VolumeName: "pv-a"},
	}
	s := scopeWith(t,
		[]corev1.Pod{pod("app", "web-0", "n1", corev1.PodRunning, "data-web-0")},
		[]corev1.PersistentVolumeClaim{pvc},
		[]corev1.PersistentVolume{localPV("pv-a", "n1", "app", "data-web-0", corev1.VolumeBound)},
	)

	if len(s.PVCs) != 1 {
		t.Fatalf("pod-derived scope = %d PVCs, want 1", len(s.PVCs))
	}
	if got := s.Leftovers(); len(got) != 0 {
		t.Errorf("leftovers = %d, want 0 — this claim is already in scope via its pod", len(got))
	}
}
