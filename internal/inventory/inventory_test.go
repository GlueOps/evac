package inventory

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/GlueOps/evac/internal/kube"
)

func ptr[T any](v T) *T { return &v }

func node(name string, mutate ...func(*corev1.Node)) corev1.Node {
	n := corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{}},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
		},
	}
	for _, m := range mutate {
		m(&n)
	}
	return n
}

func labelled(k, v string) func(*corev1.Node) {
	return func(n *corev1.Node) { n.Labels[k] = v }
}

func tainted(key string, effect corev1.TaintEffect) func(*corev1.Node) {
	return func(n *corev1.Node) {
		n.Spec.Taints = append(n.Spec.Taints, corev1.Taint{Key: key, Effect: effect})
	}
}

// §3.1 lists four signals. Each must independently mark a node control plane,
// because clusters differ in which ones they set.
func TestControlPlaneDetectionSignals(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		node   corev1.Node
		wantCP bool
		signal string
	}{
		{"control-plane label", node("a", labelled(labelControlPlane, "true")), true, "label " + labelControlPlane},
		{"legacy master label", node("a", labelled(labelMaster, "")), true, "label " + labelMaster},
		{"k3s etcd label", node("a", labelled(labelEtcd, "true")), true, "label " + labelEtcd},
		{"control-plane NoSchedule taint", node("a", tainted(taintControlPlane, corev1.TaintEffectNoSchedule)), true, "taint " + taintControlPlane + ":NoSchedule"},
		{"plain worker", node("a"), false, ""},
		// A PreferNoSchedule taint is an advisory, not a control-plane marker.
		{"control-plane key with PreferNoSchedule only", node("a", tainted(taintControlPlane, corev1.TaintEffectPreferNoSchedule)), false, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotCP, gotSignal := controlPlane(&tc.node)
			if gotCP != tc.wantCP {
				t.Errorf("controlPlane = %v, want %v", gotCP, tc.wantCP)
			}
			if gotSignal != tc.signal {
				t.Errorf("signal = %q, want %q", gotSignal, tc.signal)
			}
		})
	}
}

// Observed on k3s v1.35: the server node carries the control-plane label, has
// no taint, and is schedulable. Detection must not depend on the taint, and the
// node must stay schedulable so §8's capacity math can count it.
func TestK3sServerIsControlPlaneButStillSchedulable(t *testing.T) {
	t.Parallel()
	snap := &kube.Snapshot{
		TakenAt: time.Now(),
		Nodes:   []corev1.Node{node("k3d-server-0", labelled(labelControlPlane, "true"))},
	}
	got := Build(snap)[0]

	if !got.ControlPlane {
		t.Error("ControlPlane = false — k3s servers carry the label but no taint")
	}
	if !got.Schedulable {
		t.Error("Schedulable = false — an untainted k3s server accepts workloads and must count toward post-drain capacity")
	}
	if got.Drainable() {
		t.Error("Drainable = true — control plane nodes are never drainable by any path")
	}
}

// DaemonSet pods are never drained, so counting them would overstate what a
// drain actually moves.
func TestPodCountExcludesDaemonSetPodsAndCountsDistinctPVCs(t *testing.T) {
	t.Parallel()
	pod := func(name string, ds bool, claims ...string) corev1.Pod {
		p := corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: name},
			Spec:       corev1.PodSpec{NodeName: "n1"},
		}
		if ds {
			p.OwnerReferences = []metav1.OwnerReference{{Kind: "DaemonSet", Name: "cni", Controller: ptr(true)}}
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

	snap := &kube.Snapshot{
		TakenAt: time.Now(),
		Nodes:   []corev1.Node{node("n1")},
		Pods: []corev1.Pod{
			pod("web-0", false, "data-web-0"),
			pod("web-1", false, "data-web-1"),
			// Same claim referenced twice by one pod must count once.
			pod("cache-0", false, "shared", "shared"),
			pod("cilium-xyz", true),
		},
	}
	got := Build(snap)[0]

	if got.Pods != 3 {
		t.Errorf("Pods = %d, want 3 (the DaemonSet pod must not be counted)", got.Pods)
	}
	if got.PVCs != 3 {
		t.Errorf("PVCs = %d, want 3 distinct claims", got.PVCs)
	}
}

// The gap between Age and Uptime is the signal an operator reads: a node 142d
// old reporting 2h uptime has already rebooted.
func TestUptimeComesFromReadyTransitionNotNodeAge(t *testing.T) {
	t.Parallel()
	now := time.Now()
	n := node("n1")
	n.CreationTimestamp = metav1.NewTime(now.Add(-142 * 24 * time.Hour))
	n.Status.Conditions[0].LastTransitionTime = metav1.NewTime(now.Add(-2 * time.Hour))

	snap := &kube.Snapshot{TakenAt: now, Nodes: []corev1.Node{n}}
	got := Build(snap)[0]

	if got.Age < 141*24*time.Hour {
		t.Errorf("Age = %v, want ~142d", got.Age)
	}
	if got.Uptime > 3*time.Hour {
		t.Errorf("Uptime = %v, want ~2h (from the Ready transition, not creation)", got.Uptime)
	}
}

func TestSingleNodeClusterIsDetected(t *testing.T) {
	t.Parallel()
	snap := &kube.Snapshot{
		TakenAt: time.Now(),
		Nodes:   []corev1.Node{node("solo", labelled(labelControlPlane, "true"))},
	}
	if !SingleNodeCluster(Build(snap)) {
		t.Error("SingleNodeCluster = false — a one-node k3s install has nothing drainable and needs its own message")
	}
}

func TestCuratedLabelsHidesWellKnownNoiseAndKeepsOperatorLabels(t *testing.T) {
	t.Parallel()
	got := CuratedLabels(map[string]string{
		"kubernetes.io/hostname":         "node-a-01",
		"topology.kubernetes.io/region":  "eu-1",
		"node-role.kubernetes.io/worker": "",
		"beta.kubernetes.io/arch":        "amd64",
		"glueops.dev/pool":               "a",
		"glueops.dev/generation":         "3",
	})
	want := "glueops.dev/generation=3,glueops.dev/pool=a"
	if got != want {
		t.Errorf("CuratedLabels = %q, want %q", got, want)
	}
}
