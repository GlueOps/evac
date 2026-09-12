package render

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/GlueOps/evac/internal/inventory"
	"github.com/GlueOps/evac/internal/kube"
	"github.com/GlueOps/evac/internal/scope"
)

func ptr[T any](v T) *T { return &v }

func planNode(name string) corev1.Node {
	return corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{corev1.LabelHostname: name}},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
		},
	}
}

func planPod(ns, name, nodeName string, owner string, claims ...string) corev1.Pod {
	p := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, Labels: map[string]string{"app": "x"}},
		Spec:       corev1.PodSpec{NodeName: nodeName},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
	if owner != "" {
		p.OwnerReferences = []metav1.OwnerReference{{Kind: "StatefulSet", Name: owner, Controller: ptr(true)}}
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

func planLocalPair(ns, claim, volume string) (corev1.PersistentVolumeClaim, corev1.PersistentVolume) {
	pvc := corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: claim},
		Spec:       corev1.PersistentVolumeClaimSpec{VolumeName: volume},
	}
	pv := corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: volume},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				Local: &corev1.LocalVolumeSource{Path: "/var/lib/" + volume},
			},
		},
	}
	return pvc, pv
}

func buildPlanScope(t *testing.T, fx kube.SnapshotFixture, target string) *scope.Scope {
	t.Helper()
	fx.TakenAt = time.Now()
	fx.Context = "glueops-prod-eu-1"
	snap := kube.NewSnapshotForTest(fx)
	n := inventory.ByName(inventory.Build(snap))[target]
	if n == nil {
		t.Fatalf("node %s not in fixture", target)
	}
	sc, err := scope.Build(snap, []inventory.Node{*n}, "glueops-prod-eu-1")
	if err != nil {
		t.Fatal(err)
	}
	return sc
}

func renderPlan(t *testing.T, sc *scope.Scope, opts PlanOptions) string {
	t.Helper()
	opts.Width = 300
	if opts.Context == "" {
		opts.Context = "glueops-prod-eu-1"
	}
	var buf bytes.Buffer
	if err := Plan(&buf, sc, nil, opts); err != nil {
		t.Fatal(err)
	}
	return buf.String()
}

// §8's ordering is the substance: PVC destruction is the only irreversible part
// of the operation, so it must appear above pod counts and capacity math. An
// operator scanning quickly has to hit the permanent losses first.
func TestPVCsToBeDestroyedLeadTheOutput(t *testing.T) {
	t.Parallel()
	pvc, pv := planLocalPair("platform", "data-vault-0", "pv-a")
	sc := buildPlanScope(t, kube.SnapshotFixture{
		Nodes:        []corev1.Node{planNode("n1"), planNode("n2")},
		Pods:         []corev1.Pod{planPod("platform", "vault-0", "n1", "vault", "data-vault-0")},
		PVCs:         []corev1.PersistentVolumeClaim{pvc},
		PVs:          []corev1.PersistentVolume{pv},
		StatefulSets: []appsv1.StatefulSet{{ObjectMeta: metav1.ObjectMeta{Namespace: "platform", Name: "vault"}, Spec: appsv1.StatefulSetSpec{Replicas: ptr(int32(1))}}},
	}, "n1")

	out := renderPlan(t, sc, PlanOptions{})

	destroyed := strings.Index(out, "PVCs to be DESTROYED")
	counts := strings.Index(out, "Pods on selected nodes")
	blast := strings.Index(out, "affects:")
	if destroyed < 0 {
		t.Fatalf("no PVC section:\n%s", out)
	}
	if destroyed > counts || destroyed > blast {
		t.Errorf("the irreversible losses do not lead the output:\n%s", out)
	}
	if !strings.Contains(out, "data-vault-0") {
		t.Errorf("the claim is not named:\n%s", out)
	}
}

// §8: per-PVC keeps are never reported — they are not a loss and do not need
// review — but a cluster-wide suppression must be stated, with its signal.
func TestGuardNoteAppearsOnlyWhenSuppressionIsClusterWide(t *testing.T) {
	t.Parallel()
	pvc, pv := planLocalPair("platform", "data-0", "pv-a")
	fx := kube.SnapshotFixture{
		Nodes:        []corev1.Node{planNode("n1"), planNode("n2")},
		Pods:         []corev1.Pod{planPod("platform", "web-0", "n1", "web", "data-0")},
		PVCs:         []corev1.PersistentVolumeClaim{pvc},
		PVs:          []corev1.PersistentVolume{pv},
		StatefulSets: []appsv1.StatefulSet{{ObjectMeta: metav1.ObjectMeta{Namespace: "platform", Name: "web"}, Spec: appsv1.StatefulSetSpec{Replicas: ptr(int32(3))}}},
	}

	t.Run("normal cluster prints nothing", func(t *testing.T) {
		out := renderPlan(t, buildPlanScope(t, fx, "n1"), PlanOptions{})
		if strings.Contains(out, "PVC deletion disabled") {
			t.Errorf("a guard note appeared on a cluster where deletion is enabled:\n%s", out)
		}
	})

	t.Run("suppressed cluster names the signal", func(t *testing.T) {
		sc := buildPlanScope(t, fx, "n1")
		sc.Provider.Disabled = true
		sc.Provider.Signal = "node providerID prefix aws://"

		out := renderPlan(t, sc, PlanOptions{})
		if !strings.Contains(out, "PVC deletion disabled") {
			t.Errorf("the suppression was not reported:\n%s", out)
		}
		if !strings.Contains(out, "aws://") {
			t.Errorf("the note does not name the signal that tripped it:\n%s", out)
		}
	})
}

// §8 requires unmanaged pods be listed separately and labelled as permanent.
func TestUnmanagedPodsAreListedAsPermanentLosses(t *testing.T) {
	t.Parallel()
	sc := buildPlanScope(t, kube.SnapshotFixture{
		Nodes: []corev1.Node{planNode("n1"), planNode("n2")},
		Pods:  []corev1.Pod{planPod("debug", "shell-jdoe", "n1", "")},
	}, "n1")

	out := renderPlan(t, sc, PlanOptions{})

	if !strings.Contains(out, "will NOT be recreated") {
		t.Errorf("unmanaged pods are not labelled as permanent:\n%s", out)
	}
	if !strings.Contains(out, "shell-jdoe") {
		t.Errorf("the unmanaged pod is not named:\n%s", out)
	}
}

// Two hundred rows is not readable; §8 collapses above a threshold and says so.
func TestLongPVCListCollapsesAndSaysHowToExpand(t *testing.T) {
	t.Parallel()
	fx := kube.SnapshotFixture{
		Nodes:        []corev1.Node{planNode("n1"), planNode("n2")},
		StatefulSets: []appsv1.StatefulSet{{ObjectMeta: metav1.ObjectMeta{Namespace: "bulk", Name: "sts"}, Spec: appsv1.StatefulSetSpec{Replicas: ptr(int32(100))}}},
	}
	for i := range 60 {
		claim := fmt.Sprintf("data-%d", i)
		pvc, pv := planLocalPair("bulk", claim, fmt.Sprintf("pv-%d", i))
		fx.Pods = append(fx.Pods, planPod("bulk", fmt.Sprintf("sts-%d", i), "n1", "sts", claim))
		fx.PVCs = append(fx.PVCs, pvc)
		fx.PVs = append(fx.PVs, pv)
	}
	sc := buildPlanScope(t, fx, "n1")

	collapsed := renderPlan(t, sc, PlanOptions{})
	if !strings.Contains(collapsed, "--full expands") {
		t.Errorf("a 60-row list did not collapse:\n%s", collapsed[:min(len(collapsed), 800)])
	}
	if strings.Contains(collapsed, "data-59") {
		t.Error("the collapsed view still lists individual claims")
	}

	full := renderPlan(t, sc, PlanOptions{Full: true})
	if !strings.Contains(full, "data-59") {
		t.Error("--full did not expand the list")
	}
}

// The confirmation block is what the operator's eye lands on after the PVC
// table has scrolled off, so it repeats the irreversible totals — and in the
// same order, permanent losses before recoverable eviction.
func TestConfirmationSummaryLeadsWithIrreversibleTotals(t *testing.T) {
	t.Parallel()
	pvc, pv := planLocalPair("platform", "data-vault-0", "pv-a")
	sc := buildPlanScope(t, kube.SnapshotFixture{
		Nodes: []corev1.Node{planNode("n1"), planNode("n2")},
		Pods: []corev1.Pod{
			planPod("platform", "vault-0", "n1", "vault", "data-vault-0"),
			planPod("debug", "shell-jdoe", "n1", ""),
		},
		PVCs:         []corev1.PersistentVolumeClaim{pvc},
		PVs:          []corev1.PersistentVolume{pv},
		StatefulSets: []appsv1.StatefulSet{{ObjectMeta: metav1.ObjectMeta{Namespace: "platform", Name: "vault"}, Spec: appsv1.StatefulSetSpec{Replicas: ptr(int32(1))}}},
		PDBs: []policyv1.PodDisruptionBudget{{
			ObjectMeta: metav1.ObjectMeta{Namespace: "platform", Name: "vault"},
			Spec: policyv1.PodDisruptionBudgetSpec{
				MinAvailable: ptr(intstr.FromInt32(1)),
				Selector:     &metav1.LabelSelector{MatchLabels: map[string]string{"app": "x"}},
			},
		}},
	}, "n1")

	got := ConfirmationSummary(sc)

	destroyed := strings.Index(got, "PVCs DESTROYED")
	evicted := strings.Index(got, "pods evicted")
	if destroyed < 0 || evicted < 0 {
		t.Fatalf("summary is missing totals:\n%s", got)
	}
	if destroyed > evicted {
		t.Errorf("recoverable eviction is listed above irreversible destruction:\n%s", got)
	}
	if !strings.Contains(got, "deleted permanently") {
		t.Errorf("the unmanaged pod loss is not called out:\n%s", got)
	}
	if !strings.Contains(got, "will go down") {
		t.Errorf("the downtime-bearing workload is not called out:\n%s", got)
	}
}

// A suppressed cluster must say so at the prompt too, or "0 PVCs" reads as
// "there was nothing to delete".
func TestConfirmationSummaryExplainsZeroWhenSuppressed(t *testing.T) {
	t.Parallel()
	pvc, pv := planLocalPair("platform", "data-0", "pv-a")
	sc := buildPlanScope(t, kube.SnapshotFixture{
		Nodes:        []corev1.Node{planNode("n1"), planNode("n2")},
		Pods:         []corev1.Pod{planPod("platform", "web-0", "n1", "web", "data-0")},
		PVCs:         []corev1.PersistentVolumeClaim{pvc},
		PVs:          []corev1.PersistentVolume{pv},
		StatefulSets: []appsv1.StatefulSet{{ObjectMeta: metav1.ObjectMeta{Namespace: "platform", Name: "web"}, Spec: appsv1.StatefulSetSpec{Replicas: ptr(int32(3))}}},
	}, "n1")
	sc.Provider.Disabled = true
	sc.Provider.Signal = "CSIDriver ebs.csi.aws.com present"

	got := ConfirmationSummary(sc)
	if !strings.Contains(got, "deletion disabled") {
		t.Errorf("a zero count is shown without explaining why:\n%s", got)
	}
}

// §7's PV-side discovery surfaces work no pod points at any more.
func TestLeftoversFromAnEarlierRunAreReported(t *testing.T) {
	t.Parallel()
	now := metav1.Now()
	pvc, pv := planLocalPair("platform", "data-orphan", "pv-orphan")
	pvc.DeletionTimestamp = &now
	pv.Spec.ClaimRef = &corev1.ObjectReference{Namespace: "platform", Name: "data-orphan"}
	pv.Spec.NodeAffinity = &corev1.VolumeNodeAffinity{
		Required: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{
			MatchExpressions: []corev1.NodeSelectorRequirement{{
				Key: corev1.LabelHostname, Operator: corev1.NodeSelectorOpIn, Values: []string{"n1"},
			}},
		}}},
	}

	sc := buildPlanScope(t, kube.SnapshotFixture{
		Nodes: []corev1.Node{planNode("n1"), planNode("n2")},
		PVCs:  []corev1.PersistentVolumeClaim{pvc},
		PVs:   []corev1.PersistentVolume{pv},
	}, "n1")

	out := renderPlan(t, sc, PlanOptions{})
	if !strings.Contains(out, "Leftover work") {
		t.Errorf("leftover work from an earlier run was not reported:\n%s", out)
	}
	if !strings.Contains(out, "pv-orphan") {
		t.Errorf("the leftover PV is not named:\n%s", out)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
