package picker

import (
	"strings"
	"testing"
	"time"

	"github.com/charmbracelet/huh"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/GlueOps/evac/internal/inventory"
	"github.com/GlueOps/evac/internal/kube"
)

func build(t *testing.T, nodes ...corev1.Node) []inventory.Node {
	t.Helper()
	return inventory.Build(kube.NewSnapshotForTest(kube.SnapshotFixture{
		TakenAt: time.Now(), Nodes: nodes,
	}))
}

func node(name string, controlPlane bool) corev1.Node {
	n := corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{}},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
		},
	}
	if controlPlane {
		n.Labels["node-role.kubernetes.io/control-plane"] = "true"
	}
	return n
}

// In a monospace terminal an aligned suffix reads as a table, which is what
// lets the inventory columns survive huh's single-string options.
func TestAlignRowsProducesAlignedColumns(t *testing.T) {
	t.Parallel()
	nodes := build(t, node("short", false), node("a-much-longer-node-name", false))

	got := AlignRows(nodes)
	if len(got) != 2 {
		t.Fatalf("got %d rows, want 2", len(got))
	}
	// The status column must begin at the same offset on both rows.
	off := func(s string) int { return strings.Index(s, "Ready") }
	if off(got[0]) != off(got[1]) {
		t.Errorf("columns are not aligned:\n%q\n%q", got[0], got[1])
	}
	for _, row := range got {
		for _, want := range []string{"Ready", "pods", "PVCs"} {
			if !strings.Contains(row, want) {
				t.Errorf("row %q missing %q", row, want)
			}
		}
	}
}

func TestAlignRowsMarksCordonedAndNotReady(t *testing.T) {
	t.Parallel()
	down := node("broken", false)
	down.Status.Conditions[0].Status = corev1.ConditionFalse
	cordoned := node("paused", false)
	cordoned.Spec.Unschedulable = true

	got := AlignRows(build(t, cordoned, down))

	joined := strings.Join(got, "\n")
	if !strings.Contains(joined, "NotReady") {
		t.Errorf("NotReady node is not marked:\n%s", joined)
	}
	if !strings.Contains(joined, "Cordoned") {
		t.Errorf("cordoned node is not marked:\n%s", joined)
	}
}

// §3.1's reasoning is that silence is worse than an explanation: huh cannot
// render disabled rows, so the exclusion has to be stated in the title.
func TestTitleNamesExcludedControlPlaneNodes(t *testing.T) {
	t.Parallel()
	nodes := build(t, node("worker-1", false), node("server-0", true), node("server-1", true))

	got := Title(nodes, Options{Context: "k3d-evac-dev"})

	if !strings.Contains(got, "2 control plane node(s) excluded") {
		t.Errorf("title does not mention the exclusion:\n%s", got)
	}
	for _, name := range []string{"server-0", "server-1"} {
		if !strings.Contains(got, name) {
			t.Errorf("title does not name %s:\n%s", name, got)
		}
	}
}

// There is no background refresh, so someone who left this open for twenty
// minutes needs to know what they are looking at.
func TestTitleCarriesTheSnapshotTimestamp(t *testing.T) {
	t.Parallel()
	ts := time.Date(2026, 9, 12, 14, 2, 11, 0, time.UTC)

	got := Title(build(t, node("worker-1", false)), Options{Context: "c", TakenAt: ts})

	if !strings.Contains(got, "14:02:11Z") {
		t.Errorf("title is missing the snapshot time:\n%s", got)
	}
}

// Control plane nodes must never appear as selectable options, by any path.
func TestControlPlaneNodesAreNotOfferedAsOptions(t *testing.T) {
	t.Parallel()
	nodes := build(t, node("worker-1", false), node("server-0", true))

	rows := AlignRows(inventory.Drainable(nodes))

	if len(rows) != 1 {
		t.Fatalf("got %d selectable rows, want 1", len(rows))
	}
	if strings.Contains(rows[0], "server-0") {
		t.Error("a control plane node was offered as selectable")
	}
}

func TestRunRefusesWhenEveryNodeIsControlPlane(t *testing.T) {
	t.Parallel()
	nodes := build(t, node("server-0", true))

	_, err := Run(nodes, Options{Context: "single"})
	if err == nil {
		t.Fatal("Run succeeded with nothing drainable")
	}
	if !strings.Contains(err.Error(), "control plane") {
		t.Errorf("error = %q, want it to explain why nothing is selectable", err)
	}
}

func TestAbortingThePickerIsNotAnError(t *testing.T) {
	t.Parallel()
	nodes := build(t, node("worker-1", false))

	got, err := Run(nodes, Options{
		Context: "c",
		RunForm: func(*huh.Form) error { return huh.ErrUserAborted },
	})
	if err != nil {
		t.Fatalf("aborting produced an error: %v", err)
	}
	if !got.Cancelled {
		t.Error("Cancelled = false after the operator aborted")
	}
}

// Confirming with nothing ticked is a cancellation, not an empty drain.
func TestConfirmingAnEmptySelectionIsTreatedAsCancelled(t *testing.T) {
	t.Parallel()
	nodes := build(t, node("worker-1", false))

	got, err := Run(nodes, Options{
		Context: "c",
		RunForm: func(*huh.Form) error { return nil }, // form returns, nothing selected
	})
	if err != nil {
		t.Fatal(err)
	}
	if !got.Cancelled {
		t.Error("Cancelled = false; an empty selection must not be written as a node file")
	}
}
