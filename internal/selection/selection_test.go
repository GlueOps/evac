package selection

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/GlueOps/evac/internal/inventory"
	"github.com/GlueOps/evac/internal/kube"
)

func nodes(t *testing.T, specs ...corev1.Node) []inventory.Node {
	t.Helper()
	return inventory.Build(kube.NewSnapshotForTest(kube.SnapshotFixture{
		TakenAt: time.Now(), Nodes: specs,
	}))
}

func node(name string, labels map[string]string) corev1.Node {
	if labels == nil {
		labels = map[string]string{}
	}
	return corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
		},
	}
}

func controlPlaneNode(name string) corev1.Node {
	return node(name, map[string]string{"node-role.kubernetes.io/control-plane": "true"})
}

func all(t *testing.T) []inventory.Node {
	return nodes(t,
		node("worker-1", map[string]string{"pool": "a"}),
		node("worker-2", map[string]string{"pool": "a"}),
		node("worker-3", map[string]string{"pool": "b"}),
		controlPlaneNode("server-0"),
	)
}

// --- flags -----------------------------------------------------------------

func TestNodesFlagDropsControlPlaneAndReportsIt(t *testing.T) {
	t.Parallel()
	got, err := Resolve(Request{Names: []string{"worker-1", "server-0"}}, all(t), false)
	if err != nil {
		t.Fatal(err)
	}

	if len(got.Nodes) != 1 || got.Nodes[0].Name != "worker-1" {
		t.Errorf("selected %v, want just worker-1", got.Names())
	}
	if len(got.DroppedControlPlane) != 1 || got.DroppedControlPlane[0].Name != "server-0" {
		t.Fatalf("dropped = %+v, want server-0 reported", got.DroppedControlPlane)
	}
	if got.DroppedControlPlane[0].Signal == "" {
		t.Error("drop was reported without naming which signal excluded it")
	}
}

func TestSelectorDropsControlPlane(t *testing.T) {
	t.Parallel()
	list := nodes(t,
		node("worker-1", map[string]string{"pool": "a"}),
		func() corev1.Node {
			n := controlPlaneNode("server-0")
			n.Labels["pool"] = "a"
			return n
		}(),
	)

	got, err := Resolve(Request{Selector: "pool=a"}, list, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Nodes) != 1 || got.Nodes[0].Name != "worker-1" {
		t.Errorf("selected %v, want just worker-1", got.Names())
	}
	if len(got.DroppedControlPlane) != 1 {
		t.Errorf("dropped = %+v, want the control plane node reported", got.DroppedControlPlane)
	}
}

func TestUnknownNodeIsAnError(t *testing.T) {
	t.Parallel()
	_, err := Resolve(Request{Names: []string{"worker-1", "ghost"}}, all(t), false)
	if err == nil {
		t.Fatal("Resolve succeeded with an unknown node name")
	}
	if !strings.Contains(err.Error(), "ghost") {
		t.Errorf("error = %q, want it to name the missing node", err)
	}
}

func TestSelectorMatchingNothingIsAnError(t *testing.T) {
	t.Parallel()
	if _, err := Resolve(Request{Selector: "pool=nonexistent"}, all(t), false); err == nil {
		t.Error("Resolve succeeded with a selector matching nothing")
	}
}

func TestSourcesAreMutuallyExclusive(t *testing.T) {
	t.Parallel()
	tests := []Request{
		{Names: []string{"worker-1"}, Selector: "pool=a"},
		{Names: []string{"worker-1"}, File: "x", FileExplicit: true},
		{Selector: "pool=a", File: "x", FileExplicit: true},
	}
	for _, req := range tests {
		if _, err := Resolve(req, all(t), false); err == nil {
			t.Errorf("Resolve(%+v) succeeded; combining sources should be rejected rather than given a precedence", req)
		}
	}
}

func TestDuplicateNamesAreCollapsed(t *testing.T) {
	t.Parallel()
	got, err := Resolve(Request{Names: []string{"worker-1", "worker-1", " worker-1 "}}, all(t), false)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Nodes) != 1 {
		t.Errorf("selected %v, want one entry", got.Names())
	}
}

// --- node file -------------------------------------------------------------

func writeNodeFile(t *testing.T, lines ...string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "nodes.txt")
	body := "# context: test\n" + strings.Join(lines, "\n") + "\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// The layer that matters: the node file is hand-editable, so §3.1's check
// cannot live only in the selection path. Flags filter and report; a file
// refuses, because someone typed that name deliberately.
func TestNodeFileNamingControlPlaneIsRefusedOutright(t *testing.T) {
	t.Parallel()
	path := writeNodeFile(t, "worker-1", "server-0")

	_, err := Resolve(Request{File: path, FileExplicit: true}, all(t), true)
	if err == nil {
		t.Fatal("Resolve succeeded; a node file naming a control plane node must be refused, not silently filtered")
	}

	var cp *ControlPlaneInFileError
	if !errors.As(err, &cp) {
		t.Fatalf("error = %T (%v), want *ControlPlaneInFileError", err, err)
	}
	if len(cp.Nodes) != 1 || cp.Nodes[0].Name != "server-0" {
		t.Errorf("error names %+v, want server-0", cp.Nodes)
	}
}

// Read-only callers see the same file filtered rather than refused, so plan can
// still show what a corrected selection would do.
func TestReadOnlyCallersFilterTheSameFileInsteadOfRefusing(t *testing.T) {
	t.Parallel()
	path := writeNodeFile(t, "worker-1", "server-0")

	got, err := Resolve(Request{File: path, FileExplicit: true}, all(t), false)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Nodes) != 1 || got.Nodes[0].Name != "worker-1" {
		t.Errorf("selected %v, want just worker-1", got.Names())
	}
	if len(got.DroppedControlPlane) != 1 {
		t.Errorf("dropped = %+v, want the exclusion still reported", got.DroppedControlPlane)
	}
}

func TestEmptyNodeFileIsAnError(t *testing.T) {
	t.Parallel()
	path := writeNodeFile(t, "", "   ")

	if _, err := Resolve(Request{File: path, FileExplicit: true}, all(t), true); err == nil {
		t.Error("Resolve succeeded on a node file with no nodes")
	}
}

func TestNoSelectionAtAllIsAnError(t *testing.T) {
	t.Parallel()
	req := Request{}
	if !req.Empty() {
		t.Fatal("Empty() = false for a blank request")
	}
	if _, err := Resolve(req, all(t), false); err == nil {
		t.Error("Resolve succeeded with no selection; §1 requires the operator choose targets")
	}
}

func TestResultsAreSortedByName(t *testing.T) {
	t.Parallel()
	got, err := Resolve(Request{Names: []string{"worker-3", "worker-1", "worker-2"}}, all(t), false)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"worker-1", "worker-2", "worker-3"}
	for i, n := range got.Names() {
		if n != want[i] {
			t.Fatalf("order = %v, want %v", got.Names(), want)
		}
	}
}
