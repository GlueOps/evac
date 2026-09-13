package render

import (
	"bytes"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/GlueOps/evac/internal/inventory"
	"github.com/GlueOps/evac/internal/kube"
)

// --- table -----------------------------------------------------------------

func cols(n int) []Column {
	out := make([]Column, 0, n)
	for i := range n {
		out = append(out, Column{
			Header: strings.Repeat("H", 8),
			Drop:   i, // column 0 is undroppable, the rest drop highest-first
			Value:  func(int) string { return strings.Repeat("v", 8) },
		})
	}
	return out
}

func TestNarrowTerminalsDropLowPriorityColumns(t *testing.T) {
	t.Parallel()
	var wide, narrow bytes.Buffer

	_ = Table{Columns: cols(9), Rows: 2, Width: 200}.Render(&wide)
	_ = Table{Columns: cols(9), Rows: 2, Width: 70}.Render(&narrow)

	wideCols := strings.Count(strings.Split(wide.String(), "\n")[0], "H") / 8
	narrowCols := strings.Count(strings.Split(narrow.String(), "\n")[0], "H") / 8

	if narrowCols >= wideCols {
		t.Errorf("narrow rendered %d columns and wide %d; narrow terminals must shed columns", narrowCols, wideCols)
	}
	if narrowCols < 1 {
		t.Error("every column was dropped; the undroppable one must survive")
	}
}

// Piping to a file or through tee must never lose columns: that file is the
// audit artifact.
func TestNonTerminalOutputKeepsEveryColumn(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	// Width 0 with a non-terminal writer means "do not drop".
	_ = Table{Columns: cols(9), Rows: 1, Width: 0}.Render(&buf)

	if got := strings.Count(strings.Split(buf.String(), "\n")[0], "H") / 8; got != 9 {
		t.Errorf("rendered %d columns, want all 9 when not writing to a terminal", got)
	}
}

func TestTableAlignsColumns(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	names := []string{"a", "much-longer-name"}
	_ = Table{
		Rows:  2,
		Width: 200,
		Columns: []Column{
			{Header: "NAME", Value: func(i int) string { return names[i] }},
			{Header: "X", Value: func(int) string { return "1" }},
		},
	}.Render(&buf)

	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	off := func(s string) int { return strings.LastIndex(s, "1") }
	if off(lines[1]) != off(lines[2]) {
		t.Errorf("columns not aligned:\n%s", buf.String())
	}
}

// --- node table ------------------------------------------------------------

func node(name string, mutate ...func(*corev1.Node)) corev1.Node {
	n := corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{}},
		Status: corev1.NodeStatus{
			NodeInfo:   corev1.NodeSystemInfo{KubeletVersion: "v1.35.5+k3s1"},
			Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
		},
	}
	for _, m := range mutate {
		m(&n)
	}
	return n
}

func build(t *testing.T, ns ...corev1.Node) []inventory.Node {
	t.Helper()
	return inventory.Build(kube.NewSnapshotForTest(kube.SnapshotFixture{TakenAt: time.Now(), Nodes: ns}))
}

// Show rather than hide — an operator who cannot find a node will go
// looking, and silence is worse than an explanation.
//
// The annotation says "not-drainable" rather than "excluded" because a
// schedulable control plane node is emphatically not excluded from receiving
// workload: on k3s it is untainted and will absorb everything evicted off the
// agents. Reading "excluded" as "not involved" is what makes draining every
// worker look safe.
func TestNodeTableAnnotatesControlPlaneAsNotDrainableAndSchedulable(t *testing.T) {
	t.Parallel()
	cp := node("server-0", func(n *corev1.Node) {
		n.Labels["node-role.kubernetes.io/control-plane"] = "true"
	})
	var buf bytes.Buffer
	if err := NodeTable(&buf, build(t, node("worker-1"), cp), NodeTableOptions{Width: 200}); err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	if !strings.Contains(out, "server-0") {
		t.Error("control plane node was hidden from the inventory")
	}
	if !strings.Contains(out, "control-plane/not-drainable/schedulable") {
		t.Errorf("control plane node is not annotated as a schedulable non-drain target:\n%s", out)
	}
	if strings.Contains(out, "excluded") {
		t.Errorf("still says \"excluded\", which reads as \"not involved\":\n%s", out)
	}
}

// A cordoned control plane node genuinely will not receive anything, so the
// schedulable half of the annotation must drop off.
func TestCordonedControlPlaneIsNotAdvertisedAsSchedulable(t *testing.T) {
	t.Parallel()
	cp := node("server-0", func(n *corev1.Node) {
		n.Labels["node-role.kubernetes.io/control-plane"] = "true"
		n.Spec.Unschedulable = true
	})
	var buf bytes.Buffer
	if err := NodeTable(&buf, build(t, node("worker-1"), cp), NodeTableOptions{Width: 200}); err != nil {
		t.Fatal(err)
	}
	if out := buf.String(); strings.Contains(out, "not-drainable/schedulable") {
		t.Errorf("cordoned control plane advertised as schedulable:\n%s", out)
	}
}

func TestNodeTableLabelModes(t *testing.T) {
	t.Parallel()
	n := node("worker-1", func(n *corev1.Node) {
		n.Labels["kubernetes.io/hostname"] = "worker-1"
		n.Labels["glueops.dev/pool"] = "a"
	})
	rows := build(t, n)

	t.Run("default hides well-known noise", func(t *testing.T) {
		var buf bytes.Buffer
		_ = NodeTable(&buf, rows, NodeTableOptions{Width: 300})
		if strings.Contains(buf.String(), "kubernetes.io/hostname") {
			t.Error("default label mode showed well-known noise")
		}
		if !strings.Contains(buf.String(), "glueops.dev/pool=a") {
			t.Error("default label mode dropped an operator-set label")
		}
	})

	t.Run("show-labels dumps everything", func(t *testing.T) {
		var buf bytes.Buffer
		_ = NodeTable(&buf, rows, NodeTableOptions{ShowLabels: true, Width: 300})
		if !strings.Contains(buf.String(), "kubernetes.io/hostname") {
			t.Error("--show-labels did not include well-known labels")
		}
	})

	t.Run("label-columns promotes to its own column", func(t *testing.T) {
		var buf bytes.Buffer
		_ = NodeTable(&buf, rows, NodeTableOptions{LabelColumns: []string{"glueops.dev/pool"}, Width: 300})
		if !strings.Contains(buf.String(), "GLUEOPS.DEV/POOL") {
			t.Errorf("--label-columns did not add a column:\n%s", buf.String())
		}
	})
}

func TestNodeTableSortByUptime(t *testing.T) {
	t.Parallel()
	now := time.Now()
	old := node("old")
	old.Status.Conditions[0].LastTransitionTime = metav1.NewTime(now.Add(-100 * time.Hour))
	fresh := node("fresh")
	fresh.Status.Conditions[0].LastTransitionTime = metav1.NewTime(now.Add(-1 * time.Hour))

	var buf bytes.Buffer
	_ = NodeTable(&buf, build(t, old, fresh), NodeTableOptions{SortBy: "uptime", Width: 200})

	body := buf.String()
	if strings.Index(body, "fresh") > strings.Index(body, "old") {
		t.Errorf("--sort-by uptime did not put the shortest uptime first:\n%s", body)
	}
}

func TestDurationFormatting(t *testing.T) {
	t.Parallel()
	tests := map[time.Duration]string{
		0:                    "<unknown>",
		45 * time.Second:     "45s",
		90 * time.Minute:     "1h30m",
		50 * time.Hour:       "2d",
		400 * 24 * time.Hour: "1y35d",
	}
	for in, want := range tests {
		if got := duration(in); got != want {
			t.Errorf("duration(%v) = %q, want %q", in, got, want)
		}
	}
}

// The confirmation block is the last thing before the prompt, and the incident
// it exists for was an operator mistaken about *which* nodes. A count is not
// enough, and the fact that nodes are left cordoned is part of what is being
// approved rather than something to discover afterwards.
func TestConfirmationSummaryNamesNodesAndWarnsAboutCordon(t *testing.T) {
	t.Parallel()
	sc := buildPlanScope(t, kube.SnapshotFixture{
		Nodes: []corev1.Node{planNode("agent-0"), planNode("agent-1")},
		Pods:  []corev1.Pod{planPod("platform", "web", "agent-0", "web")},
	}, "agent-0")

	got := ConfirmationSummary(sc)

	if !strings.Contains(got, "agent-0") {
		t.Errorf("summary does not name the node being drained:\n%s", got)
	}
	if !strings.Contains(got, "CORDONED") {
		t.Errorf("summary does not say nodes are left cordoned:\n%s", got)
	}
}
