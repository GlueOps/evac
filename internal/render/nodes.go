package render

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/labels"

	"github.com/GlueOps/evac/internal/inventory"
)

// NodeTableOptions carries the label display policy and sort choice.
type NodeTableOptions struct {
	// LabelColumns promotes specific labels to their own columns, mirroring
	// `kubectl get nodes -L` so the muscle memory carries over.
	LabelColumns []string
	// ShowLabels dumps every label, for investigation.
	ShowLabels bool
	// SortBy is "name", "uptime" or "kubelet".
	SortBy string
	// Width overrides terminal detection, for tests.
	Width int
}

// NodeTable renders the node inventory.
//
// Control-plane nodes are shown rather than hidden: an operator who cannot find
// a node will go looking, and silence is worse than an explanation. They are
// annotated in the STATUS column as excluded.
func NodeTable(w io.Writer, nodes []inventory.Node, opts NodeTableOptions) error {
	rows := make([]inventory.Node, len(nodes))
	copy(rows, nodes)
	sortNodes(rows, opts.SortBy)

	cols := []Column{
		{Header: "NAME", Drop: 0, Value: func(i int) string { return rows[i].Name }},
		{Header: "STATUS", Drop: 0, Value: func(i int) string { return status(rows[i]) }},
		{Header: "AGE", Drop: 4, Value: func(i int) string { return duration(rows[i].Age) }},
		{Header: "UPTIME*", Drop: 3, Value: func(i int) string { return duration(rows[i].Uptime) }},
		{Header: "KUBELET", Drop: 2, Value: func(i int) string { return rows[i].KubeletVersion }},
		{Header: "PODS", Drop: 1, Value: func(i int) string { return fmt.Sprint(rows[i].Pods) }},
		{Header: "PVCS", Drop: 1, Value: func(i int) string { return fmt.Sprint(rows[i].PVCs) }},
		{Header: "KERNEL", Drop: 6, Value: func(i int) string { return rows[i].KernelVersion }},
		{Header: "OS-IMAGE", Drop: 7, Value: func(i int) string { return rows[i].OSImage }},
	}

	// --label-columns promotes chosen labels to their own columns.
	for _, key := range opts.LabelColumns {
		cols = append(cols, Column{
			Header: strings.ToUpper(key),
			Drop:   5,
			Value:  func(i int) string { return rows[i].Labels[key] },
		})
	}

	switch {
	case opts.ShowLabels:
		cols = append(cols, Column{
			Header: "LABELS", Drop: 8,
			Value: func(i int) string { return labels.Set(rows[i].Labels).String() },
		})
	case len(opts.LabelColumns) == 0:
		// Default: the curated subset — the labels an operator chose to set,
		// without the well-known noise every node carries.
		cols = append(cols, Column{
			Header: "LABELS", Drop: 8,
			Value: func(i int) string { return inventory.CuratedLabels(rows[i].Labels) },
		})
	}

	return Table{Columns: cols, Rows: len(rows), Width: opts.Width}.Render(w)
}

// status combines the three things an operator reads at a glance: whether the
// node is healthy, whether it is already cordoned, and whether this tool will
// refuse to drain it.
func status(n inventory.Node) string {
	var parts []string
	if n.Ready {
		parts = append(parts, "Ready")
	} else {
		parts = append(parts, "NotReady")
	}
	if !n.Schedulable {
		parts = append(parts, "Cordoned")
	}
	if n.ControlPlane {
		parts = append(parts, "control-plane/excluded")
	}
	return strings.Join(parts, ",")
}

func sortNodes(n []inventory.Node, by string) {
	switch by {
	case "uptime":
		sort.SliceStable(n, func(i, j int) bool { return n[i].Uptime < n[j].Uptime })
	case "kubelet":
		sort.SliceStable(n, func(i, j int) bool { return n[i].KubeletVersion < n[j].KubeletVersion })
	default:
		sort.SliceStable(n, func(i, j int) bool { return n[i].Name < n[j].Name })
	}
}

// duration formats an age the way kubectl does: coarse and scannable, because
// nobody reads "1823h14m" as "76 days".
func duration(d time.Duration) string {
	if d <= 0 {
		return "<unknown>"
	}
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh%dm", int(d.Hours()), int(d.Minutes())%60)
	case d < 365*24*time.Hour:
		return fmt.Sprintf("%dd", int(d.Hours()/24))
	default:
		years := int(d.Hours() / 24 / 365)
		days := int(d.Hours()/24) % 365
		return fmt.Sprintf("%dy%dd", years, days)
	}
}
