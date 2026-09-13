// Package picker provides the interactive node-selection UI behind
// `evac nodes -i`.
//
// Strictly a selection aid: it reads, it never mutates, and it never drains.
// The drain runs as a separate invocation, which keeps the destructive path
// free of TUI code and keeps the execution transcript in normal scrollback.
package picker

import (
	"errors"
	"fmt"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/charmbracelet/huh"

	"github.com/GlueOps/evac/internal/inventory"
)

// Result is what the operator chose.
type Result struct {
	Selected []string
	// Cancelled is true when the picker was dismissed without confirming.
	Cancelled bool
}

// Options configure the picker.
type Options struct {
	Context string
	TakenAt time.Time
	// RunForm is a test seam; nil runs the real interactive form.
	RunForm func(*huh.Form) error
}

// Run renders the selectable inventory and returns the chosen node names.
//
// Built on huh's MultiSelect rather than a hand-rolled table component. The
// option strings are column-aligned with tabwriter, which in a monospace
// terminal reads as a table — so the inventory columns survive without
// rebuilding filtering, multi-select and selection-set handling from scratch.
//
// Two deliberate reductions follow from that choice, because huh supports
// neither: there is no in-picker sort (--sort-by applies to `evac nodes`, and
// the picker renders in that order), and control plane nodes are named in a
// note above the picker rather than rendered as unselectable rows. That still
// shows rather than hides — an operator who cannot find a node will go
// looking — without needing per-row state.
func Run(nodes []inventory.Node, opts Options) (*Result, error) {
	drainable := inventory.Drainable(nodes)
	if len(drainable) == 0 {
		return nil, fmt.Errorf("no drainable nodes: every node in %s is control plane", opts.Context)
	}

	labels := AlignRows(drainable)
	options := make([]huh.Option[string], 0, len(drainable))
	for i := range drainable {
		options = append(options, huh.NewOption(labels[i], drainable[i].Name))
	}

	var selected []string
	ms := huh.NewMultiSelect[string]().
		Title(Title(nodes, opts)).
		Description("space selects · / filters · enter confirms").
		Options(options...).
		Value(&selected).
		Filterable(true).
		Height(pickerHeight(len(drainable)))

	form := huh.NewForm(huh.NewGroup(ms))

	run := opts.RunForm
	if run == nil {
		run = func(f *huh.Form) error { return f.Run() }
	}
	if err := run(form); err != nil {
		if errors.Is(err, huh.ErrUserAborted) {
			return &Result{Cancelled: true}, nil
		}
		return nil, err
	}
	if len(selected) == 0 {
		return &Result{Cancelled: true}, nil
	}
	return &Result{Selected: selected}, nil
}

// Title carries the snapshot timestamp and the control-plane exclusion note.
//
// The timestamp matters because there is no background refresh: someone who has
// had this open for twenty minutes should know what they are looking at.
// Staleness is safe — the drain re-derives scope from live state anyway — but
// silence about it is not.
func Title(all []inventory.Node, opts Options) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Select nodes to drain — %s", opts.Context)
	if !opts.TakenAt.IsZero() {
		fmt.Fprintf(&b, "  (snapshot %s)", opts.TakenAt.UTC().Format("15:04:05Z"))
	}
	if cp := inventory.ControlPlaneNames(all); len(cp) > 0 {
		fmt.Fprintf(&b, "\n%d control plane node(s) excluded: %s", len(cp), strings.Join(cp, ", "))
	}
	return b.String()
}

// AlignRows renders each node as a column-aligned line.
func AlignRows(nodes []inventory.Node) []string {
	var buf strings.Builder
	tw := tabwriter.NewWriter(&buf, 0, 0, 2, ' ', 0)
	for i := range nodes {
		n := &nodes[i]
		status := "Ready"
		if !n.Ready {
			status = "NotReady"
		}
		if !n.Schedulable {
			status += ",Cordoned"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%d pods\t%d PVCs\t%s\n",
			n.Name, status, shortDuration(n.Uptime), n.Pods, n.PVCs, n.KubeletVersion)
	}
	_ = tw.Flush()

	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	for i := range lines {
		lines[i] = strings.TrimRight(lines[i], " ")
	}
	return lines
}

// pickerHeight keeps the list on screen without swallowing the terminal.
func pickerHeight(n int) int {
	const minH, maxH = 5, 20
	switch {
	case n < minH:
		return minH
	case n > maxH:
		return maxH
	default:
		return n + 1
	}
}

func shortDuration(d time.Duration) string {
	switch {
	case d <= 0:
		return "?"
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd", int(d.Hours()/24))
	}
}
