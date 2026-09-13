package render

import (
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/GlueOps/evac/internal/classify"
	"github.com/GlueOps/evac/internal/preflight"
	"github.com/GlueOps/evac/internal/scope"
)

// PlanOptions control plan rendering.
type PlanOptions struct {
	Context string
	// Brief collapses the PVC table to per-namespace counts.
	Brief bool
	// Full forces the PVC table open past the auto-collapse threshold.
	Full bool
	// Width overrides terminal detection, for tests.
	Width int
}

// pvcCollapseThreshold is where the PVC table stops being readable. Twenty rows
// scan fine; two hundred do not.
const pvcCollapseThreshold = 50

// Plan renders the plan output.
//
// The ordering is the substance. PVC destruction is the only irreversible part
// of this operation — everything else reschedules — so it leads, above pod
// counts and capacity math. An operator scanning quickly must hit the permanent
// losses first, not last.
func Plan(w io.Writer, s *scope.Scope, pf *preflight.Results, opts PlanOptions) error {
	if err := planPVCs(w, s, opts); err != nil {
		return err
	}
	planGuardStatus(w, s)
	planLeftovers(w, s, opts)
	planUnmanaged(w, s, opts)
	if err := planEverythingElse(w, s, opts); err != nil {
		return err
	}
	planPreflight(w, pf)
	return nil
}

// 1. PVCs to be destroyed.
func planPVCs(w io.Writer, s *scope.Scope, opts PlanOptions) error {
	targets := s.DeletablePVCs()

	if len(targets) == 0 {
		if s.Provider.Disabled {
			return nil // the guard block below explains why
		}
		fmt.Fprintf(w, "No PVCs will be destroyed.\n\n")
		return nil
	}

	fmt.Fprintf(w, "PVCs to be DESTROYED (%d node(s), %s):\n\n", len(s.Nodes), opts.Context)

	collapse := opts.Brief || (!opts.Full && len(targets) > pvcCollapseThreshold)
	if collapse {
		counts := map[string]int{}
		for _, t := range targets {
			counts[t.PVC.Namespace]++
		}
		rows := sortedCounts(counts)
		err := Table{
			Rows:  len(rows),
			Width: opts.Width,
			Columns: []Column{
				{Header: "NAMESPACE", Value: func(i int) string { return rows[i].key }},
				{Header: "PVCS", Value: func(i int) string { return fmt.Sprint(rows[i].n) }},
			},
		}.Render(indent(w))
		if err != nil {
			return err
		}
		if !opts.Brief {
			fmt.Fprintf(w, "\n  %d PVCs — collapsed for readability; --full expands\n", len(targets))
		} else {
			fmt.Fprintf(w, "\n  %d PVCs\n", len(targets))
		}
	} else {
		err := Table{
			Rows:  len(targets),
			Width: opts.Width,
			Columns: []Column{
				{Header: "NAMESPACE", Value: func(i int) string { return targets[i].PVC.Namespace }},
				{Header: "POD", Value: func(i int) string { return targets[i].Pod.Name }},
				{Header: "PVC", Value: func(i int) string { return targets[i].PVC.Name }},
				{Header: "SOURCE", Value: func(i int) string { return strings.ToLower(targets[i].Decision.Source) }},
			},
		}.Render(indent(w))
		if err != nil {
			return err
		}
		fmt.Fprintf(w, "\n  %d PVCs\n", len(targets))
	}

	// PVCs already marked for deletion are unfinished work from a previous run,
	// not new losses — worth distinguishing so a re-run reads honestly.
	var resuming int
	for _, t := range targets {
		if t.AlreadyDeleting {
			resuming++
		}
	}
	if resuming > 0 {
		fmt.Fprintf(w, "  (%d already carry a deletionTimestamp from an earlier run)\n", resuming)
	}
	fmt.Fprintln(w)
	return nil
}

// 2. Guard status — shown only when deletion is suppressed cluster-wide.
// On a normal cluster nothing is printed here, and per-PVC keeps are never
// reported: they are not a loss and do not need review.
func planGuardStatus(w io.Writer, s *scope.Scope) {
	if !s.Provider.Disabled {
		return
	}
	fmt.Fprintf(w, "NOTE  PVC deletion disabled: CSI-backed or AWS cluster detected\n")
	fmt.Fprintf(w, "      signal: %s\n", s.Provider.Signal)
	fmt.Fprintf(w, "      %d PVC(s) in scope will NOT be deleted; pods will rebind on reschedule\n\n", len(s.PVCs))
}

// 2b. Leftover work from a previous run, found via PV-side discovery.
//
// Shown only when there is something to show. It appears near the top because
// it is unfinished destructive work: a PVC already marked for deletion is data
// that dies the moment its pod restarts, whether or not this run touches it.
func planLeftovers(w io.Writer, s *scope.Scope, opts PlanOptions) {
	left := s.Leftovers()
	if len(left) == 0 {
		return
	}
	fmt.Fprintf(w, "Leftover work from an earlier run (found via PV nodeAffinity):\n\n")
	_ = Table{
		Rows:  len(left),
		Width: opts.Width,
		Columns: []Column{
			{Header: "PV", Value: func(i int) string { return left[i].PV.Name }},
			{Header: "NODE", Value: func(i int) string { return left[i].Node }},
			{Header: "CLAIM", Value: func(i int) string {
				if left[i].PVC == nil {
					return "-"
				}
				return left[i].PVC.Namespace + "/" + left[i].PVC.Name
			}},
			{Header: "WHY", Value: func(i int) string { return left[i].Reason }},
		},
	}.Render(indent(w))
	fmt.Fprintln(w)
}

// 3. Permanent pod losses.
func planUnmanaged(w io.Writer, s *scope.Scope, opts PlanOptions) {
	un := s.Unmanaged()
	if len(un) == 0 {
		return
	}
	fmt.Fprintf(w, "Unmanaged pods (no controller — will NOT be recreated):\n\n")
	_ = Table{
		Rows:  len(un),
		Width: opts.Width,
		Columns: []Column{
			{Header: "NAMESPACE", Value: func(i int) string { return un[i].Pod.Namespace }},
			{Header: "POD", Value: func(i int) string { return un[i].Pod.Name }},
			{Header: "NODE", Value: func(i int) string { return un[i].Pod.Spec.NodeName }},
		},
	}.Render(indent(w))
	fmt.Fprintln(w)
}

// 4. Everything else.
func planEverythingElse(w io.Writer, s *scope.Scope, opts PlanOptions) error {
	fmt.Fprintf(w, "Target: %s\n", opts.Context)
	fmt.Fprintf(w, "Nodes:  %s\n\n", strings.Join(nodeNames(s), ", "))

	normal := s.ByClass(classify.Normal)
	fragile := s.ByClass(classify.Fragile)
	excluded := s.ByClass(classify.Excluded)

	var ds, jobs, mirror, helper int
	for _, r := range excluded {
		switch r.Exclusion {
		case classify.ExcludedDaemonSet:
			ds++
		case classify.ExcludedJob:
			jobs++
		case classify.ExcludedMirror:
			mirror++
		case classify.ExcludedStorageHelper:
			helper++
		}
	}

	fmt.Fprintf(w, "Pods on selected nodes:\n")
	fmt.Fprintf(w, "  %4d normal    (evicted via the eviction API, PDBs respected)\n", len(normal))
	fmt.Fprintf(w, "  %4d fragile   (deleted directly — eviction would be refused)\n", len(fragile))
	fmt.Fprintf(w, "  %4d excluded  (%d DaemonSet, %d Job, %d mirror, %d storage-helper)\n\n", len(excluded), ds, jobs, mirror, helper)

	if len(fragile) > 0 {
		fmt.Fprintf(w, "Fragile pods:\n\n")
		_ = Table{
			Rows:  len(fragile),
			Width: opts.Width,
			Columns: []Column{
				{Header: "NAMESPACE", Value: func(i int) string { return fragile[i].Pod.Namespace }},
				{Header: "POD", Value: func(i int) string { return fragile[i].Pod.Name }},
				{Header: "WHY", Value: func(i int) string { return reasons(fragile[i]) }},
			},
		}.Render(indent(w))
		fmt.Fprintln(w)

		// The subset that actually goes down. The rest of the bucket would have
		// evicted cleanly; this is where the downtime lands.
		if dt := s.DowntimeBearing(); len(dt) > 0 {
			fmt.Fprintf(w, "  Of those, %d will take downtime (single replica held by a PDB allowing zero disruptions):\n", len(dt))
			for _, r := range dt {
				fmt.Fprintf(w, "    %s/%s  (%s %s, PDB %s)\n",
					r.Pod.Namespace, r.Pod.Name, r.ControllerKind, r.ControllerName,
					strings.Join(r.MatchingPDBs, ","))
			}
			fmt.Fprintln(w)
		}
	}

	if jp := s.JobPods(); len(jp) > 0 {
		fmt.Fprintf(w, "Job pods that will be waited on (phase 4): %d\n\n", len(jp))
	}

	fmt.Fprintf(w, "Draining %d node(s) affects:\n\n", len(s.Nodes))
	ns := s.ByNamespace()
	return Table{
		Rows:  len(ns),
		Width: opts.Width,
		Columns: []Column{
			{Header: "NAMESPACE", Value: func(i int) string { return ns[i].Namespace }},
			{Header: "PODS", Value: func(i int) string { return fmt.Sprint(ns[i].Pods) }},
			{Header: "PVCS", Value: func(i int) string { return fmt.Sprint(ns[i].PVCs) }},
			{Header: "NOTE", Value: func(i int) string {
				if ns[i].NoPDB > 0 {
					return fmt.Sprintf("%d pod(s) have no PDB", ns[i].NoPDB)
				}
				return ""
			}},
		},
	}.Render(indent(w))
}

// 5. Preflight.
func planPreflight(w io.Writer, pf *preflight.Results) {
	if pf == nil {
		return
	}
	fmt.Fprintf(w, "\nPreflight:\n")
	fmt.Fprint(w, pf.Summary())
}

// ConfirmationSummary is the block shown immediately above the prompt.
//
// The PVC table may have scrolled off by the time the operator reaches the
// prompt, and this is the line their eye lands on — so the irreversible items
// are repeated here, and in the same order: permanent losses first, recoverable
// eviction last.
func ConfirmationSummary(s *scope.Scope) string {
	var b strings.Builder
	fmt.Fprintf(&b, "About to drain %d node(s) in %s:\n", len(s.Nodes), s.Snapshot.Context)
	// Naming them here matters more than anywhere else on the page. The
	// incident this block exists for was an operator mistaken about *which*
	// nodes, and the one line they are certain to read is the one above the
	// prompt. The names appear once more, far above, where the tables have
	// long since scrolled off.
	fmt.Fprintf(&b, "    %s\n", confirmNodeNames(s))

	if n := len(s.DeletablePVCs()); n > 0 {
		fmt.Fprintf(&b, "  %d PVCs DESTROYED\n", n)
	} else if s.Provider.Disabled {
		fmt.Fprintf(&b, "  0 PVCs destroyed (deletion disabled: %s)\n", s.Provider.Signal)
	}
	if n := len(s.Unmanaged()); n > 0 {
		fmt.Fprintf(&b, "  %d unmanaged pods deleted permanently\n", n)
	}
	if n := len(s.DowntimeBearing()); n > 0 {
		fmt.Fprintf(&b, "  %d single-replica workloads will go down\n", n)
	}
	fmt.Fprintf(&b, "  %d pods evicted\n", s.Evictable())
	// Stated before the prompt, not only afterwards. Cordon is one-way and
	// evac never uncordons, so this is part of what is being approved.
	fmt.Fprintf(&b, "  all %d node(s) left CORDONED afterwards; evac never uncordons\n", len(s.Nodes))
	return b.String()
}

// confirmNodeNames lists the nodes, truncated so a large selection stays one
// line rather than pushing the counts off the top of the terminal.
func confirmNodeNames(s *scope.Scope) string {
	const show = 6
	names := make([]string, 0, len(s.Nodes))
	for i := range s.Nodes {
		names = append(names, s.Nodes[i].Name)
	}
	sort.Strings(names)
	if len(names) > show {
		return strings.Join(names[:show], ", ") +
			fmt.Sprintf(", and %d more", len(names)-show)
	}
	return strings.Join(names, ", ")
}

// --- helpers ---------------------------------------------------------------

func reasons(r classify.Result) string {
	out := make([]string, 0, len(r.FragileReasons))
	for _, fr := range r.FragileReasons {
		out = append(out, string(fr))
	}
	return strings.Join(out, ",")
}

func nodeNames(s *scope.Scope) []string {
	out := make([]string, 0, len(s.Nodes))
	for i := range s.Nodes {
		out = append(out, s.Nodes[i].Name)
	}
	return out
}

type kv struct {
	key string
	n   int
}

func sortedCounts(m map[string]int) []kv {
	out := make([]kv, 0, len(m))
	for k, v := range m {
		out = append(out, kv{k, v})
	}
	for i := range out {
		for j := i + 1; j < len(out); j++ {
			if out[j].key < out[i].key {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}

// indent wraps a writer so table output is inset under its heading.
func indent(w io.Writer) io.Writer { return &indentWriter{w: w, prefix: "  "} }

type indentWriter struct {
	w       io.Writer
	prefix  string
	midLine bool
}

func (iw *indentWriter) Write(p []byte) (int, error) {
	for _, b := range p {
		if !iw.midLine {
			if _, err := io.WriteString(iw.w, iw.prefix); err != nil {
				return 0, err
			}
			iw.midLine = true
		}
		if _, err := iw.w.Write([]byte{b}); err != nil {
			return 0, err
		}
		if b == '\n' {
			iw.midLine = false
		}
	}
	return len(p), nil
}
