// Package selection resolves an operator's node choice into a concrete set.
//
// The operator decides targets: there are no defaults, no automatic targeting
// and no "all nodes" fallback. If nothing is given, the caller prints the
// inventory and exits.
package selection

import (
	"fmt"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/labels"

	"github.com/GlueOps/evac/internal/inventory"
	"github.com/GlueOps/evac/internal/nodefile"
)

// Request is what the operator asked for. At most one source may be set;
// Resolve rejects combinations rather than guessing a precedence.
type Request struct {
	// Names is --nodes a,b,c
	Names []string
	// Selector is --selector k=v
	Selector string
	// File is -f path ("-" for stdin). Empty means "use the default path".
	File string
	// FileExplicit distinguishes an operator-supplied -f from the default path
	// being used implicitly, which changes the error when the file is missing.
	FileExplicit bool
}

// Empty reports whether the operator gave no selection at all.
func (r Request) Empty() bool {
	return len(r.Names) == 0 && r.Selector == "" && r.File == ""
}

// Source describes where the selection came from, for output.
type Source string

const (
	SourceFlags    Source = "flags"
	SourceSelector Source = "selector"
	SourceFile     Source = "file"
)

// Result is the resolved selection.
type Result struct {
	Nodes  []inventory.Node
	Source Source
	// Origin is the file path when Source is SourceFile.
	Origin string

	// DroppedControlPlane names control-plane nodes removed from a flag or
	// selector match. What was dropped and why is reported rather than
	// silently shrinking the set.
	DroppedControlPlane []Dropped
	// NotFound names requested nodes that do not exist in the cluster.
	NotFound []string
}

// Dropped is one excluded node and the signal that excluded it.
type Dropped struct {
	Name   string
	Signal string
}

// ControlPlaneInFileError is returned when a node file names a control-plane
// node.
//
// This is the layer that matters: the node file is hand-editable, so the check
// cannot live only in selection. Flags and selectors filter and report; a file
// refuses outright, because someone typed that name deliberately and the tool
// must not quietly do something different from what was asked.
type ControlPlaneInFileError struct {
	Path  string
	Nodes []Dropped
}

func (e *ControlPlaneInFileError) Error() string {
	names := make([]string, 0, len(e.Nodes))
	for _, n := range e.Nodes {
		names = append(names, fmt.Sprintf("%s (%s)", n.Name, n.Signal))
	}
	return fmt.Sprintf("node file %s names control plane nodes: %s", e.Path, strings.Join(names, ", "))
}

// Resolve turns a Request into a concrete node set.
//
// refuseControlPlane selects the behaviour for file input: drain passes
// true so a hand-edited file is refused, while read-only callers pass false.
func Resolve(req Request, all []inventory.Node, refuseControlPlane bool) (*Result, error) {
	switch {
	case len(req.Names) > 0 && req.Selector != "":
		return nil, fmt.Errorf("--nodes and --selector are mutually exclusive")
	case len(req.Names) > 0 && req.FileExplicit:
		return nil, fmt.Errorf("--nodes and -f are mutually exclusive")
	case req.Selector != "" && req.FileExplicit:
		return nil, fmt.Errorf("--selector and -f are mutually exclusive")
	}

	switch {
	case len(req.Names) > 0:
		return byName(req.Names, all, SourceFlags, "", false)
	case req.Selector != "":
		return bySelector(req.Selector, all)
	case req.File != "":
		f, err := nodefile.Read(req.File)
		if err != nil {
			return nil, err
		}
		if len(f.Nodes) == 0 {
			return nil, fmt.Errorf("node file %s contains no nodes", f.Path)
		}
		return byName(f.Nodes, all, SourceFile, f.Path, refuseControlPlane)
	}
	return nil, fmt.Errorf("no node selection given")
}

func byName(names []string, all []inventory.Node, src Source, origin string, refuse bool) (*Result, error) {
	index := inventory.ByName(all)
	res := &Result{Source: src, Origin: origin}

	seen := make(map[string]bool, len(names))
	for _, raw := range names {
		name := strings.TrimSpace(raw)
		if name == "" || seen[name] {
			continue
		}
		seen[name] = true

		n, ok := index[name]
		if !ok {
			res.NotFound = append(res.NotFound, name)
			continue
		}
		if n.ControlPlane {
			res.DroppedControlPlane = append(res.DroppedControlPlane,
				Dropped{Name: n.Name, Signal: n.ControlPlaneSignal})
			continue
		}
		res.Nodes = append(res.Nodes, *n)
	}

	if refuse && len(res.DroppedControlPlane) > 0 {
		return nil, &ControlPlaneInFileError{Path: origin, Nodes: res.DroppedControlPlane}
	}
	if len(res.NotFound) > 0 {
		return res, fmt.Errorf("node(s) not found in cluster: %s", strings.Join(res.NotFound, ", "))
	}
	sortNodes(res.Nodes)
	return res, nil
}

func bySelector(selector string, all []inventory.Node) (*Result, error) {
	sel, err := labels.Parse(selector)
	if err != nil {
		return nil, fmt.Errorf("parsing --selector: %w", err)
	}
	res := &Result{Source: SourceSelector, Origin: selector}
	for i := range all {
		n := &all[i]
		if !sel.Matches(labels.Set(n.Labels)) {
			continue
		}
		if n.ControlPlane {
			res.DroppedControlPlane = append(res.DroppedControlPlane,
				Dropped{Name: n.Name, Signal: n.ControlPlaneSignal})
			continue
		}
		res.Nodes = append(res.Nodes, *n)
	}
	if len(res.Nodes) == 0 && len(res.DroppedControlPlane) == 0 {
		return nil, fmt.Errorf("--selector %q matched no nodes", selector)
	}
	sortNodes(res.Nodes)
	return res, nil
}

func sortNodes(n []inventory.Node) {
	sort.Slice(n, func(i, j int) bool { return n[i].Name < n[j].Name })
}

// Names returns the selected node names.
func (r *Result) Names() []string {
	out := make([]string, 0, len(r.Nodes))
	for i := range r.Nodes {
		out = append(out, r.Nodes[i].Name)
	}
	return out
}
