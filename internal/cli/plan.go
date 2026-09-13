package cli

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/GlueOps/evac/internal/exitcode"
	"github.com/GlueOps/evac/internal/inventory"
	"github.com/GlueOps/evac/internal/kube"
	"github.com/GlueOps/evac/internal/nodefile"
	"github.com/GlueOps/evac/internal/preflight"
	"github.com/GlueOps/evac/internal/render"
	"github.com/GlueOps/evac/internal/scope"
	"github.com/GlueOps/evac/internal/selection"
)

// selectionFlags are shared by plan and drain, so the two cannot drift.
type selectionFlags struct {
	file     string
	nodes    []string
	selector string
}

func (sf *selectionFlags) register(cmd *cobra.Command) {
	f := cmd.Flags()
	f.StringVarP(&sf.file, "file", "f", "",
		"node file to read ('-' for stdin); defaults to the per-context path")
	f.StringSliceVar(&sf.nodes, "nodes", nil, "node names to target (comma-separated)")
	f.StringVar(&sf.selector, "selector", "", "label selector for target nodes")
}

// resolve loads the cluster, builds the inventory, and turns the flags into a
// concrete node set.
//
// refuseControlPlane splits the two behaviours: a node file naming a control
// plane node is refused outright, because the file is hand-editable and the
// check cannot live only in the selection path.
func (sf *selectionFlags) resolve(cmd *cobra.Command, g *globals, refuseControlPlane bool) (*kube.Client, *kube.Snapshot, *selection.Result, error) {
	cl, err := g.client()
	if err != nil {
		return nil, nil, nil, exitcode.Wrap(exitcode.Error, err)
	}

	snap, err := cl.FullSnapshot(context.Background())
	if err != nil {
		return nil, nil, nil, exitcode.Wrap(exitcode.Error, err)
	}
	all := inventory.Build(snap)

	if inventory.SingleNodeCluster(all) {
		fmt.Fprintf(cmd.OutOrStdout(),
			"%s is a single-node cluster: %s is both control plane and worker.\n"+
				"Nothing is drainable — evac drains worker nodes only.\n",
			cl.Target(), all[0].Name)
		return nil, nil, nil, exitcode.Wrap(exitcode.Usage, fmt.Errorf("no drainable nodes"))
	}

	req := selection.Request{
		Names:        sf.nodes,
		Selector:     sf.selector,
		File:         sf.file,
		FileExplicit: sf.file != "",
	}
	// Falling back to the default node file is what makes `evac nodes -i`
	// followed by a bare `evac drain` the common path.
	if req.Empty() {
		req.File = nodefile.DefaultPath(cl.Context)
	}

	sel, err := selection.Resolve(req, all, refuseControlPlane)
	if err != nil {
		return nil, nil, nil, classifySelectionError(err, req.File, req.FileExplicit)
	}
	if len(sel.Nodes) == 0 {
		return nil, nil, nil, exitcode.Wrap(exitcode.Usage, fmt.Errorf("no drainable nodes selected"))
	}

	reportDrops(cmd, sel)
	return cl, snap, sel, nil
}

// classifySelectionError maps selection failures onto evac's exit codes.
func classifySelectionError(err error, path string, explicit bool) error {
	var cp *selection.ControlPlaneInFileError
	if asControlPlaneErr(err, &cp) {
		return exitcode.Wrap(exitcode.ControlPlane, fmt.Errorf(
			"%w\n\nevac drains worker nodes only: draining a control plane node risks etcd\nquorum loss. There is no override flag. Remove those lines and re-run", err))
	}
	if !explicit {
		return exitcode.Wrap(exitcode.Usage, fmt.Errorf(
			"%w\n\nNo node selection given and the default node file %s was not usable.\nRun `evac nodes -i` to choose nodes, or pass --nodes/--selector/-f", err, path))
	}
	return exitcode.Wrap(exitcode.Usage, err)
}

func reportDrops(cmd *cobra.Command, sel *selection.Result) {
	if len(sel.DroppedControlPlane) == 0 {
		return
	}
	out := cmd.ErrOrStderr()
	fmt.Fprintf(out, "NOTE  %d control plane node(s) dropped from the selection:\n", len(sel.DroppedControlPlane))
	for _, d := range sel.DroppedControlPlane {
		fmt.Fprintf(out, "        %s (%s)\n", d.Name, d.Signal)
	}
	fmt.Fprintln(out)
}

func newPlanCmd(g *globals) *cobra.Command {
	var (
		sf    selectionFlags
		brief bool
		full  bool
	)

	cmd := &cobra.Command{
		Use:   "plan",
		Short: "Show the blast radius and preflight results without changing anything",
		Long: `Show what a drain would do, without mutating anything.

Read-only, and optional: drain runs the same plan itself, against the same live
state it is about to act on. This command exists for reviewing ahead of a
maintenance window or attaching to a change ticket.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if brief && full {
				return usageErr("--brief and --full are mutually exclusive")
			}

			// Read-only: the control-plane file refusal does not apply, and the
			// lock is never taken — an operator must always be able to look at
			// a cluster while a drain is running.
			cl, snap, sel, err := sf.resolve(cmd, g, false)
			if err != nil {
				return err
			}

			sc, err := scope.Build(snap, sel.Nodes, cl.Context)
			if err != nil {
				return exitcode.Wrap(exitcode.Error, err)
			}
			pf := preflight.Run(sc)

			out := cmd.OutOrStdout()
			if err := render.Plan(out, sc, pf, render.PlanOptions{
				Context: cl.Target(),
				Brief:   brief,
				Full:    full,
			}); err != nil {
				return exitcode.Wrap(exitcode.Error, err)
			}

			// Preflight failures block a drain, so plan reports them as a
			// non-zero exit too: a plan that says "this will not work" should
			// not look like success to a pipeline.
			if pf.Failed() {
				return exitcode.Wrap(exitcode.Preflight, fmt.Errorf("preflight failed — drain would be refused"))
			}
			return nil
		},
	}

	sf.register(cmd)
	f := cmd.Flags()
	f.BoolVar(&brief, "brief", false, "collapse the PVC list to per-namespace counts")
	f.BoolVar(&full, "full", false, "show the full PVC list even when long")
	return cmd
}
