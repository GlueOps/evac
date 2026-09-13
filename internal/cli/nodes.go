package cli

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/GlueOps/evac/internal/exitcode"
	"github.com/GlueOps/evac/internal/inventory"
	"github.com/GlueOps/evac/internal/render"
)

func newNodesCmd(g *globals) *cobra.Command {
	var (
		labelColumns []string
		showLabels   bool
		sortBy       string
		interactive  bool
		outPath      string
	)

	cmd := &cobra.Command{
		Use:   "nodes",
		Short: "Show the node inventory; -i to select nodes and write a node file",
		Long: `Show the node inventory for the active cluster.

Read-only. With -i the same table becomes selectable and the chosen nodes are
written to a node file that plan and drain read.

Control plane nodes are shown but never selectable: draining them risks etcd
quorum loss, so they are excluded at every layer.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			switch sortBy {
			case "", "name", "uptime", "kubelet":
			default:
				return usageErr("--sort-by %q: want one of name, uptime, kubelet", sortBy)
			}

			cl, err := g.client()
			if err != nil {
				return exitcode.Wrap(exitcode.Error, err)
			}

			snap, err := cl.Inventory(context.Background())
			if err != nil {
				return exitcode.Wrap(exitcode.Error, err)
			}
			nodes := inventory.Build(snap)

			if len(nodes) == 0 {
				return exitcode.Wrap(exitcode.Error, fmt.Errorf("cluster %s reports no nodes", cl.Target()))
			}

			// An edge case worth its own message: a single-node install is
			// both control plane and worker, so nothing is drainable and an
			// empty selection would be misleading.
			if inventory.SingleNodeCluster(nodes) {
				fmt.Fprintf(cmd.OutOrStdout(),
					"%s is a single-node cluster: %s is both control plane and worker.\n"+
						"Nothing is drainable — evac drains worker nodes only.\n",
					cl.Target(), nodes[0].Name)
				return exitcode.Wrap(exitcode.Usage, fmt.Errorf("no drainable nodes"))
			}

			out := cmd.OutOrStdout()
			fmt.Fprintf(out, "Cluster: %s   snapshot %s\n\n", cl.Target(), snap.TakenAt.Format("2006-01-02T15:04:05Z"))

			if err := render.NodeTable(out, nodes, render.NodeTableOptions{
				LabelColumns: labelColumns,
				ShowLabels:   showLabels,
				SortBy:       sortBy,
			}); err != nil {
				return exitcode.Wrap(exitcode.Error, err)
			}

			fmt.Fprintf(out, "\n* UPTIME is time since the Ready condition last changed, which is a proxy\n"+
				"  for boot time: it reflects the last Ready flap, not a reboot.\n")

			if cp := inventory.ControlPlaneNames(nodes); len(cp) > 0 {
				fmt.Fprintf(out, "\n%d control plane node(s) excluded from draining: %s\n",
					len(cp), joinNames(cp))
			}

			if interactive {
				return runPicker(cmd, cl, nodes, snap.TakenAt.Format(time.RFC3339), outPath)
			}
			return nil
		},
	}

	f := cmd.Flags()
	f.StringSliceVarP(&labelColumns, "label-columns", "L", nil,
		"promote these labels to their own columns (comma-separated), as kubectl get nodes -L")
	f.BoolVar(&showLabels, "show-labels", false, "show all labels rather than the curated subset")
	f.StringVar(&sortBy, "sort-by", "name", "sort by: name, uptime, kubelet")
	f.BoolVarP(&interactive, "interactive", "i", false, "select nodes and write a node file")
	f.StringVarP(&outPath, "out", "o", "", "node file to write with -i (defaults to the per-context path)")

	return cmd
}

func joinNames(names []string) string {
	out := ""
	for i, n := range names {
		if i > 0 {
			out += ", "
		}
		out += n
	}
	return out
}
