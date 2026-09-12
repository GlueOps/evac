// Package cli wires the three commands §2 defines.
package cli

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/GlueOps/evac/internal/exitcode"
	"github.com/GlueOps/evac/internal/kube"
)

// Build info, stamped at link time.
var (
	version = "dev"
	commit  = "unknown"
	date    = "unknown"
)

// SetBuildInfo is called from main with the ldflags-stamped values.
func SetBuildInfo(v, c, d string) { version, commit, date = v, c, d }

// globals holds the flags every command shares.
//
// Only two, deliberately. §1 rejects inheriting kubectl's flag surface, and
// k8s.io/cli-runtime's genericclioptions would register around twenty in one
// shot — including -n, which means nothing to a node-scoped, multi-namespace
// operation.
type globals struct {
	kubeconfig string
	context    string
}

func (g *globals) client() (*kube.Client, error) {
	return kube.New(kube.Options{Kubeconfig: g.kubeconfig, Context: g.context})
}

// Execute runs the CLI and returns the process exit code.
//
// Errors are returned up to here rather than exiting in place: §10 requires
// that no worker call os.Exit, because it skips defers and discards buffered
// log lines — and for a tool whose transcript is the audit trail, those final
// lines are the most important ones.
func Execute() exitcode.Code {
	g := &globals{}
	kube.SilenceKlog()

	root := &cobra.Command{
		Use:   "evac",
		Short: "Drain Kubernetes worker nodes and destroy their local PVCs",
		Long: `evac drains Kubernetes worker nodes and destroys their local PVCs.

This tool destroys data by design. It deletes the PersistentVolumeClaims of
every pod it evicts, and is built for clusters using node-local storage where
that data is regenerable. It refuses to delete anything not backed by a local
volume, and refuses entirely on CSI-backed or AWS/EKS clusters.

It operates on the active kubecontext and drains worker nodes only.`,
		SilenceUsage:  true,
		SilenceErrors: true,
		// No default action: §1 requires the operator choose targets, so a bare
		// invocation shows help rather than guessing.
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}

	pf := root.PersistentFlags()
	pf.StringVar(&g.kubeconfig, "kubeconfig", "", "path to the kubeconfig file (overrides $KUBECONFIG)")
	pf.StringVar(&g.context, "context", "", "kubecontext to use (defaults to the active one)")

	root.AddCommand(
		newNodesCmd(g),
		newPlanCmd(g),
		newDrainCmd(g),
		newVersionCmd(),
	)

	if err := root.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR  %v\n", err)
		return exitcode.Of(err)
	}
	return exitcode.OK
}

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			fmt.Fprintf(cmd.OutOrStdout(), "evac %s (commit %s, built %s)\n", version, commit, date)
			return nil
		},
	}
}

// usageErr marks an error as a flag or input problem, which §9 gives its own
// exit code so a wrapper can tell "you asked for something impossible" from
// "the cluster misbehaved".
func usageErr(format string, args ...any) error {
	return exitcode.Wrap(exitcode.Usage, fmt.Errorf(format, args...))
}
