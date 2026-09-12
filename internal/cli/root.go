// Package cli wires the three commands §2 defines.
package cli

import (
	"fmt"
	"os"
	"strings"

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

	// Flag parsing failures are usage errors, not runtime errors. Without this
	// an unknown flag or an unparseable value exits 1, which tells a wrapper
	// "the cluster misbehaved" when the truth is "you asked for something that
	// does not exist". Cobra consults the parent chain, so setting it on the
	// root covers every subcommand.
	root.SetFlagErrorFunc(func(_ *cobra.Command, err error) error {
		return exitcode.Wrap(exitcode.Usage, err)
	})

	root.AddCommand(
		newNodesCmd(g),
		newPlanCmd(g),
		newDrainCmd(g),
		newVersionCmd(),
	)

	if err := root.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR  %v\n", err)
		return classify(err)
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

// classify maps an error to an exit code, catching the cobra failures that
// arrive untyped.
//
// Cobra reports an unknown subcommand and a failed argument count as plain
// errors with no distinguishing type, so there is nothing to match on but the
// message. That is unpleasant, and it is still better than reporting a typo as
// an API failure: §9 gives these codes distinct meanings precisely so a wrapper
// can tell "fix your invocation" from "something is broken".
func classify(err error) exitcode.Code {
	if code := exitcode.Of(err); code != exitcode.Error {
		return code
	}
	for _, prefix := range []string{
		"unknown command",
		"unknown flag",
		"unknown shorthand flag",
		"accepts ",
		"requires at least",
		"invalid argument",
		"flag needs an argument",
	} {
		if strings.HasPrefix(err.Error(), prefix) || strings.Contains(err.Error(), prefix) {
			return exitcode.Usage
		}
	}
	return exitcode.Error
}

// usageErr marks an error as a flag or input problem, which §9 gives its own
// exit code so a wrapper can tell "you asked for something impossible" from
// "the cluster misbehaved".
func usageErr(format string, args ...any) error {
	return exitcode.Wrap(exitcode.Usage, fmt.Errorf(format, args...))
}
