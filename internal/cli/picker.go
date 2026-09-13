package cli

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/GlueOps/evac/internal/exitcode"
	"github.com/GlueOps/evac/internal/inventory"
	"github.com/GlueOps/evac/internal/kube"
	"github.com/GlueOps/evac/internal/nodefile"
	"github.com/GlueOps/evac/internal/picker"
)

// runPicker renders the interactive selection UI and writes the node file.
//
// It never drains. On exit it writes the chosen nodes and prints the resolved
// path, which is what `evac plan` and `evac drain` then read when -f is omitted.
func runPicker(cmd *cobra.Command, cl *kube.Client, nodes []inventory.Node, takenAt, outPath string) error {
	if !term.IsTerminal(int(os.Stdin.Fd())) {
		return exitcode.Wrap(exitcode.Usage, fmt.Errorf(
			"-i needs a terminal; use --nodes or --selector, or pipe a list into `evac drain -f -`"))
	}

	res, err := picker.Run(nodes, picker.Options{
		Context: cl.Target(),
		TakenAt: parseTime(takenAt),
	})
	if err != nil {
		return exitcode.Wrap(exitcode.Error, err)
	}
	if res.Cancelled {
		fmt.Fprintln(cmd.ErrOrStderr(), "\nNo nodes selected; node file not written.")
		return nil
	}

	path := outPath
	if path == "" {
		path = nodefile.DefaultPath(cl.Context)
	}
	if err := nodefile.Write(path, cl.Context, res.Selected); err != nil {
		return exitcode.Wrap(exitcode.Error, err)
	}

	out := cmd.OutOrStdout()
	fmt.Fprintf(out, "\nWrote %d node(s) to %s\n", len(res.Selected), path)
	for _, n := range res.Selected {
		fmt.Fprintf(out, "  %s\n", n)
	}
	// Naming the next command matters: the whole point of the per-context
	// default path is that drain can then be run with no arguments.
	fmt.Fprintf(out, "\nNext:  evac plan     # review\n       evac drain    # execute\n")
	return nil
}
