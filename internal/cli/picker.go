package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

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
	path := outPath
	if path == "" {
		p, err := nodefile.DefaultPath(cl.Context)
		if err != nil {
			return exitcode.Wrap(exitcode.Error, err)
		}
		path = p
	}
	if abs, err := filepath.Abs(path); err == nil {
		path = abs
	}

	// "node file not written" is true and reads as "nothing is queued", which
	// is the opposite of what has happened: an earlier selection is still on
	// disk and a bare `evac drain` will use it. Say what the state IS.
	if res.Cancelled || res.ClearedSelection {
		errOut := cmd.ErrOrStderr()
		verb := "Cancelled"
		if res.ClearedSelection {
			verb = "No nodes selected"
		}
		if existing, err := nodefile.Read(path); err == nil && len(existing.Nodes) > 0 {
			fmt.Fprintf(errOut, "\n%s; nothing written. The existing node file is UNCHANGED and\n"+
				"still what a bare `evac drain` will use:\n  %s\n  %d node(s): %s\nDelete it to clear the default selection.\n",
				verb, path, len(existing.Nodes), strings.Join(existing.Nodes, ", "))
			return nil
		}
		fmt.Fprintf(errOut, "\n%s; nothing written, and no node file at\n  %s\n", verb, path)
		return nil
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
	// default path is that drain can then be run with no arguments. With -o
	// there is no such default, so the bare commands would read a different
	// file — an older selection, or none — and the hint has to carry -f.
	if outPath != "" {
		fmt.Fprintf(out, "\nNext:  evac plan -f %s     # review\n       evac drain -f %s    # execute\n", path, path)
	} else {
		fmt.Fprintf(out, "\nNext:  evac plan     # review\n       evac drain    # execute\n")
	}
	return nil
}
