package cli

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/GlueOps/evac/internal/exitcode"
	"github.com/GlueOps/evac/internal/inventory"
	"github.com/GlueOps/evac/internal/kube"
)

// runPicker is implemented in picker_tui.go. Declared here so the nodes command
// compiles independently of the TUI.
func runPicker(cmd *cobra.Command, cl *kube.Client, nodes []inventory.Node, outPath string) error {
	return exitcode.Wrap(exitcode.Error, fmt.Errorf("interactive selection is not wired up yet"))
}
