// Command evac drains Kubernetes worker nodes and destroys their local PVCs.
//
// This tool destroys data by design: read the README before running it
// anywhere, in particular which clusters it is and is not for.
package main

import (
	"os"

	"github.com/GlueOps/evac/internal/cli"
)

// Stamped at link time by the Makefile.
var (
	version = "dev"
	commit  = "unknown"
	date    = "unknown"
)

func main() {
	cli.SetBuildInfo(version, commit, date)
	// The only os.Exit in the tool. Everything below returns errors up to here
	// so deferred cleanup runs and buffered audit lines are flushed.
	os.Exit(int(cli.Execute()))
}
