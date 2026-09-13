package cli

import (
	"errors"
	"strings"
	"testing"

	"github.com/GlueOps/evac/internal/exitcode"
)

// The codes carry distinct meanings so a wrapper can tell "fix your
// invocation" from "something is broken". Cobra reports its own parse and
// argument failures as plain errors, so without classification a typo exits 1
// and reads as an API failure.
func TestCobraParseFailuresAreUsageErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		err  error
		want exitcode.Code
	}{
		{"unknown flag", errors.New(`unknown flag: --nope`), exitcode.Usage},
		{"unknown shorthand", errors.New(`unknown shorthand flag: 'z' in -z`), exitcode.Usage},
		{"unknown command", errors.New(`unknown command "bogus" for "evac"`), exitcode.Usage},
		{"too many args", errors.New(`accepts 0 arg(s), received 2`), exitcode.Usage},
		{"missing flag value", errors.New(`flag needs an argument: --sort-by`), exitcode.Usage},
		{"bad flag value", errors.New(`invalid argument "x" for "--parallel"`), exitcode.Usage},

		// A real failure must not be reclassified as the operator's fault.
		{"api failure", errors.New("listing nodes: connection refused"), exitcode.Error},
		{"unexpected condition", errors.New("something broke"), exitcode.Error},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := classify(tc.err); got != tc.want {
				t.Errorf("classify(%q) = %d, want %d", tc.err, got, tc.want)
			}
		})
	}
}

// An error that already carries a code keeps it: classification must never
// override a deliberate exit code with a guess based on the message.
func TestExplicitCodesSurviveClassification(t *testing.T) {
	t.Parallel()
	for _, want := range []exitcode.Code{
		exitcode.ControlPlane, exitcode.LockHeld, exitcode.Preflight,
		exitcode.JobTimeout, exitcode.EvictionTimeout, exitcode.PVCStuck, exitcode.Aborted,
	} {
		err := exitcode.Wrap(want, errors.New("unknown command: a message that would otherwise be reclassified"))
		if got := classify(err); got != want {
			t.Errorf("classify = %d, want %d — an explicit code must win over the message", got, want)
		}
	}
}

// A substring match over these phrases also caught API failures whose text
// merely contains one — a dial failure reporting "connect: invalid argument"
// became exit 2, telling the operator their invocation was wrong when the
// cluster was unreachable.
func TestAPIFailuresAreNotReclassifiedAsUsageErrors(t *testing.T) {
	t.Parallel()
	for _, msg := range []string{
		"listing nodes: dial tcp 10.0.0.1:6443: connect: invalid argument",
		"deleting pvc app/data-0: etcdserver: request timed out",
		"evicting app/web-0: unknown command in server response",
		"listing pods: the server could not find the requested resource",
	} {
		if got := classify(errors.New(msg)); got != exitcode.Error {
			t.Errorf("classify(%q) = %d, want %d — a cluster failure was reported as the operator's mistake",
				msg, got, exitcode.Error)
		}
	}
}

// The re-run command is handed to an operator to paste, possibly hours later
// and possibly after switching context in another pane. Without --context it
// resolves against whatever kubecontext is active then, and pool-style node
// names collide across clusters — so a command evac printed could cordon nodes
// and delete local PVCs on a cluster the failed drain never touched.
func TestRerunCommandPinsTheClusterAndTheResolvedNodes(t *testing.T) {
	t.Parallel()
	got := rerunCommand(&globals{}, "prod-eu-1", []string{"node-a-01", "node-b-02"})

	if !strings.Contains(got, "--context prod-eu-1") {
		t.Errorf("rerun = %q, want it to pin the context", got)
	}
	if !strings.Contains(got, "--nodes node-a-01,node-b-02") {
		t.Errorf("rerun = %q, want the resolved node list", got)
	}
}

// An explicit --kubeconfig has to survive too, or the pasted command reads a
// different file than the run that produced it.
func TestRerunCommandCarriesAnExplicitKubeconfig(t *testing.T) {
	t.Parallel()
	got := rerunCommand(&globals{kubeconfig: "/tmp/kc"}, "c", []string{"n1"})
	if !strings.Contains(got, "--kubeconfig /tmp/kc") {
		t.Errorf("rerun = %q, want the kubeconfig pinned", got)
	}
}

// Selection modes that cannot be replayed must not be echoed back. -f - reads
// stdin to EOF, so re-running it blocks on the keyboard with the original node
// list already consumed; --selector re-derives a different set from live
// labels, including nodes the first run already drained.
func TestRerunCommandNeverEchoesBackStdinOrSelector(t *testing.T) {
	t.Parallel()
	got := rerunCommand(&globals{}, "c", []string{"n1", "n2"})
	for _, bad := range []string{"-f -", "--selector"} {
		if strings.Contains(got, bad) {
			t.Errorf("rerun = %q, must not contain %q", got, bad)
		}
	}
}

// The node-file advice is only true when nothing was selected and the default
// path was tried. Keyed on the -f flag it fired for every --nodes/--selector
// failure, printing "the default node file  was not usable" with an empty path
// after the operator had plainly given a selection.
func TestSelectionErrorAdviceOnlyAppearsWhenTheDefaultFileWasUsed(t *testing.T) {
	t.Parallel()
	base := errors.New("node(s) not found in cluster: typo-1")

	withDefault := classifySelectionError(base, "./evac-nodes-c.txt", true).Error()
	if !strings.Contains(withDefault, "default node file") {
		t.Errorf("default-file error = %q, want the node-file advice", withDefault)
	}

	fromFlags := classifySelectionError(base, "", false).Error()
	if strings.Contains(fromFlags, "No node selection given") {
		t.Errorf("flag error = %q, must not claim no selection was given", fromFlags)
	}
	if strings.Contains(fromFlags, "default node file") {
		t.Errorf("flag error = %q, must not offer node-file advice", fromFlags)
	}
}
