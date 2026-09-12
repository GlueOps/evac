package cli

import (
	"errors"
	"testing"

	"github.com/GlueOps/evac/internal/exitcode"
)

// §9 gives the codes distinct meanings so a wrapper can tell "fix your
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
