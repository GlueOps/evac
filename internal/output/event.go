// Package output implements §9's audit trail.
//
// §9 asks for three things at once: a streaming human line format, an always-on
// log file, and --output=json carrying the same events. Those are three
// renderings of one model, not three loggers — which is why there is no logging
// library here. slog, zerolog and zap each want to own the schema, and the
// richly formatted error blocks §5 specifies are not log lines at all.
package output

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// Level classifies an event.
type Level string

const (
	LevelInfo  Level = "info"
	LevelWarn  Level = "warn"
	LevelError Level = "error"
)

// Event is one thing that happened. It renders as a line for humans and as an
// object for --output=json, from the same fields.
type Event struct {
	Time  time.Time
	Level Level

	// Phase is the §5 phase number ("1", "1b", "2", "3", "4"), empty outside
	// the drain.
	Phase string
	// Node is the node this concerns. Required under --parallel: without it,
	// interleaved output from concurrent workers is unreadable.
	Node string

	Namespace string
	// Kind and Name identify the object, rendered as "pod/foo".
	Kind string
	Name string

	Msg string
	// Duration is an elapsed time worth reporting, such as how long a pod took
	// to terminate.
	Duration time.Duration

	// Attrs are extra key/value pairs, rendered in sorted order so the
	// transcript is stable and diffable.
	Attrs map[string]string
}

// Text renders the §9 line format:
//
//	14:02:11  phase=2 ns=platform  evicting pod/argocd-repo-server-7d4  node=node-a-01
//
// withDate switches to a full RFC3339 timestamp. The log file uses it because a
// maintenance window that crosses midnight makes a bare clock time ambiguous,
// and that file is the artifact someone reads a week later.
func (e Event) Text(withDate bool) string {
	var b strings.Builder

	if withDate {
		b.WriteString(e.Time.UTC().Format(time.RFC3339))
	} else {
		b.WriteString(e.Time.Format("15:04:05"))
	}
	b.WriteString("  ")

	if e.Level == LevelWarn || e.Level == LevelError {
		b.WriteString(strings.ToUpper(string(e.Level)))
		b.WriteString("  ")
	}

	var head []string
	if e.Phase != "" {
		head = append(head, "phase="+e.Phase)
	}
	if e.Namespace != "" {
		head = append(head, "ns="+e.Namespace)
	}
	if len(head) > 0 {
		b.WriteString(strings.Join(head, " "))
		b.WriteString("  ")
	}

	b.WriteString(e.Msg)

	if e.Kind != "" && e.Name != "" {
		b.WriteString(" ")
		b.WriteString(e.Kind)
		b.WriteString("/")
		b.WriteString(e.Name)
	}
	if e.Duration > 0 {
		fmt.Fprintf(&b, "  (%s)", e.Duration.Round(100*time.Millisecond))
	}
	if e.Node != "" {
		b.WriteString("  node=")
		b.WriteString(e.Node)
	}
	for _, k := range sortedKeys(e.Attrs) {
		fmt.Fprintf(&b, " %s=%s", k, e.Attrs[k])
	}
	return b.String()
}

// Diagnostic is a §5-style error block: a multi-line, indented explanation with
// the numbers that matter and the exact commands to run next.
//
// It is deliberately a separate type from Event. These are not log lines — they
// are the thing an operator reads at 2am when a drain has stopped, and their
// value is entirely in the formatting. In JSON they become one object with
// their fields intact, which is what makes the same information usable by a
// wrapper.
type Diagnostic struct {
	Time time.Time
	// Headline is the first line, e.g. "eviction timeout after 10m0s".
	Headline string
	// Detail is the indented body: what is stuck, with names and numbers.
	Detail []string
	// Facts are structured key/value pairs — disruptionsAllowed, currentHealthy
	// and so on — rendered in the body and preserved in JSON.
	Facts map[string]string
	// Suggested are verbatim commands the operator can run.
	Suggested []string
	// Rerun is the command to re-run once resolved. §5 requires this on every
	// failure that leaves work outstanding.
	Rerun string
	// Node, when set, scopes the diagnostic.
	Node string
}

// Text renders the indented block.
func (d Diagnostic) Text(withDate bool) string {
	var b strings.Builder

	if withDate {
		b.WriteString(d.Time.UTC().Format(time.RFC3339))
	} else {
		b.WriteString(d.Time.Format("15:04:05"))
	}
	b.WriteString("  ERROR  ")
	b.WriteString(d.Headline)
	// Under --parallel these blocks interleave, and three of the four used to
	// render without any attribution at all — leaving the operator to guess
	// which node a failure belonged to.
	if d.Node != "" && !strings.Contains(d.Headline, d.Node) {
		b.WriteString("  [node ")
		b.WriteString(d.Node)
		b.WriteString("]")
	}
	b.WriteString("\n\n")

	for _, line := range d.Detail {
		b.WriteString("  ")
		b.WriteString(line)
		b.WriteString("\n")
	}
	if len(d.Facts) > 0 {
		b.WriteString("\n")
		for _, k := range sortedKeys(d.Facts) {
			fmt.Fprintf(&b, "    %s: %s\n", k, d.Facts[k])
		}
	}
	if len(d.Suggested) > 0 {
		b.WriteString("\n  Debug:\n")
		for _, c := range d.Suggested {
			b.WriteString("    ")
			b.WriteString(c)
			b.WriteString("\n")
		}
	}
	if d.Rerun != "" {
		fmt.Fprintf(&b, "\n  Re-run once resolved:  %s\n", d.Rerun)
	}
	return b.String()
}

func sortedKeys(m map[string]string) []string {
	if len(m) == 0 {
		return nil
	}
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
