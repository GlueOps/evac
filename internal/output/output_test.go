package output

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/creack/pty"
)

func at(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}

func TestEventTextMatchesTheSpecLineFormat(t *testing.T) {
	t.Parallel()
	e := Event{
		Time:      at("2026-09-12T14:02:11Z"),
		Phase:     "2",
		Namespace: "platform",
		Node:      "node-a-01",
		Kind:      "pod",
		Name:      "argocd-repo-server-7d4",
		Msg:       "evicting",
	}
	got := e.Text(false)
	want := "14:02:11  phase=2 ns=platform  evicting pod/argocd-repo-server-7d4  node=node-a-01"
	if got != want {
		t.Errorf("Text =\n  %q\nwant\n  %q", got, want)
	}
}

// A maintenance window can cross midnight, which makes a bare clock time in the
// audit file ambiguous a week later.
func TestLogFileTimestampsCarryTheDate(t *testing.T) {
	t.Parallel()
	e := Event{Time: at("2026-09-12T14:02:11Z"), Msg: "cordoned"}

	if strings.Contains(e.Text(false), "2026") {
		t.Error("stdout format should use a bare clock time")
	}
	if !strings.Contains(e.Text(true), "2026-09-12") {
		t.Errorf("file format = %q, want a full date", e.Text(true))
	}
}

// The same event must reach stdout, the audit file, and (when asked) JSON.
func TestEventReachesStdoutAndLogFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	logPath := filepath.Join(dir, "run.log")
	var stdout bytes.Buffer

	r, err := New(Options{Stdout: &stdout, LogFile: logPath, Context: "test"})
	if err != nil {
		t.Fatal(err)
	}
	r.Event(Event{Time: at("2026-09-12T14:02:11Z"), Phase: "2", Msg: "evicting", Kind: "pod", Name: "web-0"})
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(stdout.String(), "evicting pod/web-0") {
		t.Errorf("stdout = %q", stdout.String())
	}
	body, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "evicting pod/web-0") {
		t.Errorf("log file = %q", body)
	}
	if !strings.Contains(string(body), "2026-09-12") {
		t.Error("log file entry is missing the date")
	}
}

func TestJSONOutputCarriesTheSameEvent(t *testing.T) {
	t.Parallel()
	var stdout bytes.Buffer
	r, err := New(Options{Stdout: &stdout, JSON: true, NoLogFile: true})
	if err != nil {
		t.Fatal(err)
	}
	r.Event(Event{
		Time: at("2026-09-12T14:02:11Z"), Phase: "2", Node: "node-a-01",
		Namespace: "platform", Kind: "pod", Name: "web-0", Msg: "evicting",
	})
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}

	var got jsonEventShape
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("stdout is not valid JSON: %v (%q)", err, stdout.String())
	}
	if got.Type != "event" || got.Phase != "2" || got.Node != "node-a-01" || got.Msg != "evicting" {
		t.Errorf("json = %+v", got)
	}
}

// §5's error blocks are not log lines: their value is the formatting, and in
// JSON they must keep their fields rather than collapsing into a string.
func TestDiagnosticRendersAsABlockAndAsStructuredJSON(t *testing.T) {
	t.Parallel()
	d := Diagnostic{
		Time:     at("2026-09-12T14:47:03Z"),
		Headline: "eviction timeout after 10m0s",
		Detail:   []string{"glueops/loki-write-1 on node-a-01", "  blocked by PDB glueops/loki-write"},
		Facts: map[string]string{
			"disruptionsAllowed": "0", "currentHealthy": "2", "desiredHealthy": "2",
		},
		Suggested: []string{"kubectl describe pdb -n glueops loki-write"},
		Rerun:     "evac drain",
	}

	text := d.Text(false)
	for _, want := range []string{
		"ERROR  eviction timeout after 10m0s",
		"blocked by PDB glueops/loki-write",
		"disruptionsAllowed: 0",
		"kubectl describe pdb -n glueops loki-write",
		"Re-run once resolved:  evac drain",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("diagnostic text missing %q:\n%s", want, text)
		}
	}

	var stdout bytes.Buffer
	r, _ := New(Options{Stdout: &stdout, JSON: true, NoLogFile: true})
	r.Diagnostic(d)
	_ = r.Close()

	var got jsonDiagShape
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("not valid JSON: %v", err)
	}
	if got.Facts["disruptionsAllowed"] != "0" {
		t.Errorf("facts lost in JSON: %+v", got.Facts)
	}
	if len(got.Suggested) != 1 {
		t.Errorf("suggested commands lost in JSON: %+v", got.Suggested)
	}
}

// Progress uses carriage returns, which must never reach the audit file.
func TestProgressNeverEntersTheLogFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	logPath := filepath.Join(dir, "run.log")
	var stdout bytes.Buffer

	r, err := New(Options{Stdout: &stdout, LogFile: logPath})
	if err != nil {
		t.Fatal(err)
	}
	r.Progress("waiting for pod/web-0  12s")
	r.Event(Event{Msg: "pod terminated"})
	_ = r.Close()

	body, _ := os.ReadFile(logPath)
	if strings.Contains(string(body), "\r") {
		t.Error("log file contains a carriage return")
	}
	if strings.Contains(string(body), "waiting for pod/web-0") {
		t.Error("transient progress text reached the audit file")
	}
}

func TestDefaultLogPathIsPerContextAndSanitized(t *testing.T) {
	t.Parallel()
	got := defaultLogPath("arn:aws:eks:us-east-1:123456789012:cluster/prod", at("2026-09-12T14:02:11Z"))
	if strings.ContainsAny(strings.TrimPrefix(got, "./"), ":/") {
		t.Errorf("log path %q still contains ARN separators", got)
	}
	if !strings.Contains(got, "20260912T140211Z") {
		t.Errorf("log path %q is missing the timestamp", got)
	}
}

// TestProgressIsRaceFreeOnARealTerminal exercises the path every other test in
// this file cannot reach.
//
// Progress returns immediately unless stdout is a terminal, and every other
// test here writes to a bytes.Buffer — so the progress code has never executed
// under -race despite running on every interactive drain. It used to write
// progressMsg on the calling goroutine while the writer goroutine read it to
// clear and redraw; a real pty is what makes that visible.
func TestProgressIsRaceFreeOnARealTerminal(t *testing.T) {
	ptmx, tty, err := pty.Open()
	if err != nil {
		t.Skipf("no pty available: %v", err)
	}
	defer ptmx.Close()
	defer tty.Close()

	// Drain the master side, or the writers block once its buffer fills.
	go func() {
		buf := make([]byte, 4096)
		for {
			if _, err := ptmx.Read(buf); err != nil {
				return
			}
		}
	}()

	rec, err := New(Options{Stdout: tty, NoLogFile: true})
	if err != nil {
		t.Fatal(err)
	}
	if !rec.tty {
		t.Fatal("recorder did not detect the pty; this test would prove nothing")
	}

	// Several workers updating progress while events interleave, which is what
	// --parallel does.
	var wg sync.WaitGroup
	for w := range 4 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := range 50 {
				rec.Progress(fmt.Sprintf("  worker %d waiting (%d)", id, i))
				rec.Event(Event{Phase: "2", Node: fmt.Sprintf("node-%d", id), Msg: "polling"})
			}
		}(w)
	}
	wg.Wait()

	rec.ClearProgress()
	if err := rec.Close(); err != nil {
		t.Fatal(err)
	}
}

// Close must not panic when it races an in-flight Event: a send on a closed
// channel is a ready case in a select, so the panic window is real.
func TestCloseDoesNotPanicAgainstConcurrentEvents(t *testing.T) {
	var out bytes.Buffer
	rec, err := New(Options{Stdout: &out, NoLogFile: true})
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				rec.Event(Event{Msg: "concurrent"})
			}
		}()
	}
	wg.Wait()
	if err := rec.Close(); err != nil {
		t.Fatal(err)
	}
	// Close is idempotent via sync.Once; a second call must not panic either.
	if err := rec.Close(); err != nil {
		t.Fatal(err)
	}
}
