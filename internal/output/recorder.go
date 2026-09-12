package output

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"golang.org/x/term"

	"github.com/GlueOps/evac/internal/nodefile"
)

// Options configure the recorder.
type Options struct {
	// Stdout is where the human stream goes.
	Stdout io.Writer
	// JSON switches stdout to newline-delimited JSON events.
	JSON bool
	// LogFile overrides the default path. Ignored when NoLogFile is set.
	LogFile string
	// NoLogFile opts out of the always-on audit file.
	NoLogFile bool
	// Context names the cluster, used in the default log file name.
	Context string
	// Now allows tests to control timestamps.
	Now func() time.Time
}

// Recorder fans events out to the sinks §9 requires.
//
// All writes go through a single goroutine rather than a mutex. A mutex would
// be held across three separate writes — stdout, the file, possibly JSON — so
// concurrent drain workers would block on disk I/O; the channel decouples them
// and gives one place to clear and redraw the progress line.
type Recorder struct {
	events chan payload
	done   chan struct{}

	file     *os.File
	filePath string

	stdout io.Writer
	json   bool
	now    func() time.Time

	// progress state, owned by the writer goroutine
	tty         bool
	progressMsg string

	closeOnce sync.Once
	closeErr  error
}

type payload struct {
	event *Event
	diag  *Diagnostic
	// raw is plain text passed straight through: plan output and tables, which
	// are not events.
	raw string
	// flushed, when non-nil, is closed by the writer goroutine once everything
	// queued before it has been written. See Flush.
	flushed chan struct{}
	// progress, when non-nil, replaces the transient status line.
	progress *string
}

// New builds a Recorder and opens the log file.
//
// The log file is opened before anything is cordoned so a filesystem problem
// fails the run early, rather than after the cluster has been mutated and with
// nowhere to record what happened.
func New(opts Options) (*Recorder, error) {
	if opts.Stdout == nil {
		opts.Stdout = os.Stdout
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}

	r := &Recorder{
		events: make(chan payload, 256),
		done:   make(chan struct{}),
		stdout: opts.Stdout,
		json:   opts.JSON,
		now:    opts.Now,
		tty:    isTerminal(opts.Stdout),
	}

	if !opts.NoLogFile {
		path := opts.LogFile
		if path == "" {
			path = defaultLogPath(opts.Context, opts.Now())
		}
		abs, err := filepath.Abs(path)
		if err != nil {
			return nil, fmt.Errorf("resolving log file path %q: %w", path, err)
		}
		f, err := os.OpenFile(abs, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return nil, fmt.Errorf("opening log file %s: %w", abs, err)
		}
		r.file, r.filePath = f, abs
	}

	go r.run()
	return r, nil
}

// LogPath is the absolute path of the audit file, or "" when disabled. §9
// requires printing it at both start and end.
func (r *Recorder) LogPath() string { return r.filePath }

func defaultLogPath(context string, now time.Time) string {
	return fmt.Sprintf("./evac-%s-%s.log",
		nodefile.Sanitize(context), now.UTC().Format("20060102T150405Z"))
}

// run is the single writer.
func (r *Recorder) run() {
	defer close(r.done)
	enc := json.NewEncoder(r.stdout)

	for p := range r.events {
		if p.flushed != nil {
			close(p.flushed)
			continue
		}
		if p.progress != nil {
			r.clearProgress()
			r.progressMsg = *p.progress
			r.drawProgress()
			continue
		}
		r.clearProgress()

		switch {
		case p.raw != "":
			if !r.json {
				fmt.Fprint(r.stdout, p.raw)
			}
			r.writeFile(p.raw)

		case p.event != nil:
			if r.json {
				_ = enc.Encode(jsonEvent(*p.event))
			} else {
				fmt.Fprintln(r.stdout, p.event.Text(false))
			}
			r.writeFile(p.event.Text(true) + "\n")

		case p.diag != nil:
			if r.json {
				_ = enc.Encode(jsonDiagnostic(*p.diag))
			} else {
				fmt.Fprint(r.stdout, "\n"+p.diag.Text(false))
			}
			r.writeFile("\n" + p.diag.Text(true))
		}

		r.drawProgress()
	}
	r.clearProgress()
}

// writeFile appends to the audit file and flushes immediately. The transcript
// has to survive the process being killed, so buffering it would defeat the
// purpose.
func (r *Recorder) writeFile(s string) {
	if r.file == nil {
		return
	}
	_, _ = r.file.WriteString(s)
	_ = r.file.Sync()
}

// Event records one event, stamping the time in the calling goroutine so
// timestamps stay honest even if the writer is briefly behind.
func (r *Recorder) Event(e Event) {
	if e.Time.IsZero() {
		e.Time = r.now()
	}
	if e.Level == "" {
		e.Level = LevelInfo
	}
	r.send(payload{event: &e})
}

// Diagnostic records a §5 error block.
func (r *Recorder) Diagnostic(d Diagnostic) {
	if d.Time.IsZero() {
		d.Time = r.now()
	}
	r.send(payload{diag: &d})
}

// Raw passes text straight through — plan output, tables, the confirmation
// prompt's surrounding text. It reaches the log file too, so the audit artifact
// contains what the operator actually saw.
func (r *Recorder) Raw(s string) { r.send(payload{raw: s}) }

// Rawf is Raw with formatting.
func (r *Recorder) Rawf(format string, args ...any) { r.Raw(fmt.Sprintf(format, args...)) }

// Infof records an informational event.
func (r *Recorder) Infof(phase, node, format string, args ...any) {
	r.Event(Event{Phase: phase, Node: node, Msg: fmt.Sprintf(format, args...)})
}

// Warnf records a warning.
func (r *Recorder) Warnf(phase, node, format string, args ...any) {
	r.Event(Event{Level: LevelWarn, Phase: phase, Node: node, Msg: fmt.Sprintf(format, args...)})
}

func (r *Recorder) send(p payload) {
	select {
	case r.events <- p:
	case <-r.done:
		// Recorder closed; drop rather than block forever.
	}
}

// Flush blocks until everything queued so far has actually been written.
//
// Needed before anything writes to the terminal outside the recorder — the
// confirmation prompt does, because it reads from /dev/tty rather than stdin.
// Without this the prompt can appear above the totals it is asking the operator
// to approve, since those are still sitting in the queue.
func (r *Recorder) Flush() {
	done := make(chan struct{})
	select {
	case r.events <- payload{flushed: done}:
	case <-r.done:
		return
	}
	select {
	case <-done:
	case <-r.done:
	}
}

// Close drains the queue and closes the file. It must run before the process
// exits or the final — most important — lines are lost.
func (r *Recorder) Close() error {
	r.closeOnce.Do(func() {
		close(r.events)
		<-r.done
		if r.file != nil {
			r.closeErr = r.file.Close()
		}
	})
	return r.closeErr
}

// --- progress --------------------------------------------------------------

// Progress sets the transient status line shown during waits.
//
// It never becomes an event: §9 allows carriage returns on a terminal but the
// log file and the JSON stream must not contain them. When stdout is not a
// terminal this is a no-op.
//
// The message is handed to the writer goroutine rather than rendered here, so
// progressMsg has exactly one owner. It used to be written by whichever worker
// called this while the writer goroutine read it to clear and redraw — an
// unsynchronised string on every TTY run, and one no test could catch, because
// this function returns early unless stdout is a terminal and every test writes
// to a buffer.
//
// The send is non-blocking, unlike every other payload. Progress frames are
// worth nothing once superseded, and the writer goroutine fsyncs the audit file
// on each event — so a blocking send here could park a drain worker behind a
// slow disk while it should be polling the API. Dropping a frame costs a redraw.
func (r *Recorder) Progress(msg string) {
	if !r.tty || r.json {
		return
	}
	select {
	case r.events <- payload{progress: &msg}:
	default:
	}
}

// ClearProgress removes the status line.
//
// Blocking, unlike Progress: the final clear must not be dropped, or the last
// transient line stays on the operator's terminal after the run ends.
func (r *Recorder) ClearProgress() {
	if !r.tty || r.json {
		return
	}
	empty := ""
	r.send(payload{progress: &empty})
}

// clearProgress and drawProgress are called only from the writer goroutine, so
// they need no synchronisation of their own.
func (r *Recorder) clearProgress() {
	if r.progressMsg == "" || !r.tty || r.json {
		return
	}
	fmt.Fprint(r.stdout, "\r\x1b[K")
}

func (r *Recorder) drawProgress() {
	if r.progressMsg == "" || !r.tty || r.json {
		return
	}
	fmt.Fprint(r.stdout, r.progressMsg)
}

func isTerminal(w io.Writer) bool {
	f, ok := w.(*os.File)
	if !ok {
		return false
	}
	return term.IsTerminal(int(f.Fd()))
}

// --- JSON shapes -----------------------------------------------------------

type jsonEventShape struct {
	Time      string            `json:"time"`
	Type      string            `json:"type"`
	Level     string            `json:"level"`
	Phase     string            `json:"phase,omitempty"`
	Node      string            `json:"node,omitempty"`
	Namespace string            `json:"namespace,omitempty"`
	Kind      string            `json:"kind,omitempty"`
	Name      string            `json:"name,omitempty"`
	Msg       string            `json:"msg"`
	DurationS float64           `json:"duration_seconds,omitempty"`
	Attrs     map[string]string `json:"attrs,omitempty"`
}

func jsonEvent(e Event) jsonEventShape {
	return jsonEventShape{
		Time:      e.Time.UTC().Format(time.RFC3339Nano),
		Type:      "event",
		Level:     string(e.Level),
		Phase:     e.Phase,
		Node:      e.Node,
		Namespace: e.Namespace,
		Kind:      e.Kind,
		Name:      e.Name,
		Msg:       e.Msg,
		DurationS: e.Duration.Seconds(),
		Attrs:     e.Attrs,
	}
}

type jsonDiagShape struct {
	Time      string            `json:"time"`
	Type      string            `json:"type"`
	Level     string            `json:"level"`
	Node      string            `json:"node,omitempty"`
	Headline  string            `json:"headline"`
	Detail    []string          `json:"detail,omitempty"`
	Facts     map[string]string `json:"facts,omitempty"`
	Suggested []string          `json:"suggested_commands,omitempty"`
	Rerun     string            `json:"rerun,omitempty"`
}

func jsonDiagnostic(d Diagnostic) jsonDiagShape {
	return jsonDiagShape{
		Time:      d.Time.UTC().Format(time.RFC3339Nano),
		Type:      "diagnostic",
		Level:     string(LevelError),
		Node:      d.Node,
		Headline:  d.Headline,
		Detail:    d.Detail,
		Facts:     d.Facts,
		Suggested: d.Suggested,
		Rerun:     d.Rerun,
	}
}
