// Package nodefile reads and writes the plain-text node list.
//
// The format is deliberately dull: greppable, hand-editable, diffable, and
// trivially hand-writable when someone skips the picker.
package nodefile

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// File is a parsed node list.
type File struct {
	Nodes []string
	// Context is the value of the "# context:" header, if present. It is
	// informational only: no check is performed against it, because drain
	// operates on the active kubecontext regardless. It exists so someone who
	// finds the file later knows what it was generated against.
	Context string
	// Path is where it was read from, or "-" for stdin.
	Path string
}

// DefaultPath returns the per-context node file path.
//
// Per-context rather than a fixed name because selecting for cluster B would
// otherwise silently overwrite the list for cluster A.
func DefaultPath(context string) string {
	return "./evac-nodes-" + Sanitize(context) + ".txt"
}

// Sanitize makes a kubecontext name safe to embed in a filename.
//
// EKS contexts are ARNs — arn:aws:eks:us-east-1:123456789012:cluster/prod —
// so colons and slashes have to go or the path is nonsense. Anything outside a
// conservative set becomes a dash, and runs of dashes collapse so the result
// stays readable.
func Sanitize(context string) string {
	if context == "" {
		return "unknown"
	}
	var b strings.Builder
	lastDash := false
	for _, r := range context {
		ok := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.'
		if ok {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}
	out := strings.Trim(b.String(), "-.")
	if out == "" {
		return "unknown"
	}
	return out
}

// Read parses a node file. A path of "-" reads stdin, so
// `kubectl get nodes -l ... -o name | evac drain -f -` works.
func Read(path string) (*File, error) {
	if path == "-" {
		f, err := parse(os.Stdin)
		if err != nil {
			return nil, err
		}
		f.Path = "-"
		return f, nil
	}
	fh, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("reading node file: %w", err)
	}
	// Read-only: a close error cannot affect what was already parsed.
	defer func() { _ = fh.Close() }()

	f, err := parse(fh)
	if err != nil {
		return nil, fmt.Errorf("reading node file %s: %w", path, err)
	}
	f.Path = path
	return f, nil
}

func parse(r io.Reader) (*File, error) {
	out := &File{}
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())

		if strings.HasPrefix(line, "#") {
			// Pick the context header out of the comments.
			if rest, ok := strings.CutPrefix(strings.TrimSpace(strings.TrimPrefix(line, "#")), "context:"); ok {
				out.Context = strings.TrimSpace(rest)
			}
			continue
		}
		if line == "" {
			continue
		}
		// Accept `node/foo` as well as `foo`, so the output of
		// `kubectl get nodes -o name` can be piped in unmodified.
		line = strings.TrimPrefix(line, "node/")
		out.Nodes = append(out.Nodes, line)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// Write writes a node list with the generated-at and context header. The write
// goes to a temporary file in the same directory and is then renamed, so an
// interrupted write cannot leave a half-written list that a later drain would
// act on.
func Write(path, context string, nodes []string) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".evac-nodes-*")
	if err != nil {
		return fmt.Errorf("creating node file: %w", err)
	}
	tmpName := tmp.Name()
	// No-op once the rename succeeds; a failure here leaves a stray temp file,
	// which is not worth failing a write over.
	defer func() { _ = os.Remove(tmpName) }()

	w := bufio.NewWriter(tmp)
	fmt.Fprintf(w, "# generated %s\n", time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintf(w, "# context: %s\n", context)
	for _, n := range nodes {
		fmt.Fprintln(w, n)
	}
	if err := w.Flush(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("writing node file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("writing node file: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("writing node file: %w", err)
	}
	return nil
}
