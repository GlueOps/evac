package nodefile

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// EKS contexts are ARNs full of colons and slashes; unsanitized they produce a
// nonsense path or silently write into a subdirectory.
func TestSanitizeHandlesEKSARNs(t *testing.T) {
	t.Parallel()
	got := Sanitize("arn:aws:eks:us-east-1:123456789012:cluster/prod")

	if strings.ContainsAny(got, ":/") {
		t.Errorf("Sanitize = %q, still contains a path or ARN separator", got)
	}
	if !strings.Contains(got, "prod") {
		t.Errorf("Sanitize = %q, want the cluster name to survive so the file is recognisable", got)
	}
}

func TestSanitize(t *testing.T) {
	t.Parallel()
	tests := map[string]string{
		"k3d-captain": "k3d-captain",
		"":            "unknown",
		":::":         "unknown",
		"a//b":        "a-b", // runs collapse rather than producing a--b
	}
	for in, want := range tests {
		if got := Sanitize(in); got != want {
			t.Errorf("Sanitize(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestDefaultPathIsPerContext(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	a, err := DefaultPath("cluster-a")
	if err != nil {
		t.Fatal(err)
	}
	b, err := DefaultPath("cluster-b")
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Error("two contexts produced the same path; selecting for one cluster would overwrite the other's list")
	}
}

// The node file must not land in the working directory. It used to, which put
// an operational file into whatever source tree the operator happened to be
// standing in — committable by accident — and made the selection depend on
// which directory `evac drain` was run from.
func TestDefaultPathIsOutsideTheWorkingDirectory(t *testing.T) {
	state := t.TempDir()
	t.Setenv("XDG_STATE_HOME", state)

	got, err := DefaultPath("k3d-evac-dev")
	if err != nil {
		t.Fatal(err)
	}
	if !filepath.IsAbs(got) {
		t.Errorf("DefaultPath = %q, want an absolute path", got)
	}
	if !strings.HasPrefix(got, filepath.Join(state, "evac")+string(filepath.Separator)) {
		t.Errorf("DefaultPath = %q, want it under %s/evac", got, state)
	}
	if strings.Contains(got, "./") {
		t.Errorf("DefaultPath = %q, want no working-directory component", got)
	}
}

// With XDG_STATE_HOME unset it falls back to ~/.local/state, still outside the
// working directory.
func TestDefaultPathFallsBackToTheHomeStateDirectory(t *testing.T) {
	home := t.TempDir()
	t.Setenv("XDG_STATE_HOME", "")
	t.Setenv("HOME", home)

	got, err := DefaultPath("c")
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(home, ".local", "state", "evac")
	if !strings.HasPrefix(got, want+string(filepath.Separator)) {
		t.Errorf("DefaultPath = %q, want it under %s", got, want)
	}
}

// Write creates the state directory; it does not exist on a first run.
func TestWriteCreatesTheStateDirectory(t *testing.T) {
	state := t.TempDir()
	t.Setenv("XDG_STATE_HOME", state)

	path, err := DefaultPath("c")
	if err != nil {
		t.Fatal(err)
	}
	if err := Write(path, "c", []string{"node-a"}); err != nil {
		t.Fatalf("Write into a directory that does not exist yet: %v", err)
	}
	f, err := Read(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(f.Nodes) != 1 || f.Nodes[0] != "node-a" {
		t.Errorf("Nodes = %v, want [node-a]", f.Nodes)
	}
}

func TestReadSkipsCommentsAndAcceptsKubectlNameOutput(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "nodes.txt")
	content := `# generated 2026-09-12T14:02:11Z
# context: glueops-prod-eu-1

node-a-01
node/node-a-04

  node-b-02
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	f, err := Read(path)
	if err != nil {
		t.Fatal(err)
	}

	want := []string{"node-a-01", "node-a-04", "node-b-02"}
	if len(f.Nodes) != len(want) {
		t.Fatalf("nodes = %v, want %v", f.Nodes, want)
	}
	for i := range want {
		if f.Nodes[i] != want[i] {
			t.Errorf("nodes[%d] = %q, want %q", i, f.Nodes[i], want[i])
		}
	}
	if f.Context != "glueops-prod-eu-1" {
		t.Errorf("context = %q, want the header value to be parsed", f.Context)
	}
}

func TestWriteThenReadRoundTrips(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "nodes.txt")
	if err := Write(path, "k3d-evac-dev", []string{"a", "b"}); err != nil {
		t.Fatal(err)
	}

	f, err := Read(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(f.Nodes) != 2 || f.Nodes[0] != "a" || f.Nodes[1] != "b" {
		t.Errorf("nodes = %v, want [a b]", f.Nodes)
	}
	if f.Context != "k3d-evac-dev" {
		t.Errorf("context = %q, want k3d-evac-dev", f.Context)
	}
}
