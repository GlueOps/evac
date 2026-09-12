//go:build integration

package integration

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/creack/pty"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/GlueOps/evac/internal/exitcode"
	"github.com/GlueOps/evac/internal/lock"
)

// These tests drive the compiled binary rather than the Go API.
//
// Everything else in this package calls the packages directly, which leaves the
// entire CLI layer — flag parsing, the confirmation prompt, the lock, and every
// exit code — unexercised. §9 gives those codes distinct meanings specifically
// so a wrapper can branch on them, which makes them part of the contract rather
// than an implementation detail.

var (
	buildOnce sync.Once
	binPath   string
	buildErr  error
)

// evacBinary builds the CLI once per run and returns its path.
func evacBinary(t *testing.T) string {
	t.Helper()
	buildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "evac-bin-")
		if err != nil {
			buildErr = err
			return
		}
		binPath = filepath.Join(dir, "evac")
		cmd := exec.Command("go", "build", "-o", binPath, "./cmd/evac")
		cmd.Dir = repoRoot(t)
		if out, err := cmd.CombinedOutput(); err != nil {
			buildErr = errors.New(string(out))
		}
	})
	if buildErr != nil {
		t.Fatalf("building evac: %v", buildErr)
	}
	return binPath
}

func repoRoot(t *testing.T) string {
	t.Helper()
	// The test binary runs in test/integration.
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Join(wd, "..", "..")
}

type result struct {
	stdout, stderr string
	code           int
}

func (r result) output() string { return r.stdout + r.stderr }

// runEvac runs the binary with the test cluster's context and returns its exit
// code. Stdin is /dev/null, so this is the non-interactive path.
func runEvac(t *testing.T, env []string, args ...string) result {
	t.Helper()
	full := append([]string{"--context", client.Context}, args...)
	cmd := exec.Command(evacBinary(t), full...)
	cmd.Env = append(os.Environ(), env...)

	devnull, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatal(err)
	}
	defer devnull.Close()
	cmd.Stdin = devnull

	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr

	err = cmd.Run()
	code := 0
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		code = ee.ExitCode()
	} else if err != nil {
		t.Fatalf("running evac: %v", err)
	}
	return result{stdout: stdout.String(), stderr: stderr.String(), code: code}
}

func wantCode(t *testing.T, got result, want exitcode.Code, why string) {
	t.Helper()
	if got.code != int(want) {
		t.Errorf("exit code = %d, want %d (%s)\n--- output ---\n%s", got.code, want, why, got.output())
	}
}

// --- read-only paths succeed ----------------------------------------------

func TestNodesAndPlanExitZero(t *testing.T) {
	nodes := workerNodes(t, 1)

	got := runEvac(t, nil, "nodes")
	wantCode(t, got, exitcode.OK, "the inventory is read-only")
	if !strings.Contains(got.stdout, nodes[0].Name) {
		t.Errorf("inventory does not list %s:\n%s", nodes[0].Name, got.stdout)
	}

	got = runEvac(t, nil, "plan", "--nodes", nodes[0].Name)
	wantCode(t, got, exitcode.OK, "plan is read-only")
}

// --- usage errors ----------------------------------------------------------

func TestUsageErrors(t *testing.T) {
	nodes := workerNodes(t, 1)

	tests := []struct {
		name string
		args []string
		why  string
	}{
		{"unknown flag", []string{"nodes", "--nope"}, "an unknown flag is a usage error"},
		{"bad --sort-by", []string{"nodes", "--sort-by", "bogus"}, "the value is not one of the accepted keys"},
		{"--nodes with --selector", []string{"plan", "--nodes", nodes[0].Name, "--selector", "a=b"},
			"selection sources are mutually exclusive rather than given a precedence"},
		{"unreadable node file", []string{"plan", "-f", "/nonexistent/nodes.txt"}, "the node file cannot be read"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			wantCode(t, runEvac(t, nil, tc.args...), exitcode.Usage, tc.why)
		})
	}
}

// §8: if stdin is not a TTY and --yes was not passed, fail rather than
// proceeding or hanging on a prompt nobody can answer. Hanging would be the
// worse outcome in a pipeline, so the test also bounds the time.
func TestNonInteractiveDrainRefusesWithoutYes(t *testing.T) {
	nodes := workerNodes(t, 1)

	done := make(chan result, 1)
	go func() {
		done <- runEvac(t, nil, "drain", "--nodes", nodes[0].Name, "--no-log-file")
	}()

	select {
	case got := <-done:
		wantCode(t, got, exitcode.Usage, "no TTY and no --yes")
		if !strings.Contains(got.output(), "not a terminal") {
			t.Errorf("the refusal does not explain itself:\n%s", got.output())
		}
	case <-time.After(90 * time.Second):
		t.Fatal("drain hung waiting for a prompt nobody can answer")
	}
}

// --- refusals --------------------------------------------------------------

// §3.1's layer that matters: the node file is hand-editable, so a control plane
// node named there is refused outright rather than silently filtered.
func TestControlPlaneInNodeFileExitsSeven(t *testing.T) {
	cp := controlPlaneNodeName(t)

	path := filepath.Join(t.TempDir(), "nodes.txt")
	body := "# context: " + client.Context + "\n" + cp + "\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}

	got := runEvac(t, nil, "drain", "-f", path, "--yes", "--no-log-file")
	wantCode(t, got, exitcode.ControlPlane, "a node file naming a control plane node")
	if !strings.Contains(got.output(), "worker nodes only") {
		t.Errorf("the refusal does not explain why:\n%s", got.output())
	}
	if !strings.Contains(got.output(), cp) {
		t.Errorf("the refusal does not name the offending node:\n%s", got.output())
	}
}

// §6: one drain at a time. The lock is taken here in-process against a private
// path, and the binary is pointed at the same one, so the contention is
// deterministic rather than a race between two real drains.
func TestSecondDrainExitsEight(t *testing.T) {
	nodes := workerNodes(t, 1)
	runtimeDir := t.TempDir()

	held, err := lock.AcquireAt(filepath.Join(runtimeDir, "evac.lock"), "test-holder")
	if err != nil {
		t.Fatal(err)
	}
	defer held.Release()

	got := runEvac(t, []string{"XDG_RUNTIME_DIR=" + runtimeDir},
		"drain", "--nodes", nodes[0].Name, "--yes", "--no-log-file")

	wantCode(t, got, exitcode.LockHeld, "another drain holds the lock")
	if !strings.Contains(got.output(), "already running") {
		t.Errorf("the error does not say another drain is running:\n%s", got.output())
	}
}

// §8: default to no. Answering anything other than yes must abort before a
// single node is cordoned.
//
// This needs a real terminal, because the non-TTY path is a different branch
// that refuses outright — so a pipe would test the wrong thing.
func TestAnsweringNoAtThePromptAbortsWithoutCordoning(t *testing.T) {
	nodes := workerNodes(t, 1)
	target := nodes[0].Name

	cmd := exec.Command(evacBinary(t),
		"--context", client.Context, "drain", "--nodes", target, "--no-log-file")
	cmd.Env = os.Environ()

	ptmx, err := pty.Start(cmd)
	if err != nil {
		t.Skipf("no pty available: %v", err)
	}
	defer ptmx.Close()
	// A leaked evac process holds the lock file, which would then fail
	// TestSecondDrainExitsEight for a reason that has nothing to do with it.
	t.Cleanup(func() { _ = cmd.Process.Kill() })

	// Written by the reader goroutine below and read by this one, so it needs
	// its own lock — integration runs without -race, so a plain bytes.Buffer
	// would corrupt silently rather than reporting.
	var out syncBuffer
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		buf := make([]byte, 4096)
		for {
			n, err := ptmx.Read(buf)
			if n > 0 {
				out.Write(buf[:n])
			}
			if err != nil {
				return
			}
		}
	}()

	// Wait for the prompt before answering, or the write races the read.
	deadline := time.Now().Add(90 * time.Second)
	for !strings.Contains(out.String(), "Proceed?") {
		if time.Now().After(deadline) {
			_ = cmd.Process.Kill()
			t.Fatalf("never reached the confirmation prompt:\n%s", out.String())
		}
		time.Sleep(500 * time.Millisecond)
	}
	if _, err := ptmx.Write([]byte("n\n")); err != nil {
		t.Fatal(err)
	}

	err = cmd.Wait()
	<-readDone

	code := 0
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		code = ee.ExitCode()
	}
	if code != int(exitcode.Aborted) {
		t.Errorf("exit code = %d, want %d (aborted at the prompt)\n%s", code, exitcode.Aborted, out.String())
	}

	// The point of aborting is that nothing happened.
	if cordoned(t, target) {
		t.Errorf("%s was cordoned despite the operator answering no", target)
	}
}

// --- helpers ---------------------------------------------------------------

func controlPlaneNodeName(t *testing.T) string {
	t.Helper()
	list, err := client.Clientset.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range list.Items {
		if _, ok := n.Labels["node-role.kubernetes.io/control-plane"]; ok {
			return n.Name
		}
	}
	t.Skip("no control plane node in this cluster")
	return ""
}

func cordoned(t *testing.T, name string) bool {
	t.Helper()
	n, err := client.Clientset.CoreV1().Nodes().Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return n.Spec.Unschedulable
}
