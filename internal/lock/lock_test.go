package lock

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSecondAcquireIsRefusedAndNamesTheHolder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "evac.lock")

	first, err := AcquireAt(path, "glueops-prod-eu-1")
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	defer first.Release()

	// flock attaches to the open file description, so a second open in this
	// same process conflicts exactly as another process would.
	_, err = AcquireAt(path, "other-context")
	if err == nil {
		t.Fatal("second acquire succeeded; two drains could run at once")
	}

	var held *HeldError
	if !errors.As(err, &held) {
		t.Fatalf("error = %T (%v), want *HeldError", err, err)
	}
	if held.Holder == nil {
		t.Fatal("holder metadata was not readable")
	}
	if held.Holder.PID != os.Getpid() {
		t.Errorf("holder PID = %d, want %d", held.Holder.PID, os.Getpid())
	}
	if held.Holder.Context != "glueops-prod-eu-1" {
		t.Errorf("holder context = %q, want the first acquirer's", held.Holder.Context)
	}
	if held.Holder.Started.IsZero() {
		t.Error("holder start time was not recorded")
	}
}

// The regression test for the open-flag ordering: a failed attempt must not
// destroy the live holder's metadata. Opening with O_TRUNC would wipe it on
// every refused attempt, so the second operator's error message would erase the
// information it was about to print.
func TestFailedAcquireDoesNotDestroyHolderMetadata(t *testing.T) {
	path := filepath.Join(t.TempDir(), "evac.lock")

	first, err := AcquireAt(path, "prod")
	if err != nil {
		t.Fatal(err)
	}
	defer first.Release()

	// Several failed attempts, as an impatient operator would produce.
	for range 3 {
		if _, err := AcquireAt(path, "prod"); err == nil {
			t.Fatal("acquire unexpectedly succeeded")
		}
	}

	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(body) == 0 {
		t.Fatal("lock file was truncated by a failed acquire; the holder's details are gone")
	}
	h := parseHolder(string(body))
	if h == nil || h.Context != "prod" {
		t.Errorf("holder metadata damaged: %q", body)
	}
}

func TestReleaseAllowsReacquisition(t *testing.T) {
	path := filepath.Join(t.TempDir(), "evac.lock")

	first, err := AcquireAt(path, "a")
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Release(); err != nil {
		t.Fatal(err)
	}

	second, err := AcquireAt(path, "b")
	if err != nil {
		t.Fatalf("reacquire after release failed: %v", err)
	}
	defer second.Release()

	h := readHolder(path)
	if h == nil || h.Context != "b" {
		t.Errorf("holder = %+v, want the second acquirer", h)
	}
}

// A lock file another user can write to is not a lock.
func TestWorldWritableLockFileIsRefused(t *testing.T) {
	path := filepath.Join(t.TempDir(), "evac.lock")
	if err := os.WriteFile(path, nil, 0o666); err != nil {
		t.Fatal(err)
	}
	// The umask may have already stripped the group/other bits.
	if err := os.Chmod(path, 0o666); err != nil {
		t.Fatal(err)
	}

	if _, err := AcquireAt(path, "a"); err == nil {
		t.Error("acquired a world-writable lock file; another user could interfere with it")
	}
}

// A symlink at the lock path is how a hostile local user redirects the write.
func TestSymlinkedLockPathIsRefused(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	link := filepath.Join(dir, "evac.lock")
	if err := os.WriteFile(target, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	if _, err := AcquireAt(link, "a"); err == nil {
		t.Error("followed a symlink at the lock path")
	}
}

func TestDefaultPathPrefersXDGRuntimeDir(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", "/run/user/1000")
	got, err := DefaultPath()
	if err != nil {
		t.Fatal(err)
	}
	if got != "/run/user/1000/evac.lock" {
		t.Errorf("DefaultPath = %q", got)
	}
}

// On macOS XDG_RUNTIME_DIR is essentially never set, so this is the normal path
// there rather than an exception. The uid must appear in the name.
func TestDefaultPathFallsBackToTmpdirWithUID(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", "")
	t.Setenv("TMPDIR", "/var/folders/xy")

	got, err := DefaultPath()
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join("/var/folders/xy", "evac-"+itoa(os.Getuid())+".lock")
	if got != want {
		t.Errorf("DefaultPath = %q, want %q", got, want)
	}
}

func TestHolderAge(t *testing.T) {
	h := Holder{Started: time.Now().Add(-8 * time.Minute)}
	if got := h.Age(); got < 7*time.Minute || got > 9*time.Minute {
		t.Errorf("Age = %v, want ~8m", got)
	}
	if (Holder{}).Age() != 0 {
		t.Error("zero start time should report zero age")
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	neg := i < 0
	if neg {
		i = -i
	}
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	if neg {
		return "-" + string(b)
	}
	return string(b)
}
