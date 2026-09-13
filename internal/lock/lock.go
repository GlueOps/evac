// Package lock provides the advisory file lock that keeps two drains from
// running at once.
//
// Only one drain may run at a time. Two concurrent runs would each derive scope
// from live state while the other mutates it, producing overlapping cordons and
// double evictions.
package lock

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// Holder describes the process currently holding the lock.
type Holder struct {
	PID     int
	Context string
	Started time.Time
}

// Age reports how long the holder has been running.
func (h Holder) Age() time.Duration {
	if h.Started.IsZero() {
		return 0
	}
	return time.Since(h.Started)
}

// HeldError is returned when another drain already holds the lock.
type HeldError struct {
	Path string
	// Holder is best-effort: the metadata may not have been written yet.
	Holder *Holder
}

func (e *HeldError) Error() string {
	if e.Holder == nil {
		return "another drain is already running (holder details unavailable)"
	}
	return fmt.Sprintf("another drain is already running (pid %d, started %s ago)\n       context: %s",
		e.Holder.PID, e.Holder.Age().Round(time.Second), e.Holder.Context)
}

// Lock is a held advisory file lock.
type Lock struct {
	file *os.File
}

// Acquire takes the lock, or fails immediately if another run holds it.
//
// Acquisition is non-blocking by design: an operator who runs the tool twice
// wants to be told, not left waiting behind a drain that may take an hour.
func Acquire(context string) (*Lock, error) {
	path, err := DefaultPath()
	if err != nil {
		return nil, err
	}
	return AcquireAt(path, context)
}

// AcquireAt takes the lock at an explicit path.
func AcquireAt(path, context string) (*Lock, error) {
	// O_TRUNC is deliberately absent. Truncating at open time would wipe a live
	// holder's metadata on every *failed* attempt, so the second operator's
	// error message would destroy the very information it was about to print.
	// The file is truncated only after the lock is actually held.
	//
	// O_NOFOLLOW guards the /tmp fallback: a predictable filename in a
	// world-writable directory is a symlink-swap target, and the uid in the
	// name does not address that. Go sets O_CLOEXEC on everything it opens, so
	// a forked child cannot inherit the descriptor and keep the lock alive past
	// this process — which is what makes the no-stale-lock property below hold.
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, fmt.Errorf("opening lock file %s: %w", path, err)
	}

	if err := checkOwnership(f, path); err != nil {
		_ = f.Close()
		return nil, err
	}

	// LOCK_NB: fail fast rather than queue.
	//
	// No stale-lock handling is needed anywhere in this package. The kernel
	// releases a flock when the holding process dies by any means, SIGKILL
	// included, so a crashed run can be retried immediately with no cleanup and
	// no expiry window. That is a genuine advantage over an in-cluster Lease,
	// which would need a duration, a renewal loop and a force-unlock path.
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		holder := readHolder(path)
		_ = f.Close()
		// errors.Is, not ==: the syscall wrapper is free to return a wrapped
		// errno, and a direct comparison would silently fall through to the
		// generic error path and report the wrong exit code.
		if errors.Is(err, unix.EWOULDBLOCK) {
			return nil, &HeldError{Path: path, Holder: holder}
		}
		return nil, fmt.Errorf("locking %s: %w", path, err)
	}

	// Held. Now it is safe to replace the contents.
	if err := f.Truncate(0); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("truncating lock file: %w", err)
	}
	if _, err := f.Seek(0, 0); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("rewinding lock file: %w", err)
	}
	fmt.Fprintf(f, "pid=%d\ncontext=%s\nstarted=%s\n",
		os.Getpid(), context, time.Now().UTC().Format(time.RFC3339))
	_ = f.Sync()

	return &Lock{file: f}, nil
}

// Release drops the lock. The kernel would do this on exit anyway; doing it
// explicitly keeps the intent visible.
func (l *Lock) Release() error {
	if l == nil || l.file == nil {
		return nil
	}
	_ = unix.Flock(int(l.file.Fd()), unix.LOCK_UN)
	err := l.file.Close()
	l.file = nil
	return err
}

// DefaultPath resolves the lock file location.
//
// $XDG_RUNTIME_DIR first, then $TMPDIR, then /tmp. The order matters on macOS,
// where XDG_RUNTIME_DIR is essentially never set — so what looks like the
// fallback is in practice the normal path there, and $TMPDIR (per-user, under
// /var/folders) is a better landing place than the world-writable /tmp.
func DefaultPath() (string, error) {
	if dir := os.Getenv("XDG_RUNTIME_DIR"); dir != "" {
		return filepath.Join(dir, "evac.lock"), nil
	}
	// The uid in the filename matters below: /tmp is world-writable, and a
	// fixed name would let another user create the file first and block or
	// interfere with the lock.
	name := fmt.Sprintf("evac-%d.lock", os.Getuid())
	if dir := os.Getenv("TMPDIR"); dir != "" {
		return filepath.Join(dir, name), nil
	}
	return filepath.Join("/tmp", name), nil
}

// checkOwnership refuses a lock file owned by someone else or writable by
// others, which would mean a different user can interfere with it.
func checkOwnership(f *os.File, path string) error {
	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat lock file %s: %w", path, err)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return nil // unknown platform; the flock itself still protects us
	}
	if int(st.Uid) != os.Getuid() {
		return fmt.Errorf("lock file %s is owned by uid %d, not %d — refusing to use it",
			path, st.Uid, os.Getuid())
	}
	if perm := fi.Mode().Perm(); perm&0o022 != 0 {
		return fmt.Errorf("lock file %s is writable by other users (mode %04o) — refusing to use it",
			path, perm)
	}
	return nil
}

// readHolder reads the holder metadata written by whoever holds the lock.
//
// This races with the holder's own write: the loser of the lock reads while
// holding nothing, so it can see an empty or half-written record. The fix is a
// bounded retry and then graceful degradation — deliberately *not* a
// write-to-temp-and-rename, which would swap the inode out from under the lock
// and let a third process acquire the path successfully. Correctness never
// depends on this metadata; it is diagnostic text only.
func readHolder(path string) *Holder {
	for attempt := range 3 {
		if attempt > 0 {
			time.Sleep(20 * time.Millisecond)
		}
		b, err := os.ReadFile(path)
		if err != nil || len(b) == 0 {
			continue
		}
		if h := parseHolder(string(b)); h != nil {
			return h
		}
	}
	return nil
}

func parseHolder(s string) *Holder {
	h := &Holder{}
	for _, line := range strings.Split(s, "\n") {
		k, v, ok := strings.Cut(strings.TrimSpace(line), "=")
		if !ok {
			continue
		}
		switch k {
		case "pid":
			h.PID, _ = strconv.Atoi(v)
		case "context":
			h.Context = v
		case "started":
			h.Started, _ = time.Parse(time.RFC3339, v)
		}
	}
	if h.PID == 0 {
		return nil // incomplete write
	}
	return h
}
