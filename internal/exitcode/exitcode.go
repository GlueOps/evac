// Package exitcode defines evac's process exit codes and how per-node codes
// combine into one.
//
// Distinct codes matter because "not finished yet" and "something is broken"
// call for different responses from a wrapper: the first is worth retrying on a
// timer, the second needs a human.
package exitcode

import "errors"

// Code is a process exit status.
type Code int

const (
	OK Code = 0
	// Error is an API failure or unexpected condition.
	Error Code = 1
	// Usage is a bad flag or an unreadable node file.
	Usage Code = 2
	// JobTimeout means phase 4's deadline passed with Job pods still running.
	// The only code a wrapper should retry on a timer.
	JobTimeout Code = 3
	// EvictionTimeout means phase 2's per-pod timeout expired, often a PDB stall.
	EvictionTimeout Code = 4
	// PVCStuck means a PVC stayed Terminating past its timeout.
	PVCStuck Code = 5
	// Preflight means a preflight check failed: capacity, affinity,
	// anti-affinity, or a selection whose workload would land on the control
	// plane. Distinct from ControlPlane below, which means the selection named
	// a control-plane node rather than merely targeting it as a destination.
	Preflight Code = 6
	// ControlPlane means the selection named a control-plane node.
	ControlPlane Code = 7
	// LockHeld means another drain is already running.
	LockHeld Code = 8
	// Aborted means the operator answered no at the confirmation prompt.
	Aborted Code = 9
	// Interrupted means SIGINT arrived mid-run.
	Interrupted Code = 130
)

// severity orders codes for aggregation under --parallel.
//
// The ordering exists to protect one specific property: JobTimeout is the only
// code a wrapper should retry on a timer, so it must never win over a node that
// needs a human. A wrapper seeing 3 while a PDB stall sits unresolved elsewhere
// would retry forever.
func severity(c Code) int {
	switch c {
	case Error:
		return 100
	case PVCStuck:
		return 90
	case EvictionTimeout:
		return 80
	case JobTimeout:
		return 70
	case OK:
		return 0
	case Interrupted:
		// Just above JobTimeout, and below everything that names a specific
		// broken thing. Interrupted is reachable per node, so under --parallel
		// a SIGINT could otherwise be outranked by a Job wait timing out
		// elsewhere and reported as 3 — the one code a wrapper is told it may
		// retry on a timer, which would retry a drain the operator stopped on
		// purpose. It stays below PVCStuck and EvictionTimeout because those
		// point at something the operator has to go and look at, whereas they
		// already know they pressed Ctrl-C.
		return 75
	default:
		return 50
	}
}

// Combine returns the code that should be reported when several nodes finished
// differently. Highest severity wins; OK only when everything succeeded.
func Combine(codes ...Code) Code {
	worst := OK
	for _, c := range codes {
		if severity(c) > severity(worst) {
			worst = c
		}
	}
	return worst
}

// Err carries an exit code alongside an error, so the command layer can decide
// the process status without inspecting error strings.
type Err struct {
	Code Code
	Err  error
}

func (e *Err) Error() string { return e.Err.Error() }
func (e *Err) Unwrap() error { return e.Err }

// Wrap attaches a code to an error.
func Wrap(c Code, err error) error {
	if err == nil {
		return nil
	}
	return &Err{Code: c, Err: err}
}

// Of returns the code carried by err, defaulting to Error for a plain error
// and OK for nil.
func Of(err error) Code {
	if err == nil {
		return OK
	}
	var e *Err
	if errors.As(err, &e) {
		return e.Code
	}
	return Error
}

// String names a code for humans.
//
// A per-node table reading "exit 5" makes the reader go and look the number
// up, at the moment they can least afford it. The number stays — it is the
// process contract — but it is no longer the only thing on the line.
func (c Code) String() string {
	switch c {
	case OK:
		return "drained"
	case Error:
		return "failed"
	case Usage:
		return "usage error"
	case JobTimeout:
		return "timed out waiting on Job pods"
	case EvictionTimeout:
		return "eviction timed out (often a PDB stall)"
	case PVCStuck:
		return "PVC stuck Terminating"
	case Preflight:
		return "preflight failed"
	case ControlPlane:
		return "refused: control plane node"
	case LockHeld:
		return "another drain holds the lock"
	case Aborted:
		return "aborted at the prompt"
	case Interrupted:
		return "interrupted"
	default:
		return "unknown"
	}
}
