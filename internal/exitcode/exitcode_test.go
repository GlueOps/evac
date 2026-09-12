package exitcode

import (
	"errors"
	"testing"
)

// The property the precedence exists to protect: a wrapper retries on 3, so 3
// must never mask a failure that needs a human.
func TestJobTimeoutNeverMasksAFailureNeedingAHuman(t *testing.T) {
	t.Parallel()
	for _, needsHuman := range []Code{Error, PVCStuck, EvictionTimeout} {
		if got := Combine(JobTimeout, needsHuman); got != needsHuman {
			t.Errorf("Combine(JobTimeout, %d) = %d, want %d — a wrapper retrying on a timer would loop forever", needsHuman, got, needsHuman)
		}
	}
}

func TestCombine(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		codes []Code
		want  Code
	}{
		{"all succeeded", []Code{OK, OK}, OK},
		{"single failure wins over success", []Code{OK, JobTimeout}, JobTimeout},
		{"api error outranks everything", []Code{Error, PVCStuck, EvictionTimeout, JobTimeout}, Error},
		{"stuck PVC outranks eviction timeout", []Code{EvictionTimeout, PVCStuck}, PVCStuck},
		{"nothing at all is success", nil, OK},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := Combine(tc.codes...); got != tc.want {
				t.Errorf("Combine(%v) = %d, want %d", tc.codes, got, tc.want)
			}
		})
	}
}

func TestOfUnwrapsThroughWrappedErrors(t *testing.T) {
	t.Parallel()
	base := Wrap(Preflight, errors.New("capacity shortfall"))
	wrapped := errors.Join(errors.New("context"), base)

	if got := Of(wrapped); got != Preflight {
		t.Errorf("Of = %d, want %d", got, Preflight)
	}
	if got := Of(nil); got != OK {
		t.Errorf("Of(nil) = %d, want %d", got, OK)
	}
	if got := Of(errors.New("plain")); got != Error {
		t.Errorf("Of(plain) = %d, want %d", got, Error)
	}
}
