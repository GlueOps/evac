package cli

import "testing"

// --parallel had no test anywhere: the integration test that claimed to cover
// it never touched the flag, the resolution, or the "all" sentinel.

func TestParallelValueParsing(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in      string
		wantErr bool
	}{
		{"1", false},
		{"4", false},
		{"all", false},
		{"0", true}, // a drain with no workers would silently do nothing
		{"-1", true},
		{"", true},
		{"ALL", true}, // case-sensitive: a near miss must not parse as "all"
		{"2.5", true},
		{"two", true},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			var p parallelValue
			err := p.Set(tc.in)
			if tc.wantErr && err == nil {
				t.Fatalf("Set(%q) succeeded, want an error", tc.in)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("Set(%q) = %v", tc.in, err)
			}
		})
	}
}

// "all" means one worker per selected node, whatever that count turns out to
// be; an integer is a ceiling. Getting this wrong silently serialises a drain
// the operator asked to run concurrently, or the reverse.
func TestParallelWorkersResolution(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		set   string
		nodes int
		want  int
	}{
		{"unset defaults to serial", "", 5, 1},
		{"all matches the node count", "all", 5, 5},
		{"all with one node", "all", 1, 1},
		{"explicit count is honoured", "3", 5, 3},
		{"count above the node count is not clamped here", "9", 5, 9},
		{"one stays one", "1", 5, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var p parallelValue
			if tc.set != "" {
				if err := p.Set(tc.set); err != nil {
					t.Fatal(err)
				}
			}
			if got := p.workers(tc.nodes); got != tc.want {
				t.Errorf("workers(%d) = %d, want %d", tc.nodes, got, tc.want)
			}
		})
	}
}

// The zero value must be serial. Serial is the safe default because the first
// node reveals whether the capacity math was right, and a mistake stops after
// one node instead of all of them.
func TestZeroValueIsSerial(t *testing.T) {
	t.Parallel()
	var p parallelValue
	if got := p.workers(10); got != 1 {
		t.Errorf("workers = %d, want 1 — an unset --parallel must not fan out", got)
	}
	if got := p.String(); got != "1" {
		t.Errorf("String = %q, want \"1\" so --help shows the real default", got)
	}
}

func TestParallelValueString(t *testing.T) {
	t.Parallel()
	var p parallelValue
	if err := p.Set("all"); err != nil {
		t.Fatal(err)
	}
	if got := p.String(); got != "all" {
		t.Errorf("String = %q, want \"all\"", got)
	}
	if p.Type() == "" {
		t.Error("Type is empty; --help would show no value hint")
	}
}
