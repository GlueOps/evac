package cli

import (
	"fmt"
	"strconv"
)

// parallelValue implements pflag.Value for --parallel, which takes an integer
// or the literal "all".
//
// The value is deliberately required rather than optional: pflag's NoOptDefVal
// only works with --parallel=N syntax and silently breaks --parallel N, which
// is how people actually type it.
type parallelValue struct {
	n   int
	all bool
}

func (p *parallelValue) String() string {
	if p.all {
		return "all"
	}
	if p.n == 0 {
		return "1"
	}
	return strconv.Itoa(p.n)
}

func (p *parallelValue) Set(s string) error {
	if s == "all" {
		p.all, p.n = true, 0
		return nil
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return fmt.Errorf("want a positive integer or 'all', got %q", s)
	}
	if n < 1 {
		return fmt.Errorf("want at least 1, got %d", n)
	}
	p.all, p.n = false, n
	return nil
}

func (p *parallelValue) Type() string { return "int|all" }

// workers resolves the flag against the number of selected nodes.
func (p *parallelValue) workers(nodes int) int {
	if p.all {
		return nodes
	}
	if p.n < 1 {
		return 1
	}
	return p.n
}
