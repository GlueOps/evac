// Package render turns domain data into the text tables §3 and §8 specify.
//
// stdlib text/tabwriter throughout: §9 requires the transcript be tee-able and
// greppable, which rules out borders and ANSI decoration, and the column
// alignment tabwriter gives is the whole requirement.
package render

import (
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"

	"golang.org/x/term"
)

// Column is one table column. Drop is the order in which columns are removed
// when the terminal is too narrow: higher goes first, and 0 means never drop.
type Column struct {
	Header string
	Drop   int
	Value  func(row int) string
}

// Table renders aligned columns, shedding low-priority ones when the terminal
// cannot fit them.
type Table struct {
	Columns []Column
	Rows    int
	// Width is the terminal width; 0 means "detect, and assume wide if not a
	// terminal" — piping to a file or to tee must never lose columns.
	Width int
}

const (
	// minWidth is the narrowest terminal worth trying to fit. Below this,
	// dropping more columns stops helping.
	minWidth = 60
	// colGap matches the tabwriter padding below.
	colGap = 2
)

// Render writes the table.
func (t Table) Render(w io.Writer) error {
	cols := t.fit()

	tw := tabwriter.NewWriter(w, 0, 0, colGap, ' ', 0)
	headers := make([]string, len(cols))
	for i, c := range cols {
		headers[i] = c.Header
	}
	if _, err := fmt.Fprintln(tw, strings.Join(headers, "\t")); err != nil {
		return err
	}
	for r := range t.Rows {
		cells := make([]string, len(cols))
		for i, c := range cols {
			cells[i] = c.Value(r)
		}
		if _, err := fmt.Fprintln(tw, strings.Join(cells, "\t")); err != nil {
			return err
		}
	}
	return tw.Flush()
}

// fit drops the widest-priority columns until the table fits, and always keeps
// at least the undroppable ones.
func (t Table) fit() []Column {
	width := t.Width
	if width == 0 {
		width = detectWidth()
	}
	cols := make([]Column, len(t.Columns))
	copy(cols, t.Columns)

	if width <= 0 {
		return cols // not a terminal: never drop
	}
	if width < minWidth {
		width = minWidth
	}

	for t.widthOf(cols) > width {
		victim, prio := -1, 0
		for i, c := range cols {
			if c.Drop > prio {
				victim, prio = i, c.Drop
			}
		}
		if victim < 0 {
			break // only undroppable columns remain
		}
		cols = append(cols[:victim], cols[victim+1:]...)
	}
	return cols
}

// widthOf measures the rendered width of a column set.
func (t Table) widthOf(cols []Column) int {
	total := 0
	for _, c := range cols {
		w := len(c.Header)
		for r := range t.Rows {
			if n := len(c.Value(r)); n > w {
				w = n
			}
		}
		total += w + colGap
	}
	return total
}

// detectWidth returns the terminal width, or 0 when stdout is not a terminal.
//
// Returning 0 rather than a default matters: when output is piped to a file or
// through tee, every column must survive, because that file is the §9 audit
// artifact.
func detectWidth() int {
	fd := int(os.Stdout.Fd())
	if !term.IsTerminal(fd) {
		return 0
	}
	w, _, err := term.GetSize(fd)
	if err != nil {
		return 0
	}
	return w
}
