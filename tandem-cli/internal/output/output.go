// Package output renders Admin API responses in the two modes LLD-cli.md §6 specifies:
// human (tab-aligned tables and key:value blocks, for people) and json (the raw response
// body, unmodified, for scripts and jq). Callers unmarshal into the generated client's
// typed models to build the human-mode rows; json mode never touches the parsed value at
// all, so an API field this build doesn't know about still comes through unmangled.
package output

import (
	"fmt"
	"io"
	"regexp"
	"strings"
	"text/tabwriter"
	"unicode/utf8"
)

// Mode selects how a command renders its result.
type Mode int

const (
	// Human is the default: tables for lists, key:value blocks for single objects.
	Human Mode = iota
	// JSON is the raw response body, unmodified, straight to the writer.
	JSON
)

// ParseMode validates the --output flag's value. An empty string (the flag's default)
// means Human.
func ParseMode(s string) (Mode, error) {
	switch s {
	case "", "human":
		return Human, nil
	case "json":
		return JSON, nil
	default:
		return Human, fmt.Errorf("unknown --output %q, want \"human\" or \"json\"", s)
	}
}

// Raw writes body verbatim - json mode's entire contract: no CLI-invented envelope, the
// API's own shape reprinted exactly as received, so piping through jq stays predictable.
func Raw(w io.Writer, body []byte) error {
	if _, err := w.Write(body); err != nil {
		return err
	}
	if len(body) == 0 || body[len(body)-1] != '\n' {
		_, err := w.Write([]byte("\n"))
		return err
	}
	return nil
}

// KeyValue renders a single-object response (summary, get, status) as aligned "key:
// value" lines, in the given order - order is the caller's to choose, since it is the one
// with knowledge of which fields matter most for that resource.
func KeyValue(w io.Writer, pairs [][2]string) error {
	tw := tabwriter.NewWriter(w, 0, 4, 1, ' ', 0)
	for _, p := range pairs {
		if _, err := fmt.Fprintf(tw, "%s:\t%s\n", p[0], p[1]); err != nil {
			return err
		}
	}
	return tw.Flush()
}

// Table renders a list/page response (search, buckets, workers) as a left-aligned,
// space-padded table; header and every row must have the same number of columns.
//
// Deliberately hand-rolled rather than text/tabwriter: column width is computed from each
// cell's *visible* width (visibleWidth, below), so a cell colorized with Colorize/ColorBar
// - invisible ANSI escape bytes and all - still lines up with the plain cells around it.
// tabwriter has no such concept; handing it colored cells directly would count the escape
// bytes as visible characters and misalign every column after the colored one, on exactly
// the rows where the color differs from its neighbors (found by working through a request
// to color relay buckets' COVERED/PAUSED columns - a table is the one rendering shape here
// where cells sit in the *middle* of a row, not just at the end).
func Table(w io.Writer, header []string, rows [][]string) error {
	widths := make([]int, len(header))
	for i, h := range header {
		widths[i] = visibleWidth(h)
	}
	for _, row := range rows {
		for i, cell := range row {
			if v := visibleWidth(cell); v > widths[i] {
				widths[i] = v
			}
		}
	}

	writeRow := func(cells []string) error {
		last := len(cells) - 1
		for i, cell := range cells {
			if _, err := io.WriteString(w, cell); err != nil {
				return err
			}
			if i == last {
				break
			}
			pad := widths[i] - visibleWidth(cell) + 2 // +2: the gap between columns
			if _, err := io.WriteString(w, strings.Repeat(" ", pad)); err != nil {
				return err
			}
		}
		_, err := io.WriteString(w, "\n")
		return err
	}

	if err := writeRow(header); err != nil {
		return err
	}
	for _, row := range rows {
		if err := writeRow(row); err != nil {
			return err
		}
	}
	return nil
}

// ansiEscape matches the SGR sequences Colorize/ColorBar emit (\033[<digits/semicolons>m),
// so visibleWidth can measure a cell's on-screen width regardless of whether it is colored.
var ansiEscape = regexp.MustCompile("\x1b\\[[0-9;]*m")

func visibleWidth(s string) int {
	if !strings.ContainsRune(s, '\x1b') {
		return utf8.RuneCountInString(s)
	}
	return utf8.RuneCountInString(ansiEscape.ReplaceAllString(s, ""))
}

// CursorHint prints the pagination-continuation hint human mode uses when a page ends
// with a non-null nextCursor. The API is forward-paging and cursor-only, so this is a
// hint to re-run with --cursor, never an auto-follow (LLD-cli.md §6). Reports a write
// failure like every other renderer here: it goes to stdout, so a broken pipe must reach
// the caller rather than be dropped.
func CursorHint(w io.Writer, nextCursor *string) error {
	if nextCursor == nil || *nextCursor == "" {
		return nil
	}
	_, err := fmt.Fprintf(w, "next page: --cursor=%s\n", *nextCursor)
	return err
}

// Color is an ANSI SGR foreground color code, for the --watch dashboard's per-status rows.
type Color string

const (
	Red    Color = "\033[31m"
	Green  Color = "\033[32m"
	Yellow Color = "\033[33m"

	colorReset = "\033[0m"
)

// Colorize wraps s in color's ANSI code when enabled, otherwise returns s unmodified.
// enabled is the caller's to decide (LLD-cli.md §3.1: a real terminal and NO_COLOR unset,
// https://no-color.org) - this function has no opinion of its own on when color belongs,
// only on how to apply it, so it stays pure and needs no *os.File/env-var access to test.
func Colorize(s string, color Color, enabled bool) string {
	if !enabled || s == "" {
		return s
	}
	return string(color) + s + colorReset
}

// ColorBar renders a fixed-width horizontal bar: value's share of scale as filled cells
// (█), the rest empty (░), used by --watch dashboards to show several counts on a shared
// scale at a glance. scale <= 0 (nothing to compare against yet) renders fully empty
// rather than dividing by zero; value is clamped to [0, scale] so one outlier can't
// overflow the width.
//
// Only the filled cells are colored when enabled - the empty cells stay plain, and
// neither a label nor the trailing count (both the caller's to add around this string)
// are touched. Color marks "how much of this," not "this whole row is this severity."
func ColorBar(value, scale int64, width int, color Color, enabled bool) string {
	filled, empty := barCells(value, scale, width)
	return Colorize(filled, color, enabled) + empty
}

func barCells(value, scale int64, width int) (filled, empty string) {
	if width <= 0 {
		return "", ""
	}
	if scale <= 0 {
		return "", strings.Repeat("░", width)
	}
	if value < 0 {
		value = 0
	}
	if value > scale {
		value = scale
	}
	n := int(float64(value) / float64(scale) * float64(width))
	if n > width {
		n = width
	}
	return strings.Repeat("█", n), strings.Repeat("░", width-n)
}
