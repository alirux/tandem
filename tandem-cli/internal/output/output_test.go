package output

import (
	"bytes"
	"strings"
	"testing"
)

func TestParseMode_emptyStringIsHuman(t *testing.T) {
	mode, err := ParseMode("")
	if err != nil {
		t.Fatalf("ParseMode(\"\") error = %v", err)
	}
	if mode != Human {
		t.Errorf("mode = %v, want Human", mode)
	}
}

func TestParseMode_recognizesJSON(t *testing.T) {
	mode, err := ParseMode("json")
	if err != nil {
		t.Fatalf("ParseMode(\"json\") error = %v", err)
	}
	if mode != JSON {
		t.Errorf("mode = %v, want JSON", mode)
	}
}

func TestParseMode_rejectsUnknownValue(t *testing.T) {
	if _, err := ParseMode("yaml"); err == nil {
		t.Fatal("ParseMode(\"yaml\") = nil error, want an error")
	}
}

func TestRaw_writesBodyUnmodifiedWhenAlreadyNewlineTerminated(t *testing.T) {
	var buf bytes.Buffer
	if err := Raw(&buf, []byte("{\"a\":1}\n")); err != nil {
		t.Fatalf("Raw() error = %v", err)
	}
	if got := buf.String(); got != "{\"a\":1}\n" {
		t.Errorf("output = %q, want unmodified body", got)
	}
}

func TestRaw_appendsATrailingNewlineWhenMissing(t *testing.T) {
	var buf bytes.Buffer
	if err := Raw(&buf, []byte("{\"a\":1}")); err != nil {
		t.Fatalf("Raw() error = %v", err)
	}
	if got := buf.String(); got != "{\"a\":1}\n" {
		t.Errorf("output = %q, want a trailing newline added", got)
	}
}

func TestKeyValue_rendersEachPairAsALine(t *testing.T) {
	var buf bytes.Buffer
	err := KeyValue(&buf, [][2]string{
		{"state", "RUNNING"},
		{"bucketCount", "256"},
	})
	if err != nil {
		t.Fatalf("KeyValue() error = %v", err)
	}
	got := buf.String()
	if !strings.Contains(got, "state:") || !strings.Contains(got, "RUNNING") {
		t.Errorf("output = %q, missing the state pair", got)
	}
	if !strings.Contains(got, "bucketCount:") || !strings.Contains(got, "256") {
		t.Errorf("output = %q, missing the bucketCount pair", got)
	}
}

func TestTable_rendersHeaderAndEveryRow(t *testing.T) {
	var buf bytes.Buffer
	err := Table(&buf,
		[]string{"ID", "STATUS"},
		[][]string{
			{"1", "DONE"},
			{"2", "FAILED"},
		})
	if err != nil {
		t.Fatalf("Table() error = %v", err)
	}
	got := buf.String()
	lines := strings.Split(strings.TrimRight(got, "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("got %d lines, want 3 (header + 2 rows): %q", len(lines), got)
	}
	if !strings.Contains(lines[0], "ID") || !strings.Contains(lines[0], "STATUS") {
		t.Errorf("header line = %q, missing column names", lines[0])
	}
	if !strings.Contains(lines[1], "1") || !strings.Contains(lines[1], "DONE") {
		t.Errorf("row 1 = %q", lines[1])
	}
	if !strings.Contains(lines[2], "2") || !strings.Contains(lines[2], "FAILED") {
		t.Errorf("row 2 = %q", lines[2])
	}
}

func TestTable_emptyRowsStillPrintsTheHeader(t *testing.T) {
	var buf bytes.Buffer
	if err := Table(&buf, []string{"ID"}, nil); err != nil {
		t.Fatalf("Table() error = %v", err)
	}
	if got := strings.TrimSpace(buf.String()); got != "ID" {
		t.Errorf("output = %q, want just the header", got)
	}
}

func TestTable_coloredCellsDoNotMisalignSubsequentColumns(t *testing.T) {
	// A colored cell ("true", wrapped in Red - ~9 invisible ANSI bytes) sits next to a
	// plain cell ("false") in the same column, across two rows - the exact shape that
	// breaks text/tabwriter, since it counts the invisible bytes as visible width.
	var buf bytes.Buffer
	err := Table(&buf,
		[]string{"COVERED", "OWNER"},
		[][]string{
			{Colorize("true", Red, true), "worker-1"},
			{"false", "worker-2"},
		})
	if err != nil {
		t.Fatalf("Table() error = %v", err)
	}
	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("got %d lines, want 3: %q", len(lines), buf.String())
	}
	// visibleWidth strips ANSI escapes before measuring, matching what a terminal
	// actually renders - a raw byte-offset comparison would flag the escape bytes
	// themselves as misalignment, which is not the bug this test guards against.
	ownerCol := func(line string) int { return strings.Index(ansiEscape.ReplaceAllString(line, ""), "worker-") }
	got1, got2 := ownerCol(lines[1]), ownerCol(lines[2])
	if got1 != got2 {
		t.Errorf("OWNER starts at visible column %d in row 1 but %d in row 2 - colored COVERED cell misaligned it:\n%s", got1, got2, buf.String())
	}
}

func TestCursorHint_printsHintWhenNextCursorIsPresent(t *testing.T) {
	var buf bytes.Buffer
	cursor := "abc123"
	CursorHint(&buf, &cursor)
	if got := buf.String(); got != "next page: --cursor=abc123\n" {
		t.Errorf("output = %q", got)
	}
}

func TestCursorHint_printsNothingWhenNextCursorIsNil(t *testing.T) {
	var buf bytes.Buffer
	CursorHint(&buf, nil)
	if buf.Len() != 0 {
		t.Errorf("output = %q, want empty", buf.String())
	}
}

func TestCursorHint_printsNothingWhenNextCursorIsEmptyString(t *testing.T) {
	var buf bytes.Buffer
	empty := ""
	CursorHint(&buf, &empty)
	if buf.Len() != 0 {
		t.Errorf("output = %q, want empty", buf.String())
	}
}

func TestColorize_wrapsInTheColorCodeWhenEnabled(t *testing.T) {
	got := Colorize("PENDING", Yellow, true)
	want := "\033[33mPENDING\033[0m"
	if got != want {
		t.Errorf("Colorize(...) = %q, want %q", got, want)
	}
}

func TestColorBar_onlyTheFilledCellsAreColored(t *testing.T) {
	got := ColorBar(5, 10, 10, Red, true)
	want := "\033[31m" + strings.Repeat("█", 5) + "\033[0m" + strings.Repeat("░", 5)
	if got != want {
		t.Errorf("ColorBar(5, 10, 10, Red, true) = %q, want %q", got, want)
	}
}

func TestColorBar_matchesPlainBarWhenDisabled(t *testing.T) {
	got := ColorBar(5, 10, 10, Red, false)
	want := Bar(5, 10, 10)
	if got != want {
		t.Errorf("ColorBar(..., false) = %q, want it to equal the plain Bar() = %q", got, want)
	}
}

func TestColorBar_emptyBarHasNoColorCodesAtAll(t *testing.T) {
	// value=0: nothing filled, so there is nothing to color - Colorize("", ...) must not
	// still emit a color/reset pair around an empty string.
	got := ColorBar(0, 10, 10, Red, true)
	if strings.Contains(got, "\033[") {
		t.Errorf("ColorBar(0, ...) = %q, want no ANSI codes when there are no filled cells", got)
	}
	if got != strings.Repeat("░", 10) {
		t.Errorf("ColorBar(0, ...) = %q, want fully empty", got)
	}
}

func TestColorize_returnsUnmodifiedWhenDisabled(t *testing.T) {
	got := Colorize("PENDING", Yellow, false)
	if got != "PENDING" {
		t.Errorf("Colorize(..., false) = %q, want the plain string unmodified", got)
	}
}

func TestBar_fullyFilledWhenValueEqualsMax(t *testing.T) {
	got := Bar(10, 10, 10)
	if got != strings.Repeat("█", 10) {
		t.Errorf("Bar(10, 10, 10) = %q, want fully filled", got)
	}
}

func TestBar_emptyWhenValueIsZero(t *testing.T) {
	got := Bar(0, 10, 10)
	if got != strings.Repeat("░", 10) {
		t.Errorf("Bar(0, 10, 10) = %q, want fully empty", got)
	}
}

func TestBar_halfFilledAtHalfTheMax(t *testing.T) {
	got := Bar(5, 10, 10)
	want := strings.Repeat("█", 5) + strings.Repeat("░", 5)
	if got != want {
		t.Errorf("Bar(5, 10, 10) = %q, want %q", got, want)
	}
}

func TestBar_zeroMaxRendersEmptyRatherThanDividingByZero(t *testing.T) {
	got := Bar(3, 0, 8)
	if got != strings.Repeat("░", 8) {
		t.Errorf("Bar(3, 0, 8) = %q, want fully empty", got)
	}
}

func TestBar_valueAboveMaxClampsToFull(t *testing.T) {
	got := Bar(999, 10, 6)
	if got != strings.Repeat("█", 6) {
		t.Errorf("Bar(999, 10, 6) = %q, want fully filled, not overflowing", got)
	}
}

func TestBar_negativeValueClampsToEmpty(t *testing.T) {
	got := Bar(-5, 10, 6)
	if got != strings.Repeat("░", 6) {
		t.Errorf("Bar(-5, 10, 6) = %q, want fully empty", got)
	}
}

func TestBar_zeroWidthIsEmptyString(t *testing.T) {
	if got := Bar(5, 10, 0); got != "" {
		t.Errorf("Bar(5, 10, 0) = %q, want empty string", got)
	}
}
