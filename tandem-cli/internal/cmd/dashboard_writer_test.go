package cmd

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

func TestDashboardWriter_firstFrameHasNoCursorMovement(t *testing.T) {
	var buf bytes.Buffer
	d := &dashboardWriter{w: &buf, inPlace: true}
	draw(t, d, "line one\nline two\n")

	if strings.Contains(buf.String(), "\033[") {
		t.Errorf("first frame must not move the cursor, nothing was drawn yet: %q", buf.String())
	}
}

func TestDashboardWriter_laterFramesMoveCursorUpByThePreviousFrameLineCountThenEraseToEnd(t *testing.T) {
	var buf bytes.Buffer
	d := &dashboardWriter{w: &buf, inPlace: true}
	draw(t, d, "a\nb\nc\n") // 3 lines
	buf.Reset()             // isolate the second Draw's output

	draw(t, d, "x\ny\n")

	got := buf.String()
	wantPrefix := fmt.Sprintf("\033[%dA\033[J", 3)
	if !strings.HasPrefix(got, wantPrefix) {
		t.Errorf("got %q, want it to start with cursor-up-3 + erase-to-end %q", got, wantPrefix)
	}
	if !strings.HasSuffix(got, "x\ny\n") {
		t.Errorf("got %q, want the new frame's content after the escape codes", got)
	}
}

func TestDashboardWriter_neverMovesTheCursorWhenNotInPlace(t *testing.T) {
	var buf bytes.Buffer
	d := &dashboardWriter{w: &buf, inPlace: false}
	draw(t, d, "a\nb\n")
	draw(t, d, "c\nd\n")

	got := buf.String()
	if strings.Contains(got, "\033[") {
		t.Errorf("inPlace=false (redirected output) must never emit ANSI codes, got %q", got)
	}
	if got != "a\nb\nc\nd\n" {
		t.Errorf("got %q, want frames printed one after another unmodified", got)
	}
}

func TestDashboardWriter_neverEmitsAFullScreenClear(t *testing.T) {
	// The whole point of this type: no \033[2J (clear entire screen) and no \033[H
	// (cursor to absolute origin) - only a relative cursor-up plus erase-to-end.
	var buf bytes.Buffer
	d := &dashboardWriter{w: &buf, inPlace: true}
	draw(t, d, "a\n")
	draw(t, d, "b\n")

	if strings.Contains(buf.String(), "\033[2J") || strings.Contains(buf.String(), "\033[H") {
		t.Errorf("must never emit a full-screen clear or cursor-to-origin, got %q", buf.String())
	}
}

func TestDashboardWriter_reportsAFailureWritingTheFrame(t *testing.T) {
	d := &dashboardWriter{w: failWriter{}, inPlace: true}
	if err := d.Draw("a\n"); err == nil {
		t.Error("Draw() = nil, want the write failure reported so the watch loop can stop")
	}
}

func TestDashboardWriter_reportsAFailureWritingTheCursorMovement(t *testing.T) {
	// The redraw escape sequence is written before the frame itself, so a terminal that
	// goes away between frames fails there first - that write is checked too, not just
	// the frame's.
	var buf bytes.Buffer
	d := &dashboardWriter{w: &buf, inPlace: true}
	draw(t, d, "a\n") // establishes lines > 0, so the next Draw emits the escape first
	d.w = failWriter{}
	if err := d.Draw("b\n"); err == nil {
		t.Error("Draw() = nil, want the cursor-movement write failure reported")
	}
}

// draw is Draw with its (now reported) write error failing the test - every case here
// writes to a bytes.Buffer, which cannot fail, so a non-nil error is a real regression.
func draw(t *testing.T, d *dashboardWriter, frame string) {
	t.Helper()
	if err := d.Draw(frame); err != nil {
		t.Fatalf("Draw(%q) = %v, want nil", frame, err)
	}
}
