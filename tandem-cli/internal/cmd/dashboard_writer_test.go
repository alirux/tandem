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
	d.Draw("line one\nline two\n")

	if strings.Contains(buf.String(), "\033[") {
		t.Errorf("first frame must not move the cursor, nothing was drawn yet: %q", buf.String())
	}
}

func TestDashboardWriter_laterFramesMoveCursorUpByThePreviousFrameLineCountThenEraseToEnd(t *testing.T) {
	var buf bytes.Buffer
	d := &dashboardWriter{w: &buf, inPlace: true}
	d.Draw("a\nb\nc\n") // 3 lines
	buf.Reset()         // isolate the second Draw's output

	d.Draw("x\ny\n")

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
	d.Draw("a\nb\n")
	d.Draw("c\nd\n")

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
	d.Draw("a\n")
	d.Draw("b\n")

	if strings.Contains(buf.String(), "\033[2J") || strings.Contains(buf.String(), "\033[H") {
		t.Errorf("must never emit a full-screen clear or cursor-to-origin, got %q", buf.String())
	}
}
