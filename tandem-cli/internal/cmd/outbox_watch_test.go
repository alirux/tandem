package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alirux/tandem/tandem-cli/internal/client"
	"github.com/alirux/tandem/tandem-cli/internal/output"
)

// watchUntilCancelled runs `outbox summary --watch` against server with args appended,
// under a context that cancels after budget - the test's stand-in for Ctrl+C, since a
// real SIGINT can't be delivered to the current process from within a test.
func watchUntilCancelled(t *testing.T, server *httptest.Server, budget time.Duration, extraArgs ...string) (stdout, stderr string, execErr error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	root := NewRootCmd()
	root.SetContext(ctx)
	var out, errOut bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&errOut)
	args := append([]string{"--base-url", server.URL, "outbox", "summary", "--watch"}, extraArgs...)
	root.SetArgs(args)

	execErr = root.Execute()
	return out.String(), errOut.String(), execErr
}

func TestOutboxSummaryWatch_humanRendersARepeatingBarChartDashboard(t *testing.T) {
	server := newFixtureServer(t, map[string]route{
		"GET /outbox/summary": {200, "outbox_summary.json"},
	})
	stdout, _, err := watchUntilCancelled(t, server, 130*time.Millisecond, "--interval", "20ms")
	if err != nil {
		t.Fatalf("Execute() error = %v, want nil (clean stop on context cancellation)", err)
	}

	for _, want := range []string{"PENDING", "IN_FLIGHT", "FAILED", "█", "░", "done:", "discarded:", "lagCount:"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("stdout missing %q:\n%s", want, stdout)
		}
	}
	frames := strings.Count(stdout, "Tandem outbox —")
	if frames < 2 {
		t.Errorf("expected multiple refreshes within 130ms at a 20ms interval, got %d frame(s):\n%s", frames, stdout)
	}
}

func TestOutboxSummaryWatch_jsonStreamsOneRawLinePerIntervalNoEnvelope(t *testing.T) {
	server := newFixtureServer(t, map[string]route{
		"GET /outbox/summary": {200, "outbox_summary.json"},
	})
	stdout, _, err := watchUntilCancelled(t, server, 130*time.Millisecond, "--interval", "20ms", "--output", "json")
	if err != nil {
		t.Fatalf("Execute() error = %v, want nil", err)
	}

	// The fixture is pretty-printed, so frames are not literally one-line-each; decode
	// consecutive raw JSON values instead of splitting on "\n" to count them.
	dec := json.NewDecoder(strings.NewReader(stdout))
	samples := 0
	for {
		var v map[string]any
		err := dec.Decode(&v)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("stdout is not a stream of raw JSON objects: %v\n%s", err, stdout)
		}
		if _, ok := v["lagCount"]; !ok {
			t.Errorf("decoded object missing lagCount, not a raw summary passthrough: %v", v)
		}
		samples++
	}
	if samples < 2 {
		t.Fatalf("expected multiple refreshes within the test budget, got %d:\n%s", samples, stdout)
	}
	if strings.Contains(stdout, "Tandem outbox —") {
		t.Errorf("json mode must not add the human dashboard header, got:\n%s", stdout)
	}
}

func TestOutboxSummaryWatch_zeroIntervalIsAUsageErrorBeforeAnyHTTPCall(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.WriteHeader(500)
	}))
	defer server.Close()

	_, _, code := execute(t, server.URL, "outbox", "summary", "--watch", "--interval", "0s")
	if code != 2 {
		t.Errorf("exit code = %d, want 2 (UsageError)", code)
	}
	if requests != 0 {
		t.Errorf("server received %d requests, want 0", requests)
	}
}

func TestOutboxSummaryWatch_aFailedRequestIsShownInlineAndTheLoopKeepsGoing(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/problem+json")
		w.WriteHeader(500)
		w.Write([]byte(`{"type":"https://tandem.codingful.com/problems/internal-error","title":"boom","status":500}`))
	}))
	defer server.Close()

	stdout, _, err := watchUntilCancelled(t, server, 130*time.Millisecond, "--interval", "20ms")
	if err != nil {
		t.Fatalf("Execute() error = %v, want nil - a request failure must not stop the watch loop", err)
	}
	occurrences := strings.Count(stdout, "Error:")
	if occurrences < 2 {
		t.Errorf("expected the loop to keep retrying past the failure (multiple Error: frames), got %d:\n%s", occurrences, stdout)
	}
}

func TestOutboxSummaryWatch_json_aFailedRequestGoesToStderrNotStdout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/problem+json")
		w.WriteHeader(500)
		w.Write([]byte(`{"type":"https://tandem.codingful.com/problems/internal-error","title":"boom","status":500}`))
	}))
	defer server.Close()

	stdout, stderr, err := watchUntilCancelled(t, server, 130*time.Millisecond, "--interval", "20ms", "--output", "json")
	if err != nil {
		t.Fatalf("Execute() error = %v, want nil - a request failure must not stop the watch loop", err)
	}

	if stdout != "" {
		t.Errorf("stdout must stay a clean, empty-or-valid NDJSON stream on a request failure, got %q", stdout)
	}
	if occurrences := strings.Count(stderr, "Error:"); occurrences < 2 {
		t.Errorf("expected the loop to keep retrying past the failure (multiple Error: lines on stderr), got %d:\n%s", occurrences, stderr)
	}
	if strings.Contains(stderr, "Tandem outbox —") {
		t.Errorf("json mode's error line must not use the human dashboard frame text, got:\n%s", stderr)
	}
}

// TestWriteSummaryDashboard_liveBarsStayReadableWhenDoneDwarfsThem is the regression test
// for the bug a user's question caught in review: DONE/DISCARDED accumulate for the
// outbox's whole lifetime (a row leaves DONE only when cleanup prunes it), so in any
// system with real traffic they are almost always far larger than the "live" states. If
// the bar scale were shared across all five counts, DONE would pin its own bar full and
// collapse PENDING/FAILED - the two that actually indicate a problem - to zero-width
// slivers indistinguishable from "nothing pending". This asserts the live bars are scaled
// against each other only, so a small-but-nonzero PENDING/FAILED next to a huge DONE still
// renders a visible, non-empty bar.
func TestWriteSummaryDashboard_liveBarsStayReadableWhenDoneDwarfsThem(t *testing.T) {
	done := int64(10_000_000)
	pending := int64(50)
	failed := int64(3)
	inFlight := int64(0)
	discarded := int64(0)
	summary := client.OutboxSummary{
		Counts: struct {
			DISCARDED *int64 `json:"DISCARDED,omitempty"`
			DONE      *int64 `json:"DONE,omitempty"`
			FAILED    *int64 `json:"FAILED,omitempty"`
			INFLIGHT  *int64 `json:"IN_FLIGHT,omitempty"`
			PENDING   *int64 `json:"PENDING,omitempty"`
		}{DONE: &done, PENDING: &pending, FAILED: &failed, INFLIGHT: &inFlight, DISCARDED: &discarded},
	}

	var b strings.Builder
	writeSummaryDashboard(&b, summary, false)
	rendered := b.String()

	pendingLine := lineContaining(t, rendered, "PENDING")
	if !strings.Contains(pendingLine, "█") {
		t.Errorf("PENDING (50, nonzero) rendered with no filled cells despite DONE=10,000,000 dwarfing it: %q", pendingLine)
	}
	failedLine := lineContaining(t, rendered, "FAILED")
	if !strings.Contains(failedLine, "█") {
		t.Errorf("FAILED (3, nonzero) rendered with no filled cells despite DONE=10,000,000 dwarfing it: %q", failedLine)
	}
	doneLine := lineContaining(t, rendered, "done:")
	if strings.Contains(doneLine, "█") || !strings.HasSuffix(doneLine, "10000000") {
		t.Errorf("expected DONE as a plain, right-aligned number, not a bar, got: %q", doneLine)
	}
}

func TestWriteSummaryDashboard_colorsOnlyTheFilledBarCells(t *testing.T) {
	// Deliberately unequal, so each bar is partially filled (except FAILED, which sets
	// max) - exercising a real empty-cells segment, not just a fully- or zero-filled bar.
	pending := int64(1)
	inFlight := int64(2)
	failed := int64(3)
	summary := client.OutboxSummary{
		Counts: struct {
			DISCARDED *int64 `json:"DISCARDED,omitempty"`
			DONE      *int64 `json:"DONE,omitempty"`
			FAILED    *int64 `json:"FAILED,omitempty"`
			INFLIGHT  *int64 `json:"IN_FLIGHT,omitempty"`
			PENDING   *int64 `json:"PENDING,omitempty"`
		}{PENDING: &pending, INFLIGHT: &inFlight, FAILED: &failed},
	}

	var b strings.Builder
	writeSummaryDashboard(&b, summary, true)
	rendered := b.String()

	const barWidth = 30 // must match writeSummaryDashboard's own const
	cases := []struct {
		row   string
		value int64
		color output.Color
	}{
		{"PENDING", pending, output.Yellow},
		{"IN_FLIGHT", inFlight, output.Green},
		{"FAILED", failed, output.Red},
	}
	for _, c := range cases {
		line := lineContaining(t, rendered, c.row)
		if strings.HasPrefix(line, string(c.color)) {
			t.Errorf("%s line = %q, the label must stay uncolored (line starts with the label, not a color code)", c.row, line)
		}
		wantBar := output.ColorBar(c.value, failed, barWidth, c.color, true)
		if !strings.Contains(line, wantBar) {
			t.Errorf("%s line = %q, want it to contain the color-filled-cells-only bar %q", c.row, line, wantBar)
		}
	}
}

func TestWriteSummaryDashboard_valuesAreRightAlignedIntoAStableColumn(t *testing.T) {
	// Different digit counts on purpose (1, 2, 3 digits) - the whole point of
	// right-alignment is that values keep lining up as their digit count changes
	// between refreshes (LLD-cli.md §3.1: the numbers swing every interval).
	pending := int64(7)
	inFlight := int64(42)
	failed := int64(100)
	summary := client.OutboxSummary{
		Counts: struct {
			DISCARDED *int64 `json:"DISCARDED,omitempty"`
			DONE      *int64 `json:"DONE,omitempty"`
			FAILED    *int64 `json:"FAILED,omitempty"`
			INFLIGHT  *int64 `json:"IN_FLIGHT,omitempty"`
			PENDING   *int64 `json:"PENDING,omitempty"`
		}{PENDING: &pending, INFLIGHT: &inFlight, FAILED: &failed},
	}

	var b strings.Builder
	writeSummaryDashboard(&b, summary, false) // color off: byte length == visible width
	rendered := b.String()

	cases := []struct {
		row   string
		value int64
	}{
		{"PENDING", pending},
		{"IN_FLIGHT", inFlight},
		{"FAILED", failed},
	}
	const valueWidth = 3 // len("100"), the widest of the three
	var lineLen int
	for i, c := range cases {
		line := lineContaining(t, rendered, c.row)
		if i == 0 {
			lineLen = len(line)
		} else if len(line) != lineLen {
			t.Errorf("%s line length = %d, want %d (all three rows same length so the value column stays put)", c.row, len(line), lineLen)
		}
		wantSuffix := fmt.Sprintf("%*d", valueWidth, c.value)
		if !strings.HasSuffix(line, wantSuffix) {
			t.Errorf("%s line = %q, want it to end with the right-aligned value %q", c.row, line, wantSuffix)
		}
	}
}

func TestWriteSummaryDashboard_doneDiscardedLagAlsoRightAlignInTheirOwnColumn(t *testing.T) {
	// Different digit counts across done/discarded/lagCount/lagAgeSeconds on purpose,
	// same reasoning as the live-row test above - this is a separate column (a different
	// label width), so it needs its own alignment, not a side effect of the live one's.
	done := int64(7)
	discarded := int64(4200)
	summary := client.OutboxSummary{
		Counts: struct {
			DISCARDED *int64 `json:"DISCARDED,omitempty"`
			DONE      *int64 `json:"DONE,omitempty"`
			FAILED    *int64 `json:"FAILED,omitempty"`
			INFLIGHT  *int64 `json:"IN_FLIGHT,omitempty"`
			PENDING   *int64 `json:"PENDING,omitempty"`
		}{DONE: &done, DISCARDED: &discarded},
		LagCount:      12,
		LagAgeSeconds: 3.5,
	}

	var b strings.Builder
	writeSummaryDashboard(&b, summary, false)
	rendered := b.String()

	cases := []struct {
		row, wantValue string
	}{
		{"done:", "7"},
		{"discarded:", "4200"},
		{"lagCount:", "12"},
		{"lagAgeSeconds:", "3.5"},
	}
	const valueWidth = 4 // len("4200"), the widest of the four
	var lineLen int
	for i, c := range cases {
		line := lineContaining(t, rendered, c.row)
		if i == 0 {
			lineLen = len(line)
		} else if len(line) != lineLen {
			t.Errorf("%s line length = %d, want %d (all four rows same length)", c.row, len(line), lineLen)
		}
		wantSuffix := fmt.Sprintf("%*s", valueWidth, c.wantValue)
		if !strings.HasSuffix(line, wantSuffix) {
			t.Errorf("%s line = %q, want it to end with the right-aligned value %q", c.row, line, wantSuffix)
		}
	}
}

func TestWriteSummaryDashboard_noColorCodesWhenDisabled(t *testing.T) {
	pending := int64(1)
	summary := client.OutboxSummary{
		Counts: struct {
			DISCARDED *int64 `json:"DISCARDED,omitempty"`
			DONE      *int64 `json:"DONE,omitempty"`
			FAILED    *int64 `json:"FAILED,omitempty"`
			INFLIGHT  *int64 `json:"IN_FLIGHT,omitempty"`
			PENDING   *int64 `json:"PENDING,omitempty"`
		}{PENDING: &pending},
	}

	var b strings.Builder
	writeSummaryDashboard(&b, summary, false)
	if strings.Contains(b.String(), "\033[") {
		t.Errorf("color=false must emit no ANSI codes, got:\n%s", b.String())
	}
}

func lineContaining(t *testing.T, text, substr string) string {
	t.Helper()
	for _, line := range strings.Split(text, "\n") {
		if strings.Contains(line, substr) {
			return line
		}
	}
	t.Fatalf("no line containing %q in:\n%s", substr, text)
	return ""
}
