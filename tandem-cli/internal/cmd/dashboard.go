package cmd

import (
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/alirux/tandem/tandem-cli/internal/client"
	"github.com/alirux/tandem/tandem-cli/internal/exitcode"
	"github.com/alirux/tandem/tandem-cli/internal/output"
)

// watchOutboxSummary polls GetOutboxSummary every interval until the context is cancelled
// (Ctrl+C, or a deadline a caller/test set) and re-renders each time - the live dashboard
// specified in LLD-cli.md §3.1. A transient request failure is shown in place of that
// frame and the loop keeps going, so this only ever returns nil (clean stop) or an error
// for a problem that will not fix itself.
func watchOutboxSummary(cmd *cobra.Command, app *App, interval time.Duration) error {
	ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt)
	defer stop()

	dash := &dashboardWriter{w: app.IO.Out, inPlace: app.Output == output.Human && app.IO.TTY}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		body, err := do(app.Client.GetOutboxSummary(ctx))
		switch {
		// A failure in JSON mode goes to stderr, never stdout: stdout is an NDJSON stream
		// a consumer parses line by line, and the human frame text is not valid JSON -
		// printing it there would corrupt the stream on the first hiccup (§3.1).
		case err != nil && app.Output == output.JSON:
			_, _ = fmt.Fprintf(app.IO.Err, "Error: %v\n", err)
		case err != nil:
			if derr := dash.Draw(renderWatchFrame(interval, func(w *strings.Builder) {
				fmt.Fprintf(w, "Error: %v\n", err)
			})); derr != nil {
				return derr
			}
		case app.Output == output.JSON:
			if werr := writeErr(output.Raw(app.IO.Out, body)); werr != nil {
				return werr
			}
		default:
			summary, derr := decode[client.OutboxSummary](body)
			if derr != nil {
				return derr
			}
			if derr := dash.Draw(renderWatchFrame(interval, func(w *strings.Builder) {
				writeSummaryDashboard(w, summary, app.IO.Color)
			})); derr != nil {
				return derr
			}
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// renderWatchFrame builds one dashboard frame's text: a timestamp header, then body.
func renderWatchFrame(interval time.Duration, body func(w *strings.Builder)) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Tandem outbox — %s (every %s, Ctrl+C to stop)\n\n",
		time.Now().Format("2006-01-02 15:04:05"), interval)
	body(&b)
	return b.String()
}

// dashboardWriter redraws successive frames in place on a real terminal: move the cursor
// up by exactly the previous frame's line count, then erase from there to the end of the
// screen, before printing the new one. Deliberately not a full-screen clear-and-home
// (\033[2J\033[H), which would also wipe whatever was on screen before --watch started -
// see LLD-cli.md §3.1. Off a terminal (inPlace false), frames simply print in sequence.
type dashboardWriter struct {
	w       io.Writer
	inPlace bool
	lines   int
}

func (d *dashboardWriter) Draw(frame string) error {
	if d.inPlace && d.lines > 0 {
		if _, err := fmt.Fprintf(d.w, "\033[%dA\033[J", d.lines); err != nil {
			return exitcode.Wrap(exitcode.UnexpectedError, err, "writing output")
		}
	}
	if _, err := io.WriteString(d.w, frame); err != nil {
		return exitcode.Wrap(exitcode.UnexpectedError, err, "writing output")
	}
	d.lines = strings.Count(frame, "\n")
	return nil
}

// barWidth is the fixed cell width of every live-status bar.
const barWidth = 30

// writeSummaryDashboard bar-charts only the live states - PENDING, IN_FLIGHT, FAILED -
// scaled against the largest of those three. DONE and DISCARDED are deliberately plain
// numbers on no shared scale, because they accumulate for the outbox's lifetime and would
// otherwise collapse the live bars to invisible slivers; see LLD-cli.md §3.1 for the full
// reasoning. Filled bar cells carry a traffic-light severity color; the label, the count,
// and the empty cells stay plain, so color reads as "how much of this severity" rather
// than "this whole row is this severity".
func writeSummaryDashboard(w *strings.Builder, summary client.OutboxSummary, color bool) {
	live := []struct {
		label string
		value int64
		color output.Color
	}{
		{"PENDING", countValue(summary.Counts.PENDING), output.Yellow},
		{"IN_FLIGHT", countValue(summary.Counts.INFLIGHT), output.Green},
		{"FAILED", countValue(summary.Counts.FAILED), output.Red},
	}

	var scale int64
	labelWidth, valueWidth := 0, 0
	for _, r := range live {
		if r.value > scale {
			scale = r.value
		}
		labelWidth = wider(labelWidth, r.label)
		valueWidth = wider(valueWidth, fmt.Sprint(r.value))
	}

	for _, r := range live {
		// %*d right-aligns the count, so the three form a stable numeric column - the
		// values swing every refresh, and a left-aligned count jitters as digits change.
		fmt.Fprintf(w, "%-*s  %s  %*d\n",
			labelWidth, r.label, output.ColorBar(r.value, scale, barWidth, r.color, color), valueWidth, r.value)
	}

	// Same right-alignment, in its own column: a different label width, so sharing one
	// with the live rows above would misalign both blocks.
	extra := []struct{ label, value string }{
		{"done", fmt.Sprint(countValue(summary.Counts.DONE))},
		{"discarded", fmt.Sprint(countValue(summary.Counts.DISCARDED))},
		{"lagCount", fmt.Sprint(summary.LagCount)},
		{"lagAgeSeconds", fmt.Sprintf("%g", summary.LagAgeSeconds)},
	}
	extraLabelWidth, extraValueWidth := 0, 0
	for _, e := range extra {
		extraLabelWidth = wider(extraLabelWidth, e.label)
		extraValueWidth = wider(extraValueWidth, e.value)
	}
	fmt.Fprintln(w)
	for _, e := range extra {
		fmt.Fprintf(w, "%-*s  %*s\n", extraLabelWidth+1, e.label+":", extraValueWidth, e.value)
	}
}

func wider(width int, s string) int {
	if len(s) > width {
		return len(s)
	}
	return width
}
