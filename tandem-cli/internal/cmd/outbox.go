package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/alirux/tandem/tandem-cli/internal/client"
	"github.com/alirux/tandem/tandem-cli/internal/exitcode"
	"github.com/alirux/tandem/tandem-cli/internal/output"
)

func newOutboxCmd() *cobra.Command {
	outbox := &cobra.Command{
		Use:   "outbox",
		Short: "Inspect and act on outbox messages",
	}
	outbox.AddCommand(
		newOutboxSummaryCmd(),
		newOutboxSearchCmd(),
		newOutboxGetCmd(),
		newOutboxReplayCmd(),
		newOutboxDiscardCmd(),
		newOutboxReplayBulkCmd(),
	)
	return outbox
}

func newOutboxSummaryCmd() *cobra.Command {
	var watch bool
	var interval time.Duration

	cmd := &cobra.Command{
		Use:   "summary",
		Short: "Outbox health summary: counts per status, plus lag",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if watch {
				if interval <= 0 {
					return exitcode.New(exitcode.UsageError, "--interval must be greater than zero")
				}
				return watchOutboxSummary(cmd, appFrom(cmd), interval)
			}
			return runOutboxSummaryOnce(cmd, appFrom(cmd))
		},
	}
	cmd.Flags().BoolVar(&watch, "watch", false, "refresh continuously instead of taking one reading")
	cmd.Flags().DurationVar(&interval, "interval", 2*time.Second, "refresh period with --watch")
	return cmd
}

func runOutboxSummaryOnce(cmd *cobra.Command, app *App) error {
	body, _, cerr := do(app.Client.GetOutboxSummary(cmd.Context()))
	if cerr != nil {
		return cerr
	}
	if app.Output == output.JSON {
		return writeErr(output.Raw(app.Stdout, body))
	}
	var summary client.OutboxSummary
	if uerr := json.Unmarshal(body, &summary); uerr != nil {
		return exitcode.Wrap(exitcode.UnexpectedError, uerr, "parsing response")
	}
	return writeErr(output.KeyValue(app.Stdout, summaryPairs(summary)))
}

func summaryPairs(summary client.OutboxSummary) [][2]string {
	return [][2]string{
		{"pending", count(summary.Counts.PENDING)},
		{"inFlight", count(summary.Counts.INFLIGHT)},
		{"done", count(summary.Counts.DONE)},
		{"failed", count(summary.Counts.FAILED)},
		{"discarded", count(summary.Counts.DISCARDED)},
		{"lagCount", fmt.Sprint(summary.LagCount)},
		{"lagAgeSeconds", fmt.Sprint(summary.LagAgeSeconds)},
	}
}

// watchOutboxSummary polls GetOutboxSummary every interval until the context is
// cancelled (Ctrl+C, or a context a caller/test set a deadline on) and re-renders each
// time. --output json streams one raw response line per interval (still no CLI-invented
// envelope, LLD-cli.md §6 - a consumer wanting a timestamp per sample adds one itself,
// e.g. `| ts`); --output human redraws a bar-chart dashboard instead of the plain
// key:value block runOutboxSummaryOnce uses, since comparing several counts at a glance
// is the whole point of watching. A transient request failure is shown inline and the
// loop keeps going - a dashboard dying on one dropped connection would defeat its
// purpose - so this only ever returns nil (clean stop) or a *exitcode.Error for a setup
// problem that will not fix itself.
func watchOutboxSummary(cmd *cobra.Command, app *App, interval time.Duration) error {
	ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt)
	defer stop()

	dash := &dashboardWriter{
		w:       app.Stdout,
		inPlace: app.Output == output.Human && isTerminal(os.Stdout),
	}
	// colorEnabled (relay.go) is the shared real-terminal-and-NO_COLOR-unset gate every
	// colored command uses - relay status/pause/resume included.
	color := colorEnabled()
	for {
		body, _, cerr := do(app.Client.GetOutboxSummary(ctx))
		switch {
		// A failure in JSON mode goes to stderr, never stdout: stdout is an NDJSON
		// stream a consumer parses line by line (LLD-cli.md §3.1), and the human
		// "Tandem outbox — ... Error: ..." frame text is not valid JSON - printing
		// it to stdout here would corrupt that stream on the very first hiccup.
		case cerr != nil && app.Output == output.JSON:
			fmt.Fprintf(app.Stderr, "Error: %v\n", cerr)
		case cerr != nil:
			dash.Draw(renderWatchFrame(interval, func(w *strings.Builder) {
				fmt.Fprintf(w, "Error: %v\n", cerr)
			}))
		case app.Output == output.JSON:
			if werr := output.Raw(app.Stdout, body); werr != nil {
				return exitcode.Wrap(exitcode.UnexpectedError, werr, "writing output")
			}
		default:
			var summary client.OutboxSummary
			if uerr := json.Unmarshal(body, &summary); uerr != nil {
				return exitcode.Wrap(exitcode.UnexpectedError, uerr, "parsing response")
			}
			dash.Draw(renderWatchFrame(interval, func(w *strings.Builder) {
				writeSummaryDashboard(w, summary, color)
			}))
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(interval):
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

// dashboardWriter redraws successive frames in place on a real terminal, instead of each
// one scrolling the previous off screen: move the cursor up by exactly the previous
// frame's line count, then clear from there to the end of the screen (ANSI "cursor up" +
// "erase in display, from cursor") before printing the new one. Deliberately not a
// full-screen clear-and-home (`\033[2J\033[H]`) - that resets to the terminal's absolute
// origin and wipes everything visible, including whatever was on screen above the
// dashboard before --watch started; this only ever touches the dashboard's own lines.
// Off a real terminal (inPlace false - piped to a file, redirected), frames are left to
// print one after another instead, since there is no cursor to move.
type dashboardWriter struct {
	w       io.Writer
	inPlace bool
	lines   int
}

func (d *dashboardWriter) Draw(frame string) {
	if d.inPlace && d.lines > 0 {
		fmt.Fprintf(d.w, "\033[%dA\033[J", d.lines)
	}
	fmt.Fprint(d.w, frame)
	d.lines = strings.Count(frame, "\n")
}

// writeSummaryDashboard bar-charts only the "live" states - PENDING, IN_FLIGHT, FAILED -
// scaled against the largest of those three, so a growing backlog or a stuck failure is
// visible at a glance. DONE and DISCARDED are deliberately plain numbers, not bars on the
// same scale: both are counts of rows still sitting in the table after they finished (a
// row leaves DONE only when the relay's periodic cleanup prunes it, LLD-jdbc §3.7), so
// they accumulate over the outbox's lifetime and, in any system with real traffic, quickly
// dwarf the live states. Sharing one scale across all five would pin DONE's bar full and
// collapse PENDING/FAILED - the two that actually indicate a problem - into invisible
// slivers, defeating the dashboard's purpose right when it matters most.
//
// Each live row's *filled* bar cells get a fixed color when color is enabled (LLD-cli.md
// §3.1) - yellow for PENDING (queued, not yet a problem), green for IN_FLIGHT (actively
// being delivered), red for FAILED (blocking its aggregate, needs an operator) - the same
// at-a-glance severity convention as traffic lights. The label, the trailing count, and
// the bar's empty cells stay uncolored on purpose: color marks "how much of this
// severity," not "this whole row is this severity," which a fully-colored line would read
// as even when the bar itself is nearly empty.
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

	var max int64
	labelWidth, valueWidth := 0, 0
	for _, r := range live {
		if r.value > max {
			max = r.value
		}
		if len(r.label) > labelWidth {
			labelWidth = len(r.label)
		}
		if w := len(fmt.Sprint(r.value)); w > valueWidth {
			valueWidth = w
		}
	}

	const barWidth = 30
	for _, r := range live {
		bar := output.ColorBar(r.value, max, barWidth, r.color, color)
		// %*d right-aligns the count in valueWidth, so the three counts form a stable
		// numeric column - values swing every refresh (LLD-cli.md §3.1's whole point),
		// and a left-aligned count jitters visually as its digit count changes.
		fmt.Fprintf(w, "%-*s  %s  %*d\n", labelWidth, r.label, bar, valueWidth, r.value)
	}
	// Same right-alignment treatment as the live rows above, in their own column (a
	// different label width, so a shared one would misalign both blocks).
	extra := []struct{ label, value string }{
		{"done", fmt.Sprint(countValue(summary.Counts.DONE))},
		{"discarded", fmt.Sprint(countValue(summary.Counts.DISCARDED))},
		{"lagCount", fmt.Sprint(summary.LagCount)},
		{"lagAgeSeconds", fmt.Sprintf("%g", summary.LagAgeSeconds)},
	}
	extraLabelWidth, extraValueWidth := 0, 0
	for _, e := range extra {
		if len(e.label) > extraLabelWidth {
			extraLabelWidth = len(e.label)
		}
		if len(e.value) > extraValueWidth {
			extraValueWidth = len(e.value)
		}
	}
	fmt.Fprintln(w)
	for _, e := range extra {
		fmt.Fprintf(w, "%-*s  %*s\n", extraLabelWidth+1, e.label+":", extraValueWidth, e.value)
	}
}

func count(n *int64) string {
	return fmt.Sprint(countValue(n))
}

func countValue(n *int64) int64 {
	if n == nil {
		return 0
	}
	return *n
}

func newOutboxSearchCmd() *cobra.Command {
	var status, aggregateID, aggregateType, eventType, createdFrom, createdTo, cursor string
	var limit int

	cmd := &cobra.Command{
		Use:   "search",
		Short: "Search outbox messages",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			app := appFrom(cmd)
			params := &client.SearchOutboxMessagesParams{}
			if status != "" {
				s := client.OutboxStatus(status)
				params.Status = &s
			}
			if aggregateID != "" {
				params.AggregateId = &aggregateID
			}
			if aggregateType != "" {
				params.AggregateType = &aggregateType
			}
			if eventType != "" {
				params.Type = &eventType
			}
			if cursor != "" {
				params.Cursor = &cursor
			}
			if limit > 0 {
				params.Limit = &limit
			}
			if createdFrom != "" {
				t, terr := parseTime("--created-from", createdFrom)
				if terr != nil {
					return terr
				}
				params.CreatedFrom = &t
			}
			if createdTo != "" {
				t, terr := parseTime("--created-to", createdTo)
				if terr != nil {
					return terr
				}
				params.CreatedTo = &t
			}

			body, _, cerr := do(app.Client.SearchOutboxMessages(cmd.Context(), params))
			if cerr != nil {
				return cerr
			}
			if app.Output == output.JSON {
				return writeErr(output.Raw(app.Stdout, body))
			}
			var page client.OutboxEntryPage
			if uerr := json.Unmarshal(body, &page); uerr != nil {
				return exitcode.Wrap(exitcode.UnexpectedError, uerr, "parsing response")
			}
			header := []string{"ID", "AGGREGATE_ID", "AGGREGATE_TYPE", "SEQ", "STATUS", "ATTEMPTS", "CREATED_AT"}
			rows := make([][]string, 0, len(page.Items))
			for _, e := range page.Items {
				rows = append(rows, []string{
					fmt.Sprint(e.Id), e.AggregateId, e.AggregateType, fmt.Sprint(e.Seq),
					string(e.Status), fmt.Sprint(e.Attempts), e.CreatedAt.Format(time.RFC3339),
				})
			}
			if werr := output.Table(app.Stdout, header, rows); werr != nil {
				return writeErr(werr)
			}
			output.CursorHint(app.Stdout, page.NextCursor)
			return nil
		},
	}

	f := cmd.Flags()
	f.StringVar(&status, "status", "", "filter by status (PENDING, IN_FLIGHT, DONE, FAILED, DISCARDED)")
	f.StringVar(&aggregateID, "aggregate-id", "", "filter by aggregate id")
	f.StringVar(&aggregateType, "aggregate-type", "", "filter by aggregate type")
	f.StringVar(&eventType, "type", "", "filter by CloudEvents event type")
	f.StringVar(&createdFrom, "created-from", "", "filter: created at or after (RFC3339)")
	f.StringVar(&createdTo, "created-to", "", "filter: created at or before (RFC3339)")
	f.IntVar(&limit, "limit", 0, "page size, 1-500 (default 50 server-side)")
	f.StringVar(&cursor, "cursor", "", "opaque cursor from a previous page")
	return cmd
}

func newOutboxGetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "get <id>",
		Short: "Get one outbox message, with payload and headers",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, ierr := parseID(args[0])
			if ierr != nil {
				return ierr
			}
			app := appFrom(cmd)
			body, _, cerr := do(app.Client.GetOutboxMessage(cmd.Context(), id))
			if cerr != nil {
				return cerr
			}
			if app.Output == output.JSON {
				return writeErr(output.Raw(app.Stdout, body))
			}
			var e client.OutboxEntry
			if uerr := json.Unmarshal(body, &e); uerr != nil {
				return exitcode.Wrap(exitcode.UnexpectedError, uerr, "parsing response")
			}
			return writeErr(output.KeyValue(app.Stdout, entryPairs(e)))
		},
	}
}

func entryPairs(e client.OutboxEntry) [][2]string {
	pairs := [][2]string{
		{"id", fmt.Sprint(e.Id)},
		{"aggregateId", e.AggregateId},
		{"aggregateType", e.AggregateType},
		{"seq", fmt.Sprint(e.Seq)},
		{"status", string(e.Status)},
		{"attempts", fmt.Sprint(e.Attempts)},
		{"createdAt", e.CreatedAt.Format(time.RFC3339)},
	}
	if e.Type != nil {
		pairs = append(pairs, [2]string{"type", *e.Type})
	}
	if e.LastError != nil {
		pairs = append(pairs, [2]string{"lastError", *e.LastError})
	}
	if e.DiscardReason != nil {
		pairs = append(pairs, [2]string{"discardReason", *e.DiscardReason})
	}
	if e.LockedBy != nil {
		pairs = append(pairs, [2]string{"lockedBy", *e.LockedBy})
	}
	if e.Payload != nil {
		payloadBytes, _ := json.Marshal(e.Payload)
		pairs = append(pairs, [2]string{"payload", string(payloadBytes)})
	}
	if e.Headers != nil {
		headerBytes, _ := json.Marshal(*e.Headers)
		pairs = append(pairs, [2]string{"headers", string(headerBytes)})
	}
	return pairs
}

func newOutboxReplayCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "replay <id>",
		Short: "Replay a single message (reset DONE or FAILED to PENDING)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, ierr := parseID(args[0])
			if ierr != nil {
				return ierr
			}
			app := appFrom(cmd)
			body, _, cerr := do(app.Client.ReplayMessage(cmd.Context(), id))
			if cerr != nil {
				return cerr
			}
			if app.Output == output.JSON {
				return writeErr(output.Raw(app.Stdout, body))
			}
			var e client.OutboxEntry
			if uerr := json.Unmarshal(body, &e); uerr != nil {
				return exitcode.Wrap(exitcode.UnexpectedError, uerr, "parsing response")
			}
			return writeErr(output.KeyValue(app.Stdout, entryPairs(e)))
		},
	}
}

func newOutboxDiscardCmd() *cobra.Command {
	var reason string

	cmd := &cobra.Command{
		Use:   "discard <id>",
		Short: "Discard a FAILED message (irreversible; unblocks the aggregate)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, ierr := parseID(args[0])
			if ierr != nil {
				return ierr
			}
			// Stricter than the contract on purpose (LLD-cli.md §5): DiscardRequest.reason
			// is optional there, required here, since discard is irreversible and the
			// reason is what a later investigation has to go on.
			if reason == "" {
				return exitcode.New(exitcode.UsageError, "--reason is required for discard")
			}
			app := appFrom(cmd)
			if cerr := confirm(app, app.Yes, fmt.Sprintf(
				"Discard message %d? This is irreversible and breaks per-aggregate ordering.", id)); cerr != nil {
				return cerr
			}

			ack := true
			body, _, cerr := do(app.Client.DiscardMessage(cmd.Context(), id, client.DiscardMessageJSONRequestBody{
				AcknowledgeOrderingBreak: ack,
				Reason:                   &reason,
			}))
			if cerr != nil {
				return cerr
			}
			if app.Output == output.JSON {
				return writeErr(output.Raw(app.Stdout, body))
			}
			var e client.OutboxEntry
			if uerr := json.Unmarshal(body, &e); uerr != nil {
				return exitcode.Wrap(exitcode.UnexpectedError, uerr, "parsing response")
			}
			return writeErr(output.KeyValue(app.Stdout, entryPairs(e)))
		},
	}
	cmd.Flags().StringVar(&reason, "reason", "", "reason for the discard, recorded for audit (required)")
	return cmd
}

func newOutboxReplayBulkCmd() *cobra.Command {
	var aggregateID, aggregateType string
	var fromID, toID int64
	var statuses []string
	var dryRun bool

	cmd := &cobra.Command{
		Use:   "replay-bulk",
		Short: "Replay all DONE/FAILED messages matching criteria",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			app := appFrom(cmd)
			body := client.ReplayBulkJSONRequestBody{}
			if aggregateID != "" {
				body.AggregateId = &aggregateID
			}
			if aggregateType != "" {
				body.AggregateType = &aggregateType
			}
			if fromID != 0 {
				body.FromId = &fromID
			}
			if toID != 0 {
				body.ToId = &toID
			}
			if len(statuses) > 0 {
				parsed := make([]client.OutboxStatus, len(statuses))
				for i, s := range statuses {
					parsed[i] = client.OutboxStatus(s)
				}
				body.Statuses = &parsed
			}

			if dryRun {
				return runReplayBulk(cmd, app, body, true)
			}
			if cerr := confirmReplayBulk(cmd, app, body); cerr != nil {
				return cerr
			}
			return runReplayBulk(cmd, app, body, false)
		},
	}

	f := cmd.Flags()
	f.StringVar(&aggregateID, "aggregate-id", "", "selector: aggregate id")
	f.StringVar(&aggregateType, "aggregate-type", "", "selector: aggregate type")
	f.Int64Var(&fromID, "from-id", 0, "selector: outbox id range start")
	f.Int64Var(&toID, "to-id", 0, "selector: outbox id range end")
	f.StringArrayVar(&statuses, "status", nil, "selector: eligible status (DONE or FAILED), repeatable")
	f.BoolVar(&dryRun, "dry-run", false, "preview the matched count without changing anything")
	return cmd
}

// confirmReplayBulk implements the two-shot --yes/TTY gate for a non-dry-run bulk replay
// (LLD-cli.md §5): on a TTY, it previews the match count via a real dryRun call before
// asking; piped, it requires --yes up front and never calls the API at all if it's absent.
func confirmReplayBulk(cmd *cobra.Command, app *App, body client.ReplayBulkJSONRequestBody) *exitcode.Error {
	if app.Yes {
		return nil
	}
	if !isTerminal(os.Stdout) {
		return exitcode.New(exitcode.ConfirmationRequired,
			"replay-bulk requires --yes or --dry-run when not attached to a terminal")
	}

	preview := body
	dryRun := true
	preview.DryRun = &dryRun
	respBody, _, cerr := do(app.Client.ReplayBulk(cmd.Context(), preview))
	if cerr != nil {
		return cerr
	}
	var result client.ReplayResult
	if uerr := json.Unmarshal(respBody, &result); uerr != nil {
		return exitcode.Wrap(exitcode.UnexpectedError, uerr, "parsing dry-run preview")
	}
	return confirm(app, false, fmt.Sprintf("Replay %d matched message(s)?", result.Matched))
}

func runReplayBulk(cmd *cobra.Command, app *App, body client.ReplayBulkJSONRequestBody, dryRun bool) error {
	body.DryRun = &dryRun
	respBody, _, cerr := do(app.Client.ReplayBulk(cmd.Context(), body))
	if cerr != nil {
		return cerr
	}
	if app.Output == output.JSON {
		return writeErr(output.Raw(app.Stdout, respBody))
	}
	var result client.ReplayResult
	if uerr := json.Unmarshal(respBody, &result); uerr != nil {
		return exitcode.Wrap(exitcode.UnexpectedError, uerr, "parsing response")
	}
	return writeErr(output.KeyValue(app.Stdout, [][2]string{
		{"matched", fmt.Sprint(result.Matched)},
		{"replayed", fmt.Sprint(result.Replayed)},
		{"dryRun", fmt.Sprint(result.DryRun)},
	}))
}

func parseID(raw string) (int64, *exitcode.Error) {
	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, exitcode.New(exitcode.UsageError, "invalid id %q: must be an integer", raw)
	}
	return id, nil
}

func parseTime(flag, raw string) (time.Time, *exitcode.Error) {
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, exitcode.New(exitcode.UsageError, "invalid %s %q: want RFC3339, e.g. 2026-08-05T00:00:00Z", flag, raw)
	}
	return t, nil
}
