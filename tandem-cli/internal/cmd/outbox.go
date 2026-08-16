package cmd

import (
	"encoding/json"
	"fmt"
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
			app := appFrom(cmd)
			if watch {
				if interval <= 0 {
					return exitcode.New(exitcode.UsageError, "--interval must be greater than zero")
				}
				return watchOutboxSummary(cmd, app, interval)
			}
			body, err := do(app.Client.GetOutboxSummary(cmd.Context()))
			if err != nil {
				return err
			}
			return renderObject(app, body, summaryPairs)
		},
	}
	cmd.Flags().BoolVar(&watch, "watch", false, "refresh continuously instead of taking one reading")
	cmd.Flags().DurationVar(&interval, "interval", 2*time.Second, "refresh period with --watch")
	return cmd
}

func summaryPairs(_ *App, summary client.OutboxSummary) [][2]string {
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
	var status, aggregateID, aggregateType, eventType, createdFrom, createdTo, correlationID, cursor string
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
			if correlationID != "" {
				params.CorrelationId = &correlationID
			}
			if cursor != "" {
				params.Cursor = &cursor
			}
			if limit > 0 {
				params.Limit = &limit
			}
			if createdFrom != "" {
				t, err := parseTime("--created-from", createdFrom)
				if err != nil {
					return err
				}
				params.CreatedFrom = &t
			}
			if createdTo != "" {
				t, err := parseTime("--created-to", createdTo)
				if err != nil {
					return err
				}
				params.CreatedTo = &t
			}

			body, err := do(app.Client.SearchOutboxMessages(cmd.Context(), params))
			if err != nil {
				return err
			}
			// Not renderList: the response is a page object, not a bare array, and human
			// mode has to print the continuation hint after the table.
			if app.Output == output.JSON {
				return writeErr(output.Raw(app.IO.Out, body))
			}
			page, err := decode[client.OutboxEntryPage](body)
			if err != nil {
				return err
			}
			header := []string{"ID", "AGGREGATE_ID", "AGGREGATE_TYPE", "SEQ", "STATUS", "ATTEMPTS", "CREATED_AT"}
			if werr := output.Table(app.IO.Out, header, rows(app, page.Items, entryRow)); werr != nil {
				return writeErr(werr)
			}
			return writeErr(output.CursorHint(app.IO.Out, page.NextCursor))
		},
	}

	f := cmd.Flags()
	f.StringVar(&status, "status", "", "filter by status (PENDING, IN_FLIGHT, DONE, FAILED, DISCARDED)")
	f.StringVar(&aggregateID, "aggregate-id", "", "filter by aggregate id")
	f.StringVar(&aggregateType, "aggregate-type", "", "filter by aggregate type")
	f.StringVar(&eventType, "type", "", "filter by CloudEvents event type")
	f.StringVar(&createdFrom, "created-from", "", "filter: created at or after (RFC3339)")
	f.StringVar(&createdTo, "created-to", "", "filter: created at or before (RFC3339)")
	f.StringVar(&correlationID, "correlation-id", "", "filter by correlation id (matches every message of one business operation)")
	f.IntVar(&limit, "limit", 0, "page size, 1-500 (default 50 server-side)")
	f.StringVar(&cursor, "cursor", "", "opaque cursor from a previous page")
	return cmd
}

func entryRow(_ *App, e client.OutboxEntry) []string {
	return []string{
		fmt.Sprint(e.Id), e.AggregateId, e.AggregateType, fmt.Sprint(e.Seq),
		string(e.Status), fmt.Sprint(e.Attempts), e.CreatedAt.Format(time.RFC3339),
	}
}

func newOutboxGetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "get <id>",
		Short: "Get one outbox message, with payload and headers",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := parseInt[int64]("id", args[0])
			if err != nil {
				return err
			}
			app := appFrom(cmd)
			body, err := do(app.Client.GetOutboxMessage(cmd.Context(), id))
			if err != nil {
				return err
			}
			return renderObject(app, body, entryPairs)
		},
	}
}

func entryPairs(_ *App, e client.OutboxEntry) [][2]string {
	pairs := [][2]string{
		{"id", fmt.Sprint(e.Id)},
		{"aggregateId", e.AggregateId},
		{"aggregateType", e.AggregateType},
		{"seq", fmt.Sprint(e.Seq)},
		{"status", string(e.Status)},
		{"attempts", fmt.Sprint(e.Attempts)},
		{"createdAt", e.CreatedAt.Format(time.RFC3339)},
	}
	// Shown whenever the server reports it, zero included: "replays 0" means this message was never
	// replayed, while the line being absent means the admin instance predates the field.
	if e.Replays != nil {
		pairs = append(pairs, [2]string{"replays", fmt.Sprint(*e.Replays)})
	}
	if e.Type != nil {
		pairs = append(pairs, [2]string{"type", *e.Type})
	}
	if e.CorrelationId != nil {
		pairs = append(pairs, [2]string{"correlationId", *e.CorrelationId})
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
			id, err := parseInt[int64]("id", args[0])
			if err != nil {
				return err
			}
			app := appFrom(cmd)
			body, err := do(app.Client.ReplayMessage(cmd.Context(), id))
			if err != nil {
				return err
			}
			return renderObject(app, body, entryPairs)
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
			id, err := parseInt[int64]("id", args[0])
			if err != nil {
				return err
			}
			// Stricter than the contract on purpose (LLD-cli.md §5): DiscardRequest.reason
			// is optional there, required here, since discard is irreversible and the
			// reason is what a later investigation has to go on.
			if reason == "" {
				return exitcode.New(exitcode.UsageError, "--reason is required for discard")
			}
			app := appFrom(cmd)
			if err := confirm(app, fmt.Sprintf(
				"Discard message %d? This is irreversible and breaks per-aggregate ordering.", id)); err != nil {
				return err
			}

			body, err := do(app.Client.DiscardMessage(cmd.Context(), id, client.DiscardMessageJSONRequestBody{
				AcknowledgeOrderingBreak: true,
				Reason:                   &reason,
			}))
			if err != nil {
				return err
			}
			return renderObject(app, body, entryPairs)
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
			if err := confirmReplayBulk(cmd, app, body); err != nil {
				return err
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
func confirmReplayBulk(cmd *cobra.Command, app *App, body client.ReplayBulkJSONRequestBody) error {
	if app.Yes {
		return nil
	}
	const refusal = "replay-bulk requires --yes or --dry-run when not attached to a terminal"
	if !app.IO.CanPrompt() {
		return exitcode.New(exitcode.ConfirmationRequired, "%s", refusal)
	}

	preview := body
	dryRun := true
	preview.DryRun = &dryRun
	respBody, err := do(app.Client.ReplayBulk(cmd.Context(), preview))
	if err != nil {
		return err
	}
	result, err := decode[client.ReplayResult](respBody)
	if err != nil {
		return err
	}
	return promptYesNo(app, fmt.Sprintf("Replay %d matched message(s)?", result.Matched), refusal)
}

func runReplayBulk(cmd *cobra.Command, app *App, body client.ReplayBulkJSONRequestBody, dryRun bool) error {
	body.DryRun = &dryRun
	respBody, err := do(app.Client.ReplayBulk(cmd.Context(), body))
	if err != nil {
		return err
	}
	return renderObject(app, respBody, replayResultPairs)
}

func replayResultPairs(_ *App, result client.ReplayResult) [][2]string {
	return [][2]string{
		{"matched", fmt.Sprint(result.Matched)},
		{"replayed", fmt.Sprint(result.Replayed)},
		{"dryRun", fmt.Sprint(result.DryRun)},
	}
}
