package cmd

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/spf13/cobra"

	"github.com/alirux/tandem/tandem-cli/internal/client"
	"github.com/alirux/tandem/tandem-cli/internal/exitcode"
	"github.com/alirux/tandem/tandem-cli/internal/output"
)

func newRelayCmd() *cobra.Command {
	relay := &cobra.Command{
		Use:   "relay",
		Short: "Inspect and control the relay",
	}
	relay.AddCommand(
		newRelayStatusCmd(),
		newRelayPauseCmd(),
		newRelayResumeCmd(),
		newRelayBucketsCmd(),
		newRelayReleaseBucketCmd(),
		newRelayWorkersCmd(),
	)
	return relay
}

// relayStatusPairs colors the state with the traffic-light severity palette shared with
// the outbox dashboard (LLD-cli.md §3.1): green RUNNING, yellow PAUSED (deliberate, worth
// a second look), red DOWN (nothing has heartbeated - needs a human). DOWN wins over
// PAUSED server-side (HLD-admin-api §4.1), so this never has to choose between them. A
// future additive enum value falls through uncolored rather than guessing a severity.
func relayStatusPairs(app *App, s client.RelayStatus) [][2]string {
	state := string(s.State)
	switch s.State {
	case client.RUNNING:
		state = output.Colorize(state, output.Green, app.IO.Color)
	case client.PAUSED:
		state = output.Colorize(state, output.Yellow, app.IO.Color)
	case client.DOWN:
		state = output.Colorize(state, output.Red, app.IO.Color)
	}
	return [][2]string{
		{"state", state},
		{"bucketCount", fmt.Sprint(s.BucketCount)},
		{"uncoveredBuckets", fmt.Sprint(s.UncoveredBuckets)},
		{"workers", fmt.Sprint(s.Workers)},
	}
}

func newRelayStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Relay running state, bucket coverage, and worker count",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			app := appFrom(cmd)
			body, err := do(app.Client.GetRelayStatus(cmd.Context()))
			if err != nil {
				return err
			}
			return renderObject(app, body, relayStatusPairs)
		},
	}
}

// bucketSelectorCall is the shape both PauseRelay and ResumeRelay have: the two commands
// differ only in which one they call, so newPauseResumeCmd takes the call itself rather
// than re-deriving it from the command's own name.
type bucketSelectorCall func(ctx context.Context, body client.BucketSelector,
	reqEditors ...client.RequestEditorFn) (*http.Response, error)

func newRelayPauseCmd() *cobra.Command {
	return newPauseResumeCmd("pause", "Pause the relay (optionally a single bucket)",
		func(app *App) bucketSelectorCall { return app.Client.PauseRelay })
}

func newRelayResumeCmd() *cobra.Command {
	return newPauseResumeCmd("resume", "Resume the relay (optionally a single bucket)",
		func(app *App) bucketSelectorCall { return app.Client.ResumeRelay })
}

func newPauseResumeCmd(use, short string, call func(*App) bucketSelectorCall) *cobra.Command {
	var bucket int
	cmd := &cobra.Command{
		Use:   use,
		Short: short,
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			app := appFrom(cmd)
			selector := client.BucketSelector{}
			if cmd.Flags().Changed("bucket") {
				selector.Bucket = &bucket
			}
			body, err := do(call(app)(cmd.Context(), selector))
			if err != nil {
				return err
			}
			return renderObject(app, body, relayStatusPairs)
		},
	}
	cmd.Flags().IntVar(&bucket, "bucket", 0, "act on a single bucket (requires LEASE coordination)")
	return cmd
}

// coveredCell colors both values: unlike paused below, "not covered" (nothing is draining
// this bucket) is as worth flagging as the healthy case is worth confirming.
func coveredCell(covered bool, color bool) string {
	c := output.Green
	if !covered {
		c = output.Red
	}
	return output.Colorize(fmt.Sprint(covered), c, color)
}

// pausedCell colors only true - an operator-initiated exception worth noticing. false is
// the boring default and stays plain (same "highlight the exception only" rule as §3.1).
func pausedCell(paused bool, color bool) string {
	if !paused {
		return fmt.Sprint(paused)
	}
	return output.Colorize(fmt.Sprint(paused), output.Yellow, color)
}

func bucketStatusPairs(app *App, b client.BucketStatus) [][2]string {
	pairs := [][2]string{
		{"bucket", fmt.Sprint(b.Bucket)},
		{"covered", coveredCell(b.Covered, app.IO.Color)},
		{"paused", pausedCell(b.Paused, app.IO.Color)},
		{"pendingCount", fmt.Sprint(b.PendingCount)},
	}
	if b.Owner != nil {
		pairs = append(pairs, [2]string{"owner", *b.Owner})
	}
	if b.LeaseUntil != nil {
		pairs = append(pairs, [2]string{"leaseUntil", b.LeaseUntil.Format(time.RFC3339)})
	}
	if b.LagAgeSeconds != nil {
		pairs = append(pairs, [2]string{"lagAgeSeconds", fmt.Sprint(*b.LagAgeSeconds)})
	}
	return pairs
}

func bucketStatusRow(app *App, b client.BucketStatus) []string {
	return []string{
		fmt.Sprint(b.Bucket),
		coveredCell(b.Covered, app.IO.Color),
		pausedCell(b.Paused, app.IO.Color),
		optional(b.Owner, func(s string) string { return s }),
		fmt.Sprint(b.PendingCount),
		optional(b.LagAgeSeconds, func(f float32) string { return fmt.Sprint(f) }),
	}
}

func newRelayBucketsCmd() *cobra.Command {
	var uncoveredOnly bool
	cmd := &cobra.Command{
		Use:   "buckets [bucket]",
		Short: "Per-bucket ownership and lag - all buckets, or one",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			app := appFrom(cmd)

			if len(args) == 1 {
				if cmd.Flags().Changed("uncovered-only") {
					return exitcode.New(exitcode.UsageError,
						"--uncovered-only applies only when listing every bucket, not a single <bucket>")
				}
				bucket, err := parseInt[int]("bucket", args[0])
				if err != nil {
					return err
				}
				body, err := do(app.Client.GetRelayBucket(cmd.Context(), bucket))
				if err != nil {
					return err
				}
				return renderObject(app, body, bucketStatusPairs)
			}

			params := &client.GetRelayBucketsParams{}
			if uncoveredOnly {
				params.UncoveredOnly = &uncoveredOnly
			}
			body, err := do(app.Client.GetRelayBuckets(cmd.Context(), params))
			if err != nil {
				return err
			}
			header := []string{"BUCKET", "COVERED", "PAUSED", "OWNER", "PENDING", "LAG_AGE_SECONDS"}
			return renderList(app, body, header, bucketStatusRow)
		},
	}
	cmd.Flags().BoolVar(&uncoveredOnly, "uncovered-only", false, "list only buckets with no live owner (list form only)")
	return cmd
}

func newRelayReleaseBucketCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "release-bucket <bucket>",
		Short: "Force-release a bucket for reassignment (zombie owner recovery)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			bucket, err := parseInt[int]("bucket", args[0])
			if err != nil {
				return err
			}
			app := appFrom(cmd)
			body, err := do(app.Client.ReleaseBucket(cmd.Context(), bucket))
			if err != nil {
				return err
			}
			return renderObject(app, body, bucketStatusPairs)
		},
	}
}

func workerRow(_ *App, w client.WorkerInfo) []string {
	return []string{
		w.WorkerId,
		fmt.Sprint(w.BucketCount),
		optional(w.LastHeartbeat, func(t time.Time) string { return t.Format(time.RFC3339) }),
	}
}

func newRelayWorkersCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "workers",
		Short: "Active relay workers",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			app := appFrom(cmd)
			body, err := do(app.Client.GetRelayWorkers(cmd.Context()))
			if err != nil {
				return err
			}
			header := []string{"WORKER_ID", "BUCKET_COUNT", "LAST_HEARTBEAT"}
			return renderList(app, body, header, workerRow)
		},
	}
}
