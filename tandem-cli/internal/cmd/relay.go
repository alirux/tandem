package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
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

// relayStatusPairs colors the state value with the same severity palette as the outbox
// summary dashboard (LLD-cli.md §3.1) - green for RUNNING (healthy, same meaning as
// IN_FLIGHT: work is actively moving), yellow for PAUSED (an operator-initiated,
// deliberate state, same meaning as PENDING: not broken, but worth a second look), red for
// DOWN (no relay instance has heartbeated recently - same meaning as the outbox's FAILED,
// something needs a human). DOWN takes priority over PAUSED server-side (HLD-admin-api
// §4.1: "is anything running at all" matters more than the desired-state flag), so this
// switch never needs to choose between them. A future additive enum value the contract
// adds (forward compatibility, §1.4) falls through uncolored rather than guessing a
// severity for it.
func relayStatusPairs(s client.RelayStatus, color bool) [][2]string {
	state := string(s.State)
	switch s.State {
	case client.RUNNING:
		state = output.Colorize(state, output.Green, color)
	case client.PAUSED:
		state = output.Colorize(state, output.Yellow, color)
	case client.DOWN:
		state = output.Colorize(state, output.Red, color)
	}
	return [][2]string{
		{"state", state},
		{"bucketCount", fmt.Sprint(s.BucketCount)},
		{"uncoveredBuckets", fmt.Sprint(s.UncoveredBuckets)},
		{"workers", fmt.Sprint(s.Workers)},
	}
}

// colorEnabled is the shared gate for every relay/outbox command that colors its output:
// a real terminal, and NO_COLOR unset (https://no-color.org) - same as the --watch
// dashboard, extended here to relay status/pause/resume's single-shot rendering too.
func colorEnabled() bool {
	return isTerminal(os.Stdout) && os.Getenv("NO_COLOR") == ""
}

func newRelayStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Relay running state, bucket coverage, and worker count",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			app := appFrom(cmd)
			body, _, cerr := do(app.Client.GetRelayStatus(cmd.Context()))
			if cerr != nil {
				return cerr
			}
			if app.Output == output.JSON {
				return writeErr(output.Raw(app.Stdout, body))
			}
			var s client.RelayStatus
			if uerr := json.Unmarshal(body, &s); uerr != nil {
				return exitcode.Wrap(exitcode.UnexpectedError, uerr, "parsing response")
			}
			return writeErr(output.KeyValue(app.Stdout, relayStatusPairs(s, colorEnabled())))
		},
	}
}

func newRelayPauseCmd() *cobra.Command {
	return newPauseResumeCmd("pause", "Pause the relay (optionally a single bucket)")
}

func newRelayResumeCmd() *cobra.Command {
	return newPauseResumeCmd("resume", "Resume the relay (optionally a single bucket)")
}

func newPauseResumeCmd(use, short string) *cobra.Command {
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

			var body []byte
			var cerr *exitcode.Error
			if use == "pause" {
				body, _, cerr = do(app.Client.PauseRelay(cmd.Context(), selector))
			} else {
				body, _, cerr = do(app.Client.ResumeRelay(cmd.Context(), selector))
			}
			if cerr != nil {
				return cerr
			}
			if app.Output == output.JSON {
				return writeErr(output.Raw(app.Stdout, body))
			}
			var s client.RelayStatus
			if uerr := json.Unmarshal(body, &s); uerr != nil {
				return exitcode.Wrap(exitcode.UnexpectedError, uerr, "parsing response")
			}
			return writeErr(output.KeyValue(app.Stdout, relayStatusPairs(s, colorEnabled())))
		},
	}
	cmd.Flags().IntVar(&bucket, "bucket", 0, "act on a single bucket (requires LEASE coordination)")
	return cmd
}

// coveredCell colors covered the same way relayStatusPairs colors RUNNING/PAUSED: green
// for true (a healthy, owned bucket), red for false (nothing is draining it right now) -
// unlike paused below, both values are worth flagging, not just the exceptional one.
func coveredCell(covered bool, color bool) string {
	c := output.Green
	if !covered {
		c = output.Red
	}
	return output.Colorize(fmt.Sprint(covered), c, color)
}

// pausedCell colors paused only when true - an operator-initiated exception worth
// noticing (same "only highlight the exception" rule as the outbox dashboard's bars);
// false is the boring default and stays plain, same as relayStatusPairs has no color
// for a hypothetical non-RUNNING/PAUSED state.
func pausedCell(paused bool, color bool) string {
	if !paused {
		return fmt.Sprint(paused)
	}
	return output.Colorize(fmt.Sprint(paused), output.Yellow, color)
}

func bucketStatusPairs(b client.BucketStatus, color bool) [][2]string {
	pairs := [][2]string{
		{"bucket", fmt.Sprint(b.Bucket)},
		{"covered", coveredCell(b.Covered, color)},
		{"paused", pausedCell(b.Paused, color)},
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
				bucket, ierr := parseInt("bucket", args[0])
				if ierr != nil {
					return ierr
				}
				body, _, cerr := do(app.Client.GetRelayBucket(cmd.Context(), bucket))
				if cerr != nil {
					return cerr
				}
				if app.Output == output.JSON {
					return writeErr(output.Raw(app.Stdout, body))
				}
				var b client.BucketStatus
				if uerr := json.Unmarshal(body, &b); uerr != nil {
					return exitcode.Wrap(exitcode.UnexpectedError, uerr, "parsing response")
				}
				return writeErr(output.KeyValue(app.Stdout, bucketStatusPairs(b, colorEnabled())))
			}

			params := &client.GetRelayBucketsParams{}
			if uncoveredOnly {
				params.UncoveredOnly = &uncoveredOnly
			}
			body, _, cerr := do(app.Client.GetRelayBuckets(cmd.Context(), params))
			if cerr != nil {
				return cerr
			}
			if app.Output == output.JSON {
				return writeErr(output.Raw(app.Stdout, body))
			}
			var buckets []client.BucketStatus
			if uerr := json.Unmarshal(body, &buckets); uerr != nil {
				return exitcode.Wrap(exitcode.UnexpectedError, uerr, "parsing response")
			}
			header := []string{"BUCKET", "COVERED", "PAUSED", "OWNER", "PENDING", "LAG_AGE_SECONDS"}
			color := colorEnabled()
			rows := make([][]string, 0, len(buckets))
			for _, b := range buckets {
				owner := ""
				if b.Owner != nil {
					owner = *b.Owner
				}
				lag := ""
				if b.LagAgeSeconds != nil {
					lag = fmt.Sprint(*b.LagAgeSeconds)
				}
				rows = append(rows, []string{
					fmt.Sprint(b.Bucket), coveredCell(b.Covered, color), pausedCell(b.Paused, color),
					owner, fmt.Sprint(b.PendingCount), lag,
				})
			}
			return writeErr(output.Table(app.Stdout, header, rows))
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
			bucket, ierr := parseInt("bucket", args[0])
			if ierr != nil {
				return ierr
			}
			app := appFrom(cmd)
			body, _, cerr := do(app.Client.ReleaseBucket(cmd.Context(), bucket))
			if cerr != nil {
				return cerr
			}
			if app.Output == output.JSON {
				return writeErr(output.Raw(app.Stdout, body))
			}
			var b client.BucketStatus
			if uerr := json.Unmarshal(body, &b); uerr != nil {
				return exitcode.Wrap(exitcode.UnexpectedError, uerr, "parsing response")
			}
			return writeErr(output.KeyValue(app.Stdout, bucketStatusPairs(b, colorEnabled())))
		},
	}
}

func newRelayWorkersCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "workers",
		Short: "Active relay workers",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			app := appFrom(cmd)
			body, _, cerr := do(app.Client.GetRelayWorkers(cmd.Context()))
			if cerr != nil {
				return cerr
			}
			if app.Output == output.JSON {
				return writeErr(output.Raw(app.Stdout, body))
			}
			var workers []client.WorkerInfo
			if uerr := json.Unmarshal(body, &workers); uerr != nil {
				return exitcode.Wrap(exitcode.UnexpectedError, uerr, "parsing response")
			}
			header := []string{"WORKER_ID", "BUCKET_COUNT", "LAST_HEARTBEAT"}
			rows := make([][]string, 0, len(workers))
			for _, w := range workers {
				heartbeat := ""
				if w.LastHeartbeat != nil {
					heartbeat = w.LastHeartbeat.Format(time.RFC3339)
				}
				rows = append(rows, []string{w.WorkerId, fmt.Sprint(w.BucketCount), heartbeat})
			}
			return writeErr(output.Table(app.Stdout, header, rows))
		},
	}
}

func parseInt(name, raw string) (int, *exitcode.Error) {
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, exitcode.New(exitcode.UsageError, "invalid %s %q: must be an integer", name, raw)
	}
	return n, nil
}
