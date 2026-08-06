package cmd

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alirux/tandem/tandem-cli/internal/client"
	"github.com/alirux/tandem/tandem-cli/internal/output"
)

func TestRelayStatus_humanRendersState(t *testing.T) {
	server := newFixtureServer(t, map[string]route{
		"GET /relay/status": {200, "relay_status.json"},
	})
	stdout, _, code := execute(t, server.URL, "relay", "status")
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stdout=%q", code, stdout)
	}
	for _, want := range []string{"state:", "RUNNING", "bucketCount:", "256", "workers:", "2"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("stdout = %q, missing %q", stdout, want)
		}
	}
}

func TestRelayStatus_apiErrorMapsToItsExitCode(t *testing.T) {
	server := newFixtureServer(t, map[string]route{
		"GET /relay/status": {500, "problem_internal_error.json"},
	})
	_, stderr, code := execute(t, server.URL, "relay", "status")
	if code != 1 {
		t.Errorf("exit code = %d, want 1 (UnexpectedError); stderr=%q", code, stderr)
	}
}

func TestRelayStatus_malformedResponseIsAnUnexpectedError(t *testing.T) {
	server := newFixtureServer(t, map[string]route{
		"GET /relay/status": {200, "malformed.json"},
	})
	_, stderr, code := execute(t, server.URL, "relay", "status")
	if code != 1 {
		t.Errorf("exit code = %d, want 1 (UnexpectedError); stderr=%q", code, stderr)
	}
}

func TestRelayStatusPairs_colorsRunningGreenPausedYellowDownRed(t *testing.T) {
	cases := []struct {
		state client.RelayStatusState
		color output.Color
	}{
		{client.RUNNING, output.Green},
		{client.PAUSED, output.Yellow},
		{client.DOWN, output.Red},
	}
	for _, c := range cases {
		pairs := relayStatusPairs(appWithColor(true), client.RelayStatus{State: c.state})
		want := [2]string{"state", output.Colorize(string(c.state), c.color, true)}
		if pairs[0] != want {
			t.Errorf("relayStatusPairs(%s, true)[0] = %v, want %v", c.state, pairs[0], want)
		}
	}
}

func TestRelayStatusPairs_noColorCodesWhenDisabled(t *testing.T) {
	pairs := relayStatusPairs(appWithColor(false), client.RelayStatus{State: client.RUNNING})
	if pairs[0] != [2]string{"state", "RUNNING"} {
		t.Errorf("relayStatusPairs(RUNNING, false)[0] = %v, want the plain unmodified state", pairs[0])
	}
}

func TestRelayStatusPairs_unknownFutureStateStaysUncolored(t *testing.T) {
	// Forward compatibility (AGENTS.md §1.4): an enum value this build predates must not
	// crash or guess a severity for it - it just passes through as plain text.
	pairs := relayStatusPairs(appWithColor(true), client.RelayStatus{State: "DRAINING"})
	if pairs[0] != [2]string{"state", "DRAINING"} {
		t.Errorf("relayStatusPairs(DRAINING, true)[0] = %v, want the plain unmodified state", pairs[0])
	}
}

func TestCoveredCell_bothValuesAreColoredGreenOrRed(t *testing.T) {
	if got, want := coveredCell(true, true), output.Colorize("true", output.Green, true); got != want {
		t.Errorf("coveredCell(true, true) = %q, want %q", got, want)
	}
	if got, want := coveredCell(false, true), output.Colorize("false", output.Red, true); got != want {
		t.Errorf("coveredCell(false, true) = %q, want %q", got, want)
	}
}

func TestCoveredCell_noColorCodesWhenDisabled(t *testing.T) {
	if got := coveredCell(true, false); got != "true" {
		t.Errorf("coveredCell(true, false) = %q, want the plain unmodified value", got)
	}
	if got := coveredCell(false, false); got != "false" {
		t.Errorf("coveredCell(false, false) = %q, want the plain unmodified value", got)
	}
}

func TestPausedCell_onlyTrueIsColoredYellow(t *testing.T) {
	if got, want := pausedCell(true, true), output.Colorize("true", output.Yellow, true); got != want {
		t.Errorf("pausedCell(true, true) = %q, want %q", got, want)
	}
	if got := pausedCell(false, true); got != "false" {
		t.Errorf("pausedCell(false, true) = %q, want the plain unmodified value (false is the boring default)", got)
	}
}

func TestPausedCell_noColorCodesWhenDisabled(t *testing.T) {
	if got := pausedCell(true, false); got != "true" {
		t.Errorf("pausedCell(true, false) = %q, want the plain unmodified value", got)
	}
}

func TestRelayPause_wholeRelaySendsAnEmptySelector(t *testing.T) {
	var body map[string]any
	bodySeen := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&body)
		bodySeen = true
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"state":"PAUSED","bucketCount":256,"uncoveredBuckets":0,"workers":2}`))
	}))
	defer server.Close()

	stdout, _, code := execute(t, server.URL, "relay", "pause")
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stdout=%q", code, stdout)
	}
	if !bodySeen {
		t.Fatal("server never received a request body")
	}
	if _, hasBucket := body["bucket"]; hasBucket && body["bucket"] != nil {
		t.Errorf("body[bucket] = %v, want omitted/null for a whole-relay pause", body["bucket"])
	}
	if !strings.Contains(stdout, "PAUSED") {
		t.Errorf("stdout = %q, missing the updated state", stdout)
	}
}

func TestRelayPause_withBucketSendsTheBucketNumber(t *testing.T) {
	var body map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&body)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"state":"RUNNING","bucketCount":256,"uncoveredBuckets":0,"workers":2}`))
	}))
	defer server.Close()

	_, _, code := execute(t, server.URL, "relay", "pause", "--bucket", "7")
	if code != 0 {
		t.Fatalf("exit code = %d, want 0", code)
	}
	if body["bucket"] != float64(7) {
		t.Errorf("body[bucket] = %v, want 7", body["bucket"])
	}
}

func TestRelayPause_coordinationUnsupportedMapsToExitCode6(t *testing.T) {
	server := newFixtureServer(t, map[string]route{
		"POST /relay/pause": {409, "problem_coordination_unsupported.json"},
	})
	_, stderr, code := execute(t, server.URL, "relay", "pause", "--bucket", "7")
	if code != 6 {
		t.Errorf("exit code = %d, want 6 (Conflict); stderr=%q", code, stderr)
	}
}

func TestRelayPauseResume_malformedResponseIsAnUnexpectedError(t *testing.T) {
	server := newFixtureServer(t, map[string]route{
		"POST /relay/pause": {200, "malformed.json"},
	})
	_, stderr, code := execute(t, server.URL, "relay", "pause")
	if code != 1 {
		t.Errorf("exit code = %d, want 1 (UnexpectedError); stderr=%q", code, stderr)
	}
}

func TestRelayResume_wholeRelay(t *testing.T) {
	server := newFixtureServer(t, map[string]route{
		"POST /relay/resume": {200, "relay_status.json"},
	})
	stdout, _, code := execute(t, server.URL, "relay", "resume")
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stdout=%q", code, stdout)
	}
	if !strings.Contains(stdout, "RUNNING") {
		t.Errorf("stdout = %q, missing the state", stdout)
	}
}

func TestBucketStatusPairs_omitsOwnerLeaseUntilAndLagAgeSecondsWhenNil(t *testing.T) {
	pairs := bucketStatusPairs(appWithColor(false), client.BucketStatus{
		Bucket: 1, Covered: false, Paused: false, PendingCount: 0,
	})
	for _, key := range []string{"owner", "leaseUntil", "lagAgeSeconds"} {
		for _, p := range pairs {
			if p[0] == key {
				t.Errorf("pairs contain %q = %q, want it omitted when nil", key, p[1])
			}
		}
	}
}

func TestBucketStatusPairs_includesOwnerLeaseUntilAndLagAgeSecondsWhenSet(t *testing.T) {
	owner := "worker-1"
	leaseUntil := mustParseTime(t, "2026-08-05T00:05:00Z")
	lag := float32(1.2)
	pairs := bucketStatusPairs(appWithColor(false), client.BucketStatus{
		Bucket: 0, Covered: true, Paused: false, PendingCount: 3,
		Owner: &owner, LeaseUntil: &leaseUntil, LagAgeSeconds: &lag,
	})
	want := map[string]string{
		"owner":         "worker-1",
		"leaseUntil":    "2026-08-05T00:05:00Z",
		"lagAgeSeconds": "1.2",
	}
	for key, wantValue := range want {
		found := false
		for _, p := range pairs {
			if p[0] == key {
				found = true
				if p[1] != wantValue {
					t.Errorf("pairs[%q] = %q, want %q", key, p[1], wantValue)
				}
			}
		}
		if !found {
			t.Errorf("pairs missing %q", key)
		}
	}
}

func mustParseTime(t *testing.T, s string) time.Time {
	t.Helper()
	tm, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatalf("parsing %q: %v", s, err)
	}
	return tm
}

func TestRelayBuckets_listRendersATable(t *testing.T) {
	server := newFixtureServer(t, map[string]route{
		"GET /relay/buckets": {200, "relay_buckets.json"},
	})
	stdout, _, code := execute(t, server.URL, "relay", "buckets")
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stdout=%q", code, stdout)
	}
	if !strings.Contains(stdout, "worker-1") {
		t.Errorf("stdout = %q, missing the owned bucket's owner", stdout)
	}
	lines := strings.Split(strings.TrimRight(stdout, "\n"), "\n")
	if len(lines) != 3 { // header + 2 buckets
		t.Errorf("got %d lines, want 3 (header + 2 rows): %q", len(lines), stdout)
	}
}

func TestRelayBuckets_singleBucketRendersKeyValue(t *testing.T) {
	server := newFixtureServer(t, map[string]route{
		"GET /relay/buckets/0": {200, "relay_bucket.json"},
	})
	stdout, _, code := execute(t, server.URL, "relay", "buckets", "0")
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stdout=%q", code, stdout)
	}
	if !strings.Contains(stdout, "worker-1") {
		t.Errorf("stdout = %q, missing the owner", stdout)
	}
}

func TestRelayBuckets_singleBucketApiErrorMapsToItsExitCode(t *testing.T) {
	server := newFixtureServer(t, map[string]route{
		"GET /relay/buckets/0": {404, "problem_not_found.json"},
	})
	_, stderr, code := execute(t, server.URL, "relay", "buckets", "0")
	if code != 4 {
		t.Errorf("exit code = %d, want 4 (NotFound); stderr=%q", code, stderr)
	}
}

func TestRelayBuckets_singleBucketMalformedResponseIsAnUnexpectedError(t *testing.T) {
	server := newFixtureServer(t, map[string]route{
		"GET /relay/buckets/0": {200, "malformed.json"},
	})
	_, stderr, code := execute(t, server.URL, "relay", "buckets", "0")
	if code != 1 {
		t.Errorf("exit code = %d, want 1 (UnexpectedError); stderr=%q", code, stderr)
	}
}

func TestRelayBuckets_listApiErrorMapsToItsExitCode(t *testing.T) {
	server := newFixtureServer(t, map[string]route{
		"GET /relay/buckets": {500, "problem_internal_error.json"},
	})
	_, stderr, code := execute(t, server.URL, "relay", "buckets")
	if code != 1 {
		t.Errorf("exit code = %d, want 1 (UnexpectedError); stderr=%q", code, stderr)
	}
}

func TestRelayBuckets_listMalformedResponseIsAnUnexpectedError(t *testing.T) {
	server := newFixtureServer(t, map[string]route{
		"GET /relay/buckets": {200, "malformed.json"},
	})
	_, stderr, code := execute(t, server.URL, "relay", "buckets")
	if code != 1 {
		t.Errorf("exit code = %d, want 1 (UnexpectedError); stderr=%q", code, stderr)
	}
}

func TestRelayBuckets_uncoveredOnlyWithABucketArgumentIsAUsageError(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.WriteHeader(500)
	}))
	defer server.Close()

	_, _, code := execute(t, server.URL, "relay", "buckets", "0", "--uncovered-only")
	if code != 2 {
		t.Errorf("exit code = %d, want 2 (UsageError)", code)
	}
	if requests != 0 {
		t.Errorf("server received %d requests, want 0", requests)
	}
}

func TestRelayBuckets_uncoveredOnlyIsForwardedAsAQueryParam(t *testing.T) {
	var gotQuery string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`[]`))
	}))
	defer server.Close()

	_, _, code := execute(t, server.URL, "relay", "buckets", "--uncovered-only")
	if code != 0 {
		t.Fatalf("exit code = %d, want 0", code)
	}
	if !strings.Contains(gotQuery, "uncoveredOnly=true") {
		t.Errorf("query = %q, want uncoveredOnly=true", gotQuery)
	}
}

func TestRelayReleaseBucket_rendersTheReleasedBucket(t *testing.T) {
	server := newFixtureServer(t, map[string]route{
		"POST /relay/buckets/0/release": {200, "relay_bucket.json"},
	})
	stdout, _, code := execute(t, server.URL, "relay", "release-bucket", "0")
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stdout=%q", code, stdout)
	}
	if !strings.Contains(stdout, "bucket:") || !strings.Contains(stdout, "0") {
		t.Errorf("stdout = %q, missing the bucket number", stdout)
	}
}

func TestRelayReleaseBucket_apiErrorMapsToItsExitCode(t *testing.T) {
	server := newFixtureServer(t, map[string]route{
		"POST /relay/buckets/0/release": {404, "problem_not_found.json"},
	})
	_, stderr, code := execute(t, server.URL, "relay", "release-bucket", "0")
	if code != 4 {
		t.Errorf("exit code = %d, want 4 (NotFound); stderr=%q", code, stderr)
	}
}

func TestRelayReleaseBucket_malformedResponseIsAnUnexpectedError(t *testing.T) {
	server := newFixtureServer(t, map[string]route{
		"POST /relay/buckets/0/release": {200, "malformed.json"},
	})
	_, stderr, code := execute(t, server.URL, "relay", "release-bucket", "0")
	if code != 1 {
		t.Errorf("exit code = %d, want 1 (UnexpectedError); stderr=%q", code, stderr)
	}
}

func TestRelayReleaseBucket_invalidBucketIsAUsageError(t *testing.T) {
	_, _, code := executeNoServer(t, "--base-url", "http://unused.invalid", "relay", "release-bucket", "not-a-number")
	if code != 2 {
		t.Errorf("exit code = %d, want 2 (UsageError)", code)
	}
}

func TestRelayWorkers_rendersATableOfWorkers(t *testing.T) {
	server := newFixtureServer(t, map[string]route{
		"GET /relay/workers": {200, "relay_workers.json"},
	})
	stdout, _, code := execute(t, server.URL, "relay", "workers")
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stdout=%q", code, stdout)
	}
	if !strings.Contains(stdout, "worker-1") || !strings.Contains(stdout, "worker-2") {
		t.Errorf("stdout = %q, missing both workers", stdout)
	}
}

func TestRelayBuckets_jsonIsRawPassthrough(t *testing.T) {
	server := newFixtureServer(t, map[string]route{
		"GET /relay/buckets": {200, "relay_buckets.json"},
	})
	stdout, _, code := execute(t, server.URL, "relay", "buckets", "--output", "json")
	if code != 0 {
		t.Fatalf("exit code = %d, want 0", code)
	}
	// A list response must stay a JSON array, not be reshaped into an envelope the
	// contract does not have (LLD-cli.md §6).
	var got []map[string]any
	if err := json.Unmarshal([]byte(stdout), &got); err != nil {
		t.Fatalf("stdout is not a JSON array: %v (%q)", err, stdout)
	}
	if len(got) == 0 {
		t.Errorf("stdout = %q, want the fixture's buckets passed through", stdout)
	}
}

func TestRelayWorkers_malformedResponseIsAnUnexpectedError(t *testing.T) {
	server := newFixtureServer(t, map[string]route{
		"GET /relay/workers": {200, "malformed.json"},
	})
	_, stderr, code := execute(t, server.URL, "relay", "workers")
	if code != 1 {
		t.Errorf("exit code = %d, want 1 (UnexpectedError); stderr=%q", code, stderr)
	}
}

func TestRelayWorkers_coordinationUnsupportedUnderSingleMapsToExitCode6(t *testing.T) {
	server := newFixtureServer(t, map[string]route{
		"GET /relay/workers": {409, "problem_coordination_unsupported.json"},
	})
	_, stderr, code := execute(t, server.URL, "relay", "workers")
	if code != 6 {
		t.Errorf("exit code = %d, want 6 (Conflict); stderr=%q", code, stderr)
	}
}
