package cmd

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestOutboxSummary_humanRendersCountsAndLag(t *testing.T) {
	server := newFixtureServer(t, map[string]route{
		"GET /outbox/summary": {200, "outbox_summary.json"},
	})
	stdout, _, code := execute(t, server.URL, "outbox", "summary")
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stdout=%q", code, stdout)
	}
	for _, want := range []string{"pending:", "3", "failed:", "1", "lagAgeSeconds:", "12.5"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("stdout = %q, missing %q", stdout, want)
		}
	}
}

func TestOutboxSummary_jsonIsRawPassthrough(t *testing.T) {
	server := newFixtureServer(t, map[string]route{
		"GET /outbox/summary": {200, "outbox_summary.json"},
	})
	stdout, _, code := execute(t, server.URL, "outbox", "summary", "--output", "json")
	if code != 0 {
		t.Fatalf("exit code = %d, want 0", code)
	}
	var got map[string]any
	if err := json.Unmarshal([]byte(stdout), &got); err != nil {
		t.Fatalf("stdout is not valid JSON: %v (%q)", err, stdout)
	}
	if got["lagCount"] != float64(3) {
		t.Errorf("lagCount = %v, want 3", got["lagCount"])
	}
}

func TestOutboxSummary_apiErrorMapsToItsExitCode(t *testing.T) {
	server := newFixtureServer(t, map[string]route{
		"GET /outbox/summary": {500, "problem_internal_error.json"},
	})
	_, stderr, code := execute(t, server.URL, "outbox", "summary")
	if code != 1 {
		t.Errorf("exit code = %d, want 1 (UnexpectedError); stderr=%q", code, stderr)
	}
}

func TestOutboxSummary_malformedResponseIsAnUnexpectedError(t *testing.T) {
	server := newFixtureServer(t, map[string]route{
		"GET /outbox/summary": {200, "malformed.json"},
	})
	_, stderr, code := execute(t, server.URL, "outbox", "summary")
	if code != 1 {
		t.Errorf("exit code = %d, want 1 (UnexpectedError); stderr=%q", code, stderr)
	}
}

func TestOutboxSearch_humanRendersATableAndCursorHint(t *testing.T) {
	server := newFixtureServer(t, map[string]route{
		"GET /outbox/messages": {200, "outbox_search_page.json"},
	})
	stdout, _, code := execute(t, server.URL, "outbox", "search")
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stdout=%q", code, stdout)
	}
	if !strings.Contains(stdout, "order-1") || !strings.Contains(stdout, "order-2") {
		t.Errorf("stdout = %q, missing both rows", stdout)
	}
	if !strings.Contains(stdout, "next page: --cursor=2") {
		t.Errorf("stdout = %q, missing the cursor hint", stdout)
	}
}

func TestOutboxSearch_noCursorHintWhenNextCursorIsNull(t *testing.T) {
	server := newFixtureServer(t, map[string]route{
		"GET /outbox/messages": {200, "outbox_search_page_no_next.json"},
	})
	stdout, _, code := execute(t, server.URL, "outbox", "search")
	if code != 0 {
		t.Fatalf("exit code = %d, want 0", code)
	}
	if strings.Contains(stdout, "next page:") {
		t.Errorf("stdout = %q, should not print a cursor hint", stdout)
	}
}

func TestOutboxSearch_apiErrorMapsToItsExitCode(t *testing.T) {
	server := newFixtureServer(t, map[string]route{
		"GET /outbox/messages": {500, "problem_internal_error.json"},
	})
	_, stderr, code := execute(t, server.URL, "outbox", "search")
	if code != 1 {
		t.Errorf("exit code = %d, want 1 (UnexpectedError); stderr=%q", code, stderr)
	}
}

func TestOutboxSearch_createdFromAndCreatedToAreForwardedAsRFC3339(t *testing.T) {
	var gotQuery string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"items":[]}`))
	}))
	defer server.Close()

	_, _, code := execute(t, server.URL, "outbox", "search",
		"--created-from", "2026-08-01T00:00:00Z", "--created-to", "2026-08-05T00:00:00Z")
	if code != 0 {
		t.Fatalf("exit code = %d, want 0", code)
	}
	if !strings.Contains(gotQuery, "createdFrom=2026-08-01") {
		t.Errorf("query = %q, missing createdFrom", gotQuery)
	}
	if !strings.Contains(gotQuery, "createdTo=2026-08-05") {
		t.Errorf("query = %q, missing createdTo", gotQuery)
	}
}

func TestOutboxSearch_invalidCreatedFromIsAUsageErrorBeforeAnyHTTPCall(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.WriteHeader(500)
	}))
	defer server.Close()

	_, stderr, code := execute(t, server.URL, "outbox", "search", "--created-from", "not-a-date")
	if code != 2 {
		t.Errorf("exit code = %d, want 2 (UsageError); stderr=%q", code, stderr)
	}
	if requests != 0 {
		t.Errorf("server received %d requests, want 0 - a usage error must not reach the network", requests)
	}
}

func TestOutboxSearch_invalidCreatedToIsAUsageErrorBeforeAnyHTTPCall(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.WriteHeader(500)
	}))
	defer server.Close()

	_, stderr, code := execute(t, server.URL, "outbox", "search", "--created-to", "not-a-date")
	if code != 2 {
		t.Errorf("exit code = %d, want 2 (UsageError); stderr=%q", code, stderr)
	}
	if requests != 0 {
		t.Errorf("server received %d requests, want 0 - a usage error must not reach the network", requests)
	}
}

func TestOutboxGet_humanRendersPayloadAndHeaders(t *testing.T) {
	server := newFixtureServer(t, map[string]route{
		"GET /outbox/messages/1": {200, "outbox_entry.json"},
	})
	stdout, _, code := execute(t, server.URL, "outbox", "get", "1")
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stdout=%q", code, stdout)
	}
	if !strings.Contains(stdout, `"amount":42`) {
		t.Errorf("stdout = %q, missing the payload", stdout)
	}
}

func TestOutboxGet_rendersTypeLastErrorDiscardReasonAndLockedByWhenPresent(t *testing.T) {
	server := newFixtureServer(t, map[string]route{
		"GET /outbox/messages/2": {200, "outbox_entry_full.json"},
	})
	stdout, _, code := execute(t, server.URL, "outbox", "get", "2")
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stdout=%q", code, stdout)
	}
	for _, want := range []string{
		"type:", "OrderPlaced",
		"lastError:", "connection refused",
		"discardReason:", "poison message",
		"lockedBy:", "worker-3",
	} {
		if !strings.Contains(stdout, want) {
			t.Errorf("stdout = %q, missing %q", stdout, want)
		}
	}
}

func TestOutboxGet_malformedResponseIsAnUnexpectedError(t *testing.T) {
	server := newFixtureServer(t, map[string]route{
		"GET /outbox/messages/1": {200, "malformed.json"},
	})
	_, stderr, code := execute(t, server.URL, "outbox", "get", "1")
	if code != 1 {
		t.Errorf("exit code = %d, want 1 (UnexpectedError); stderr=%q", code, stderr)
	}
}

func TestOutboxGet_notFoundMapsToExitCode4(t *testing.T) {
	server := newFixtureServer(t, map[string]route{
		"GET /outbox/messages/42": {404, "problem_not_found.json"},
	})
	_, stderr, code := execute(t, server.URL, "outbox", "get", "42")
	if code != 4 {
		t.Errorf("exit code = %d, want 4 (NotFound); stderr=%q", code, stderr)
	}
}

func TestOutboxGet_invalidIDIsAUsageError(t *testing.T) {
	_, _, code := executeNoServer(t, "--base-url", "http://unused.invalid", "outbox", "get", "not-a-number")
	if code != 2 {
		t.Errorf("exit code = %d, want 2 (UsageError)", code)
	}
}

func TestOutboxReplay_humanRendersTheUpdatedMessage(t *testing.T) {
	server := newFixtureServer(t, map[string]route{
		"POST /outbox/messages/1/replay": {200, "outbox_entry.json"},
	})
	stdout, _, code := execute(t, server.URL, "outbox", "replay", "1")
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stdout=%q", code, stdout)
	}
	if !strings.Contains(stdout, "order-1") {
		t.Errorf("stdout = %q, missing the replayed message", stdout)
	}
}

func TestOutboxReplay_apiErrorMapsToItsExitCode(t *testing.T) {
	server := newFixtureServer(t, map[string]route{
		"POST /outbox/messages/1/replay": {404, "problem_not_found.json"},
	})
	_, stderr, code := execute(t, server.URL, "outbox", "replay", "1")
	if code != 4 {
		t.Errorf("exit code = %d, want 4 (NotFound); stderr=%q", code, stderr)
	}
}

func TestOutboxReplay_malformedResponseIsAnUnexpectedError(t *testing.T) {
	server := newFixtureServer(t, map[string]route{
		"POST /outbox/messages/1/replay": {200, "malformed.json"},
	})
	_, stderr, code := execute(t, server.URL, "outbox", "replay", "1")
	if code != 1 {
		t.Errorf("exit code = %d, want 1 (UnexpectedError); stderr=%q", code, stderr)
	}
}

func TestOutboxDiscard_invalidIDIsAUsageErrorBeforeAnyHTTPCall(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.WriteHeader(500)
	}))
	defer server.Close()

	_, _, code := execute(t, server.URL, "outbox", "discard", "not-a-number", "--yes", "--reason", "x")
	if code != 2 {
		t.Errorf("exit code = %d, want 2 (UsageError)", code)
	}
	if requests != 0 {
		t.Errorf("server received %d requests, want 0 - a usage error must not reach the network", requests)
	}
}

func TestOutboxDiscard_malformedResponseIsAnUnexpectedError(t *testing.T) {
	server := newFixtureServer(t, map[string]route{
		"POST /outbox/messages/1/discard": {200, "malformed.json"},
	})
	_, stderr, code := execute(t, server.URL, "outbox", "discard", "1", "--yes", "--reason", "x")
	if code != 1 {
		t.Errorf("exit code = %d, want 1 (UnexpectedError); stderr=%q", code, stderr)
	}
}

func TestOutboxDiscard_missingReasonIsAUsageErrorBeforeAnyHTTPCall(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.WriteHeader(500)
	}))
	defer server.Close()

	_, _, code := execute(t, server.URL, "outbox", "discard", "1", "--yes")
	if code != 2 {
		t.Errorf("exit code = %d, want 2 (UsageError)", code)
	}
	if requests != 0 {
		t.Errorf("server received %d requests, want 0 - a usage error must not reach the network", requests)
	}
}

func TestOutboxDiscard_withoutYesOnANonTTYIsConfirmationRequired(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.WriteHeader(500)
	}))
	defer server.Close()

	_, stderr, code := execute(t, server.URL, "outbox", "discard", "1", "--reason", "bad payload")
	if code != 7 {
		t.Errorf("exit code = %d, want 7 (ConfirmationRequired); stderr=%q", code, stderr)
	}
	if requests != 0 {
		t.Errorf("server received %d requests, want 0 - must not call the API without confirmation", requests)
	}
}

func TestOutboxDiscard_sendsAcknowledgeOrderingBreakAndTheReason(t *testing.T) {
	var body map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&body)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"id":1,"aggregateId":"order-1","aggregateType":"Order","seq":1,"status":"DISCARDED","attempts":10,"createdAt":"2026-08-05T00:00:00Z"}`))
	}))
	defer server.Close()

	_, _, code := execute(t, server.URL, "outbox", "discard", "1", "--yes", "--reason", "poison message")
	if code != 0 {
		t.Fatalf("exit code = %d, want 0", code)
	}
	if body["acknowledgeOrderingBreak"] != true {
		t.Errorf("acknowledgeOrderingBreak = %v, want true", body["acknowledgeOrderingBreak"])
	}
	if body["reason"] != "poison message" {
		t.Errorf("reason = %v, want %q", body["reason"], "poison message")
	}
}

func TestOutboxDiscard_conflictMapsToExitCode6(t *testing.T) {
	server := newFixtureServer(t, map[string]route{
		"POST /outbox/messages/1/discard": {409, "problem_message_not_discardable.json"},
	})
	_, stderr, code := execute(t, server.URL, "outbox", "discard", "1", "--yes", "--reason", "x")
	if code != 6 {
		t.Errorf("exit code = %d, want 6 (Conflict); stderr=%q", code, stderr)
	}
}

func TestOutboxReplayBulk_dryRunNeedsNoConfirmation(t *testing.T) {
	requests := 0
	server := newFixtureServer(t, map[string]route{
		"POST /outbox/replay": {200, "replay_result_dry_run.json"},
	})
	server.Config.Handler = countingWrapper(&requests, server.Config.Handler)

	stdout, _, code := execute(t, server.URL, "outbox", "replay-bulk", "--aggregate-type", "Order", "--dry-run")
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stdout=%q", code, stdout)
	}
	if !strings.Contains(stdout, "matched:") || !strings.Contains(stdout, "5") {
		t.Errorf("stdout = %q, missing the matched count", stdout)
	}
	if requests != 1 {
		t.Errorf("server received %d requests, want exactly 1 (the dry run itself)", requests)
	}
}

func TestOutboxReplayBulk_dryRunApiErrorMapsToItsExitCode(t *testing.T) {
	server := newFixtureServer(t, map[string]route{
		"POST /outbox/replay": {500, "problem_internal_error.json"},
	})
	_, stderr, code := execute(t, server.URL, "outbox", "replay-bulk", "--aggregate-type", "Order", "--dry-run")
	if code != 1 {
		t.Errorf("exit code = %d, want 1 (UnexpectedError); stderr=%q", code, stderr)
	}
}

func TestOutboxReplayBulk_dryRunMalformedResponseIsAnUnexpectedError(t *testing.T) {
	server := newFixtureServer(t, map[string]route{
		"POST /outbox/replay": {200, "malformed.json"},
	})
	_, stderr, code := execute(t, server.URL, "outbox", "replay-bulk", "--aggregate-type", "Order", "--dry-run")
	if code != 1 {
		t.Errorf("exit code = %d, want 1 (UnexpectedError); stderr=%q", code, stderr)
	}
}

func TestOutboxReplayBulk_withYesApiErrorMapsToItsExitCode(t *testing.T) {
	server := newFixtureServer(t, map[string]route{
		"POST /outbox/replay": {500, "problem_internal_error.json"},
	})
	_, stderr, code := execute(t, server.URL, "outbox", "replay-bulk", "--aggregate-type", "Order", "--yes")
	if code != 1 {
		t.Errorf("exit code = %d, want 1 (UnexpectedError); stderr=%q", code, stderr)
	}
}

func TestOutboxReplayBulk_withYesMalformedResponseIsAnUnexpectedError(t *testing.T) {
	server := newFixtureServer(t, map[string]route{
		"POST /outbox/replay": {200, "malformed.json"},
	})
	_, stderr, code := execute(t, server.URL, "outbox", "replay-bulk", "--aggregate-type", "Order", "--yes")
	if code != 1 {
		t.Errorf("exit code = %d, want 1 (UnexpectedError); stderr=%q", code, stderr)
	}
}

func TestOutboxReplayBulk_everySelectorFlagIsForwardedInTheRequestBody(t *testing.T) {
	var body map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&body)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"matched":2,"replayed":2,"dryRun":false}`))
	}))
	defer server.Close()

	_, _, code := execute(t, server.URL, "outbox", "replay-bulk", "--yes",
		"--aggregate-id", "order-1", "--aggregate-type", "Order",
		"--from-id", "10", "--to-id", "20",
		"--status", "DONE", "--status", "FAILED")
	if code != 0 {
		t.Fatalf("exit code = %d, want 0", code)
	}
	if body["aggregateId"] != "order-1" {
		t.Errorf("aggregateId = %v, want order-1", body["aggregateId"])
	}
	if body["aggregateType"] != "Order" {
		t.Errorf("aggregateType = %v, want Order", body["aggregateType"])
	}
	if body["fromId"] != float64(10) {
		t.Errorf("fromId = %v, want 10", body["fromId"])
	}
	if body["toId"] != float64(20) {
		t.Errorf("toId = %v, want 20", body["toId"])
	}
	statuses, _ := body["statuses"].([]any)
	if len(statuses) != 2 || statuses[0] != "DONE" || statuses[1] != "FAILED" {
		t.Errorf("statuses = %v, want [DONE FAILED]", body["statuses"])
	}
}

func TestOutboxReplayBulk_withoutYesOnANonTTYIsConfirmationRequired(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.WriteHeader(500)
	}))
	defer server.Close()

	_, stderr, code := execute(t, server.URL, "outbox", "replay-bulk", "--aggregate-type", "Order")
	if code != 7 {
		t.Errorf("exit code = %d, want 7 (ConfirmationRequired); stderr=%q", code, stderr)
	}
	if requests != 0 {
		t.Errorf("server received %d requests, want 0 - must not call the API without confirmation or --dry-run", requests)
	}
}

func TestOutboxReplayBulk_withYesSkipsThePreviewAndReplays(t *testing.T) {
	var gotDryRun *bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			DryRun *bool `json:"dryRun"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		gotDryRun = req.DryRun
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"matched":5,"replayed":5,"dryRun":false}`))
	}))
	defer server.Close()

	stdout, _, code := execute(t, server.URL, "outbox", "replay-bulk", "--aggregate-type", "Order", "--yes")
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stdout=%q", code, stdout)
	}
	if gotDryRun == nil || *gotDryRun != false {
		t.Errorf("dryRun sent = %v, want false", gotDryRun)
	}
	if !strings.Contains(stdout, "replayed:") || !strings.Contains(stdout, "5") {
		t.Errorf("stdout = %q, missing the replayed count", stdout)
	}
}

func TestExecute_missingBaseURLIsAUsageError(t *testing.T) {
	_, stderr, code := executeNoServer(t, "outbox", "summary")
	if code != 2 {
		t.Errorf("exit code = %d, want 2 (UsageError); stderr=%q", code, stderr)
	}
}

// countingWrapper increments *n for every request before delegating to next - used where
// newFixtureServer's canned routing is enough for the response but a test still needs to
// assert exactly how many requests were made.
func countingWrapper(n *int, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*n++
		next.ServeHTTP(w, r)
	})
}
