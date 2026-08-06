package exitcode

import (
	"errors"
	"net/http"
	"testing"
)

func TestForProblem_knownSlugsMapToTheirDocumentedCode(t *testing.T) {
	cases := []struct {
		status int
		slug   string
		want   int
	}{
		{http.StatusUnauthorized, "unauthorized", Unauthorized},
		{http.StatusNotFound, "not-found", NotFound},
		{http.StatusBadRequest, "invalid-parameter", InvalidParameter},
		{http.StatusConflict, "message-not-replayable", Conflict},
		{http.StatusConflict, "message-not-discardable", Conflict},
		{http.StatusConflict, "relay-coordination-unsupported", Conflict},
		{http.StatusBadRequest, "ordering-break-not-acknowledged", ConfirmationRequired},
		{http.StatusInternalServerError, "internal-error", UnexpectedError},
	}
	for _, c := range cases {
		got := ForProblem(c.status, slugPrefix+c.slug)
		if got != c.want {
			t.Errorf("ForProblem(%d, %q) = %d, want %d", c.status, c.slug, got, c.want)
		}
	}
}

func TestForProblem_unrecognizedSlugFallsBackToStatusWhereUnambiguous(t *testing.T) {
	cases := []struct {
		status int
		want   int
	}{
		{http.StatusUnauthorized, Unauthorized},
		{http.StatusNotFound, NotFound},
		{http.StatusConflict, Conflict},
	}
	for _, c := range cases {
		got := ForProblem(c.status, slugPrefix+"a-future-additive-slug")
		if got != c.want {
			t.Errorf("ForProblem(%d, future-slug) = %d, want %d", c.status, got, c.want)
		}
	}
}

func TestForProblem_ambiguousStatusFallsBackToUnexpectedRatherThanGuessing(t *testing.T) {
	// 400 covers both InvalidParameter and ConfirmationRequired in the known table, so an
	// unrecognized slug at 400 must not silently pick one.
	got := ForProblem(http.StatusBadRequest, slugPrefix+"a-future-additive-slug")
	if got != UnexpectedError {
		t.Errorf("ForProblem(400, future-slug) = %d, want %d", got, UnexpectedError)
	}
}

func TestForProblem_nonCanonicalTypeStillFallsBackToStatus(t *testing.T) {
	// Not a https://tandem.codingful.com/problems/{slug} shape at all - a misbehaving
	// proxy in front of the Admin API, say - Slug returns it unchanged and no case matches.
	got := ForProblem(http.StatusNotFound, "about:blank")
	if got != NotFound {
		t.Errorf("ForProblem(404, about:blank) = %d, want %d", got, NotFound)
	}
}

func TestSlug_stripsTheCanonicalPrefix(t *testing.T) {
	got := Slug("https://tandem.codingful.com/problems/not-found")
	if got != "not-found" {
		t.Errorf("Slug(...) = %q, want %q", got, "not-found")
	}
}

func TestCodeOf_nilErrorIsSuccess(t *testing.T) {
	if got := CodeOf(nil); got != Success {
		t.Errorf("CodeOf(nil) = %d, want %d", got, Success)
	}
}

func TestCodeOf_typedErrorReturnsItsOwnCode(t *testing.T) {
	err := New(ConnectionFailure, "could not reach %s", "https://example.invalid")
	if got := CodeOf(err); got != ConnectionFailure {
		t.Errorf("CodeOf(typed) = %d, want %d", got, ConnectionFailure)
	}
}

func TestCodeOf_bareErrorIsTreatedAsAUsageError(t *testing.T) {
	// Every RunE in internal/cmd is expected to only ever return nil or *Error; a bare
	// error reaching main can only have come from cobra's own flag/argument parsing.
	if got := CodeOf(errors.New("unknown flag: --frobnicate")); got != UsageError {
		t.Errorf("CodeOf(bare) = %d, want %d", got, UsageError)
	}
}

func TestError_messageAloneWhenNoUnderlyingErrorIsWrapped(t *testing.T) {
	err := New(UsageError, "missing base URL: pass --base-url or set %s", "TANDEM_ADMIN_URL")
	want := "missing base URL: pass --base-url or set TANDEM_ADMIN_URL"
	if got := err.Error(); got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
}

func TestError_appendsTheUnderlyingErrorWhenWrapped(t *testing.T) {
	cause := errors.New("dial tcp: connection refused")
	err := Wrap(ConnectionFailure, cause, "connecting to %s", "https://example.invalid")
	want := "connecting to https://example.invalid: dial tcp: connection refused"
	if got := err.Error(); got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
}

func TestWrap_preservesTheUnderlyingErrorForUnwrapping(t *testing.T) {
	cause := errors.New("dial tcp: connection refused")
	wrapped := Wrap(ConnectionFailure, cause, "connecting to %s", "https://example.invalid")

	if !errors.Is(wrapped, cause) {
		t.Errorf("errors.Is(wrapped, cause) = false, want true")
	}
	if wrapped.Code != ConnectionFailure {
		t.Errorf("wrapped.Code = %d, want %d", wrapped.Code, ConnectionFailure)
	}
}
