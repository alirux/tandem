// Package exitcode is the CLI's exit-code contract (LLD-cli.md §7): every failure mode
// gets a distinct, documented non-zero code, keyed off the RFC 9457 ProblemDetail.type
// slug rather than the raw HTTP status alone, since two problems can share a status.
package exitcode

import (
	"fmt"
	"net/http"
	"strings"
)

const (
	// Success means the operation completed; 2xx from the API, or a read that found nothing.
	Success = 0
	// UnexpectedError is internal-error (500), or any response the client can't otherwise classify.
	UnexpectedError = 1
	// UsageError is bad flags/args - cobra's own usage-error path, a missing base URL,
	// discard without --reason, or --uncovered-only alongside a bucket argument. Never a
	// missing credential: none is required.
	UsageError = 2
	// Unauthorized is unauthorized (401).
	Unauthorized = 3
	// NotFound is not-found (404).
	NotFound = 4
	// InvalidParameter is invalid-parameter (400).
	InvalidParameter = 5
	// Conflict is message-not-replayable, message-not-discardable, or
	// relay-coordination-unsupported (409).
	Conflict = 6
	// ConfirmationRequired is ordering-break-not-acknowledged (400), or a local --yes/TTY
	// gate that stopped the CLI before any HTTP call.
	ConfirmationRequired = 7
	// ConnectionFailure means --base-url could not be reached at all - DNS, TCP, TLS
	// verification, or the --timeout elapsing - so there is no HTTP response to classify.
	ConnectionFailure = 8
)

// Error carries the exit code a command should terminate with, alongside the message to
// print on stderr. Every RunE in internal/cmd returns either nil or *Error - never a bare
// error - so main can read the code straight off it; see CodeOf.
type Error struct {
	Code    int
	Message string
	Err     error
}

func (e *Error) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Err)
	}
	return e.Message
}

func (e *Error) Unwrap() error { return e.Err }

// New builds an *Error carrying code, with a formatted message and no wrapped cause.
func New(code int, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

// Wrap builds an *Error carrying code around an existing error, for a failure the CLI
// classifies (e.g. a connection error) rather than constructs its own message for.
func Wrap(code int, err error, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...), Err: err}
}

// CodeOf returns the process exit code for err: 0 for nil, an *Error's own Code, and
// UsageError for anything else - every other error reaching main is one cobra raised
// itself during flag/argument parsing, before any RunE ran.
func CodeOf(err error) int {
	if err == nil {
		return Success
	}
	if e, ok := err.(*Error); ok {
		return e.Code
	}
	return UsageError
}

// slugPrefix is the fixed, canonical problem-type namespace every Tandem RFC 9457
// response uses (AGENTS.md, HLD-admin-api.md §3) - never about:blank or a bare slug.
const slugPrefix = "https://tandem.codingful.com/problems/"

// Slug extracts the stable kebab-case identifier from a canonical
// https://tandem.codingful.com/problems/{slug} problem-type URI. Anything not matching
// that shape - a future scheme change, or a non-Tandem proxy's own error page - returns
// the input unchanged, so ForProblem's switch simply falls through to its status-based
// fallback rather than panicking on an unexpected shape.
func Slug(problemType string) string {
	return strings.TrimPrefix(problemType, slugPrefix)
}

// ForProblem maps an HTTP status and RFC 9457 problem-type slug to the exit code table
// in LLD-cli.md §7. A slug this build doesn't recognize - the contract evolves
// additively, so a newer server can return one an older CLI predates - falls back to the
// HTTP status for the handful of statuses that mean exactly one thing in the table;
// status 400 alone is ambiguous (InvalidParameter vs ConfirmationRequired), so an
// unrecognized 400 lands on UnexpectedError rather than guessing which.
func ForProblem(status int, problemType string) int {
	switch Slug(problemType) {
	case "unauthorized":
		return Unauthorized
	case "not-found":
		return NotFound
	case "invalid-parameter":
		return InvalidParameter
	case "message-not-replayable", "message-not-discardable", "relay-coordination-unsupported":
		return Conflict
	case "ordering-break-not-acknowledged":
		return ConfirmationRequired
	case "internal-error":
		return UnexpectedError
	}
	switch status {
	case http.StatusUnauthorized:
		return Unauthorized
	case http.StatusNotFound:
		return NotFound
	case http.StatusConflict:
		return Conflict
	default:
		return UnexpectedError
	}
}
