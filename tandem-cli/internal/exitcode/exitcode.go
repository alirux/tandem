// Package exitcode is the CLI's exit-code contract (LLD-cli.md §7): every failure mode
// gets a distinct, documented non-zero code, keyed off the RFC 9457 ProblemDetail.type
// slug rather than the raw HTTP status alone, since two problems can share a status.
package exitcode

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
)

// Code is a process exit status. A named type, so a code can never be confused with any
// other integer a caller happens to have in hand.
type Code int

const (
	// Success means the operation completed; 2xx from the API, or a read that found nothing.
	Success Code = 0
	// UnexpectedError is internal-error (500), or any response the client can't otherwise classify.
	UnexpectedError Code = 1
	// UsageError is bad flags/args - cobra's own usage-error path, a missing base URL,
	// discard without --reason, or --uncovered-only alongside a bucket argument. Never a
	// missing credential: none is required.
	UsageError Code = 2
	// Unauthorized is unauthorized (401).
	Unauthorized Code = 3
	// NotFound is not-found (404).
	NotFound Code = 4
	// InvalidParameter is invalid-parameter (400).
	InvalidParameter Code = 5
	// Conflict is message-not-replayable, message-not-discardable, or
	// relay-coordination-unsupported (409).
	Conflict Code = 6
	// ConfirmationRequired is ordering-break-not-acknowledged (400), or a local --yes/TTY
	// gate that stopped the CLI before any HTTP call.
	ConfirmationRequired Code = 7
	// ConnectionFailure means --base-url could not be reached at all - DNS, TCP, TLS
	// verification, or the --timeout elapsing - so there is no HTTP response to classify.
	ConnectionFailure Code = 8
)

// Error carries the exit code a command should terminate with, alongside the message to
// print on stderr. Recovered from any error chain by CodeOf.
type Error struct {
	Code    Code
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

// New builds an error carrying code, with a formatted message and no wrapped cause.
// Returns the error interface, never the concrete *Error: a helper handing back a typed
// nil pointer is the classic way to produce a non-nil error that wraps nothing.
func New(code Code, format string, args ...any) error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

// Wrap builds an error carrying code around an existing cause, for a failure the CLI
// classifies (e.g. a connection error) rather than constructs its own message for.
func Wrap(code Code, err error, format string, args ...any) error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...), Err: err}
}

// CodeOf returns the process exit code for err: Success for nil, the Code of the first
// *Error in the chain, and UsageError for anything else - every other error reaching main
// is one cobra raised itself during flag/argument parsing, before any RunE ran. Matching
// through the whole chain (errors.As, not a bare type assertion) is what keeps a code
// intact when an *Error is later wrapped by a caller.
func CodeOf(err error) Code {
	if err == nil {
		return Success
	}
	var e *Error
	if errors.As(err, &e) {
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
func ForProblem(status int, problemType string) Code {
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
