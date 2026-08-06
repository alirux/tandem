package cmd

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/alirux/tandem/tandem-cli/internal/exitcode"
)

// do reads an Admin API call's result - the (*http.Response, error) pair every generated
// client method returns - and classifies it per LLD-cli.md §7. A transport-level failure
// (no response at all: DNS, TCP, TLS, or --timeout elapsing) is ConnectionFailure; a
// non-2xx response is mapped off its RFC 9457 problem body via exitcode.ForProblem. On
// success, body is the raw response bytes and cerr is nil.
func do(resp *http.Response, err error) (body []byte, status int, cerr *exitcode.Error) {
	if err != nil {
		return nil, 0, exitcode.Wrap(exitcode.ConnectionFailure, err, "connecting to the Admin API")
	}
	defer resp.Body.Close()

	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, resp.StatusCode, exitcode.Wrap(exitcode.UnexpectedError, readErr, "reading response body")
	}

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return body, resp.StatusCode, nil
	}

	// Best-effort RFC 9457 parse: a body that isn't a problem+json document (a proxy's
	// own HTML error page, say) still gets classified by status alone - see
	// exitcode.ForProblem's fallback.
	var problem struct {
		Type   string `json:"type"`
		Title  string `json:"title"`
		Detail string `json:"detail"`
	}
	_ = json.Unmarshal(body, &problem)

	code := exitcode.ForProblem(resp.StatusCode, problem.Type)
	msg := problem.Title
	if msg == "" {
		msg = fmt.Sprintf("HTTP %d", resp.StatusCode)
	}
	if problem.Detail != "" {
		msg = fmt.Sprintf("%s: %s", msg, problem.Detail)
	}
	return body, resp.StatusCode, exitcode.New(code, "%s", msg)
}

// writeErr turns a plain write failure (from output.Raw/KeyValue/Table) into the error
// type every RunE must return - nil stays nil, so this never introduces the typed-nil
// interface trap (a non-nil error interface wrapping a nil *exitcode.Error).
func writeErr(err error) error {
	if err == nil {
		return nil
	}
	return exitcode.Wrap(exitcode.UnexpectedError, err, "writing output")
}

// confirm implements the --yes/TTY gate shared by discard and non-dry-run replay-bulk
// (LLD-cli.md §5): --yes always proceeds; otherwise, attached to a TTY it prompts and
// reads y/N from stdin; piped or non-interactive, it refuses rather than blocking or
// silently proceeding. prompt is printed before the y/N read.
func confirm(app *App, yes bool, prompt string) *exitcode.Error {
	if yes {
		return nil
	}
	if !isTerminal(os.Stdout) {
		return exitcode.New(exitcode.ConfirmationRequired,
			"%s (refusing without --yes: not attached to a terminal)", prompt)
	}
	fmt.Fprintf(app.Stderr, "%s [y/N] ", prompt)
	reader := bufio.NewReader(os.Stdin)
	line, _ := reader.ReadString('\n')
	if !isAffirmative(line) {
		return exitcode.New(exitcode.ConfirmationRequired, "aborted: confirmation declined")
	}
	return nil
}

// isAffirmative reports whether a line read in response to confirm's y/N prompt counts as
// yes - case-insensitive, and tolerant of the surrounding whitespace/newline
// bufio.Reader.ReadString('\n') leaves in place.
func isAffirmative(line string) bool {
	line = strings.TrimSpace(strings.ToLower(line))
	return line == "y" || line == "yes"
}

// isTerminal reports whether f is attached to an interactive terminal, matching the
// backlog's "no interactive prompt unless attached to a TTY" (LLD-cli.md §5). Deliberately
// stdlib-only (a character-device check), so a TTY library is not added to the binary's
// third-party surface just for this.
func isTerminal(f *os.File) bool {
	fi, err := f.Stat()
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeCharDevice != 0
}
