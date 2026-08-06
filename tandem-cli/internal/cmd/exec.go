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
// non-2xx response is mapped off its RFC 9457 problem body via exitcode.ForProblem.
func do(resp *http.Response, err error) ([]byte, error) {
	if err != nil {
		return nil, exitcode.Wrap(exitcode.ConnectionFailure, err, "connecting to the Admin API")
	}
	defer resp.Body.Close()

	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, exitcode.Wrap(exitcode.UnexpectedError, readErr, "reading response body")
	}

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return body, nil
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

	msg := problem.Title
	if msg == "" {
		msg = fmt.Sprintf("HTTP %d", resp.StatusCode)
	}
	if problem.Detail != "" {
		msg = fmt.Sprintf("%s: %s", msg, problem.Detail)
	}
	return body, exitcode.New(exitcode.ForProblem(resp.StatusCode, problem.Type), "%s", msg)
}

// writeErr turns a plain write failure (from output.Raw/KeyValue/Table) into a
// classified error - nil stays nil.
func writeErr(err error) error {
	if err == nil {
		return nil
	}
	return exitcode.Wrap(exitcode.UnexpectedError, err, "writing output")
}

// confirm implements the --yes/TTY gate shared by discard and non-dry-run replay-bulk
// (LLD-cli.md §5): --yes always proceeds; otherwise it prompts on a terminal, and refuses
// rather than blocking or silently proceeding when there is nobody to ask.
func confirm(app *App, prompt string) error {
	if app.Yes {
		return nil
	}
	return promptYesNo(app, prompt, fmt.Sprintf(
		"%s (refusing without --yes: not attached to a terminal)", prompt))
}

// promptYesNo asks prompt and reads y/N from app's input, failing with refusal when there
// is no terminal to ask on. Split from confirm so replay-bulk can run its dry-run preview
// between the --yes check and the question, and word its own refusal message.
func promptYesNo(app *App, prompt, refusal string) error {
	if !app.IO.CanPrompt() {
		return exitcode.New(exitcode.ConfirmationRequired, "%s", refusal)
	}
	_, _ = fmt.Fprintf(app.IO.Err, "%s [y/N] ", prompt)
	line, _ := bufio.NewReader(app.IO.In).ReadString('\n')
	if !isAffirmative(line) {
		return exitcode.New(exitcode.ConfirmationRequired, "aborted: confirmation declined")
	}
	return nil
}

// isAffirmative reports whether a line read in response to a y/N prompt counts as yes -
// case-insensitive, and tolerant of the surrounding whitespace/newline
// bufio.Reader.ReadString('\n') leaves in place.
func isAffirmative(line string) bool {
	line = strings.TrimSpace(strings.ToLower(line))
	return line == "y" || line == "yes"
}

// isTerminalWriter reports whether w is a terminal: only an *os.File can be, so anything
// else (a test's buffer, a pipe wrapper) is not, without needing to probe it.
func isTerminalWriter(w io.Writer) bool {
	f, ok := w.(*os.File)
	return ok && isTerminal(f)
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
