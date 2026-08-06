package cmd

import (
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/alirux/tandem/tandem-cli/internal/exitcode"
)

func TestDo_transportErrorIsAConnectionFailure(t *testing.T) {
	_, err := do(nil, errors.New("dial tcp: connection refused"))
	if err == nil {
		t.Fatal("do(nil, err) = nil, want a ConnectionFailure error")
	}
	if got := exitcode.CodeOf(err); got != exitcode.ConnectionFailure {
		t.Errorf("CodeOf = %d, want ConnectionFailure (%d)", got, exitcode.ConnectionFailure)
	}
}

// errReader always fails to read, standing in for a response body that breaks mid-stream
// (a truncated connection) rather than one that simply isn't there at all.
type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("unexpected EOF") }

func TestDo_bodyReadFailureIsAnUnexpectedError(t *testing.T) {
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(errReader{}),
	}
	_, err := do(resp, nil)
	if err == nil {
		t.Fatal("do(resp, nil) = nil, want an UnexpectedError from the broken body")
	}
	if got := exitcode.CodeOf(err); got != exitcode.UnexpectedError {
		t.Errorf("CodeOf = %d, want UnexpectedError (%d)", got, exitcode.UnexpectedError)
	}
}

func TestDo_nonProblemErrorBodyFallsBackToAnHTTPStatusMessage(t *testing.T) {
	// A body that isn't RFC 9457 at all - a proxy's own HTML/text error page - still
	// leaves problem.Title empty after the best-effort json.Unmarshal silently fails, so
	// the message must fall back to "HTTP <status>" rather than printing an empty title.
	resp := &http.Response{
		StatusCode: http.StatusBadGateway,
		Body:       io.NopCloser(strings.NewReader("Bad Gateway")),
	}
	_, err := do(resp, nil)
	if err == nil {
		t.Fatal("do(502 non-JSON body) = nil, want an error")
	}
	if !strings.Contains(err.Error(), "HTTP 502") {
		t.Errorf("Error() = %q, want it to contain the HTTP-status fallback message", err.Error())
	}
}

func TestWriteErr_nilStaysNil(t *testing.T) {
	if err := writeErr(nil); err != nil {
		t.Errorf("writeErr(nil) = %v, want nil", err)
	}
}

func TestWriteErr_wrapsAWriteFailureAsUnexpectedError(t *testing.T) {
	cause := errors.New("broken pipe")
	err := writeErr(cause)
	if err == nil {
		t.Fatal("writeErr(cause) = nil, want a wrapped error")
	}
	if code := exitcode.CodeOf(err); code != exitcode.UnexpectedError {
		t.Errorf("CodeOf(writeErr(cause)) = %d, want UnexpectedError (%d)", code, exitcode.UnexpectedError)
	}
	if !errors.Is(err, cause) {
		t.Error("errors.Is(writeErr(cause), cause) = false, want true - the original error must stay unwrappable")
	}
}

func TestConfirm_yesFlagSkipsThePromptEntirely(t *testing.T) {
	// TTY: false and In: nil would fail this test if confirm ever tried to read - the
	// --yes flag must short-circuit before touching IO at all.
	app := &App{Yes: true, IO: IOStreams{TTY: false}}
	if err := confirm(app, "Discard message 1?"); err != nil {
		t.Errorf("confirm(Yes=true) = %v, want nil", err)
	}
}

func TestPromptYesNo_readsAnAffirmativeAnswerFromTheInjectedReader(t *testing.T) {
	var errOut strings.Builder
	app := &App{IO: IOStreams{TTY: true, In: strings.NewReader("y\n"), Err: &errOut}}

	if err := promptYesNo(app, "Proceed?", "refused"); err != nil {
		t.Errorf("promptYesNo(y) = %v, want nil", err)
	}
	if !strings.Contains(errOut.String(), "Proceed?") {
		t.Errorf("stderr = %q, want the prompt text written to it", errOut.String())
	}
}

func TestPromptYesNo_aNegativeAnswerIsConfirmationRequired(t *testing.T) {
	app := &App{IO: IOStreams{TTY: true, In: strings.NewReader("n\n"), Err: io.Discard}}

	err := promptYesNo(app, "Proceed?", "refused")
	if got := exitcode.CodeOf(err); got != exitcode.ConfirmationRequired {
		t.Errorf("CodeOf(promptYesNo(n)) = %d, want ConfirmationRequired (%d)", got, exitcode.ConfirmationRequired)
	}
}

func TestPromptYesNo_emptyInputIsConfirmationRequired(t *testing.T) {
	// io.EOF with no bytes read at all - e.g. stdin closed mid-prompt - reaches
	// isAffirmative as "", the same as an explicit "n".
	app := &App{IO: IOStreams{TTY: true, In: strings.NewReader(""), Err: io.Discard}}

	err := promptYesNo(app, "Proceed?", "refused")
	if got := exitcode.CodeOf(err); got != exitcode.ConfirmationRequired {
		t.Errorf("CodeOf(promptYesNo(EOF)) = %d, want ConfirmationRequired (%d)", got, exitcode.ConfirmationRequired)
	}
}

func TestPromptYesNo_withoutATerminalRefusesWithoutReadingInput(t *testing.T) {
	// In is nil: if this ever reached the ReadString call it would panic, so a passing
	// test proves the CanPrompt gate really runs first.
	app := &App{IO: IOStreams{TTY: false, In: nil, Err: io.Discard}}

	err := promptYesNo(app, "Proceed?", "refused: not a terminal")
	if got := exitcode.CodeOf(err); got != exitcode.ConfirmationRequired {
		t.Errorf("CodeOf(promptYesNo, no TTY) = %d, want ConfirmationRequired (%d)", got, exitcode.ConfirmationRequired)
	}
	if err == nil || !strings.Contains(err.Error(), "refused: not a terminal") {
		t.Errorf("Error() = %v, want it to contain the caller's own refusal message", err)
	}
}

func TestIsAffirmative_yAndYesAreCaseInsensitive(t *testing.T) {
	for _, line := range []string{"y", "Y", "yes", "YES", "Yes", " y \n", "y\n", "  yes\n"} {
		if !isAffirmative(line) {
			t.Errorf("isAffirmative(%q) = false, want true", line)
		}
	}
}

func TestIsAffirmative_anythingElseIsNo(t *testing.T) {
	for _, line := range []string{"n", "no", "", "\n", "maybe", "yesplease"} {
		if isAffirmative(line) {
			t.Errorf("isAffirmative(%q) = true, want false", line)
		}
	}
}

func TestIsTerminal_aCharacterDeviceIsATerminal(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("/dev/null character-device semantics do not apply on windows")
	}
	f, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatalf("opening %s: %v", os.DevNull, err)
	}
	defer f.Close()
	if !isTerminal(f) {
		t.Errorf("isTerminal(%s) = false, want true (it is a character device)", os.DevNull)
	}
}

func TestIsTerminal_aRegularFileIsNotATerminal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "regular.txt")
	if err := os.WriteFile(path, []byte("data"), 0o600); err != nil {
		t.Fatalf("writing %s: %v", path, err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("opening %s: %v", path, err)
	}
	defer f.Close()
	if isTerminal(f) {
		t.Error("isTerminal(regular file) = true, want false")
	}
}

func TestIsTerminal_aStatFailureIsNotATerminal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "closed.txt")
	if err := os.WriteFile(path, []byte("data"), 0o600); err != nil {
		t.Fatalf("writing %s: %v", path, err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("opening %s: %v", path, err)
	}
	f.Close() // Stat on an already-closed *os.File returns an error.
	if isTerminal(f) {
		t.Error("isTerminal(closed file) = true, want false (Stat itself failed)")
	}
}
