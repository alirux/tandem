package cmd

import (
	"bytes"
	"strings"
	"testing"
)

func TestSetupApp_invalidOutputFlagIsAUsageError(t *testing.T) {
	_, _, code := executeNoServer(t, "--base-url", "http://unused.invalid", "--output", "xml", "outbox", "summary")
	if code != 2 {
		t.Errorf("exit code = %d, want 2 (UsageError)", code)
	}
}

func TestSetupApp_insecureWarnsOnStderrThroughARealInvocation(t *testing.T) {
	server := newFixtureServer(t, map[string]route{
		"GET /outbox/summary": {200, "outbox_summary.json"},
	})

	root := NewRootCmd()
	var out, errOut bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetArgs([]string{"--base-url", server.URL, "--insecure", "outbox", "summary"})
	if err := root.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !strings.Contains(errOut.String(), "--insecure") {
		t.Errorf("stderr = %q, want the --insecure warning", errOut.String())
	}
}

func TestPrepareForDocGeneration_disablesTheAutoGenTagOnTheWholeTree(t *testing.T) {
	root := NewRootCmd()
	PrepareForDocGeneration(root)

	if !root.DisableAutoGenTag {
		t.Error("root.DisableAutoGenTag = false, want true")
	}
	for _, c := range root.Commands() {
		if !c.DisableAutoGenTag {
			t.Errorf("command %q: DisableAutoGenTag = false, want true", c.Name())
		}
	}
}

func TestPrepareForDocGeneration_includesHelpAndCompletionInTheTree(t *testing.T) {
	root := NewRootCmd()
	PrepareForDocGeneration(root)

	var hasHelp, hasCompletion bool
	for _, c := range root.Commands() {
		switch c.Name() {
		case "help":
			hasHelp = true
		case "completion":
			hasCompletion = true
		}
	}
	if !hasHelp {
		t.Error("root.Commands() missing help - PrepareForDocGeneration must materialize it")
	}
	if !hasCompletion {
		t.Error("root.Commands() missing completion - PrepareForDocGeneration must materialize it")
	}
}

func TestHelp_completionSortsLastAfterHelpItself(t *testing.T) {
	root := NewRootCmd()
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetArgs([]string{"--help"})
	if err := root.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	text := out.String()
	helpIdx := strings.Index(text, "\n  help ")
	completionIdx := strings.Index(text, "\n  completion ")
	if helpIdx == -1 || completionIdx == -1 {
		t.Fatalf("expected both help and completion listed, got:\n%s", text)
	}
	if completionIdx < helpIdx {
		t.Errorf("completion (at %d) should sort after help (at %d):\n%s", completionIdx, helpIdx, text)
	}
}

func TestHelp_doesNotRequireABaseURL(t *testing.T) {
	root := NewRootCmd()
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetArgs([]string{"help"})
	if err := root.Execute(); err != nil {
		t.Fatalf("`tandem-cli help` must not require --base-url, got error: %v", err)
	}
	if !strings.Contains(out.String(), "Available Commands:") {
		t.Errorf("expected the command list, got:\n%s", out.String())
	}
}

func TestCompletion_doesNotRequireABaseURL(t *testing.T) {
	root := NewRootCmd()
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetArgs([]string{"completion", "bash"})
	if err := root.Execute(); err != nil {
		t.Fatalf("`tandem-cli completion bash` must not require --base-url, got error: %v", err)
	}
	if !strings.Contains(out.String(), "bash completion") {
		t.Errorf("expected a bash completion script, got %d bytes", out.Len())
	}
}
