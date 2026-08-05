package cmd

import (
	"bytes"
	"strings"
	"testing"
)

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
