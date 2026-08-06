package cmd

import (
	"bytes"
	"testing"

	"github.com/alirux/tandem/tandem-cli/internal/exitcode"
)

// execute runs a fresh root command end to end (LLD-cli.md §10): real cobra parsing, real
// flag/env resolution, a real HTTP round trip to server, no mocks. Every case gets its own
// NewRootCmd(), so nothing leaks between them even when run in parallel.
func execute(t *testing.T, baseURL string, args ...string) (stdout, stderr string, code exitcode.Code) {
	t.Helper()
	root := NewRootCmd()
	var out, errOut bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetArgs(append([]string{"--base-url", baseURL}, args...))

	err := root.Execute()
	return out.String(), errOut.String(), exitcode.CodeOf(err)
}

// appWithColor is the minimal App a pairs/row rendering helper needs: nothing but the
// color decision, since those helpers never touch the client or the streams.
func appWithColor(color bool) *App {
	return &App{IO: IOStreams{Color: color}}
}

// executeNoServer is execute without prepending --base-url, for cases exercising base-URL
// resolution itself (missing, env-var fallback) or pure usage-error paths that never reach
// the network.
func executeNoServer(t *testing.T, args ...string) (stdout, stderr string, code exitcode.Code) {
	t.Helper()
	root := NewRootCmd()
	var out, errOut bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetArgs(args)

	err := root.Execute()
	return out.String(), errOut.String(), exitcode.CodeOf(err)
}
