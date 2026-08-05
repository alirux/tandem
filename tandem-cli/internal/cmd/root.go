// Package cmd is tandem-cli's hand-written command layer over the generated Admin API
// client (LLD-cli.md §2/§8): one cobra command per operationId, plus the shared
// connection, auth, output, and confirmation machinery every command uses.
package cmd

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/alirux/tandem/tandem-cli/internal/auth"
	"github.com/alirux/tandem/tandem-cli/internal/client"
	"github.com/alirux/tandem/tandem-cli/internal/exitcode"
	"github.com/alirux/tandem/tandem-cli/internal/output"
)

// Version is tandem-cli's own semver, set via -ldflags at build time (goreleaser); "dev"
// when built without it (`go run`, `go test`, a plain `go build`).
var Version = "dev"

// AdminAPIVersion is the Admin API major version this build targets (LLD-cli.md §9.1).
// Reported by --version alongside Version, so an operator can tell at a glance whether a
// binary can drive a given deployment; bumped only alongside a deliberate /v2 migration.
const AdminAPIVersion = "v1"

// legalNotice is printed as part of the root command's help (`tandem-cli --help` /
// `tandem-cli` with no args) - the first, and for a binary-only user maybe only, screen
// they see. LICENSE carries the full text; this is the short form. Not repeated under
// --version: that output is meant for scripts/version checks, where the notice is noise
// on every single invocation.
const legalNotice = `Copyright 2026 Alberto Lirussi
Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
software except in compliance with the License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, this software is distributed
on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing permissions and limitations.`

type rootFlags struct {
	baseURL  string
	token    string
	apiKey   string
	headers  []string
	caCert   string
	insecure bool
	timeout  time.Duration
	output   string
	yes      bool
}

// NewRootCmd builds a fresh command tree with its own flag state - never package-level
// vars - so tests can construct as many independent instances as they need (LLD-cli.md
// §10) without one invocation's flags leaking into the next.
func NewRootCmd() *cobra.Command {
	flags := &rootFlags{}

	const short = "Inspect and act on a Tandem outbox and relay"
	root := &cobra.Command{
		Use:           "tandem-cli",
		Short:         short,
		Long:          short + "\n\n" + legalNotice,
		Version:       fmt.Sprintf("%s (Admin API %s)", Version, AdminAPIVersion),
		SilenceUsage:  true,
		SilenceErrors: true,
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			return setupApp(cmd, flags)
		},
	}
	root.SetVersionTemplate("{{.Version}}\n")

	pf := root.PersistentFlags()
	pf.StringVar(&flags.baseURL, "base-url", "", "Admin API base URL (required; or set TANDEM_ADMIN_URL)")
	pf.StringVar(&flags.token, "token", "", "bearer token (or set TANDEM_ADMIN_TOKEN)")
	pf.StringVar(&flags.apiKey, "api-key", "", "API key (or set TANDEM_ADMIN_API_KEY)")
	pf.StringArrayVar(&flags.headers, "header", nil, "extra request header 'Name: value' (repeatable)")
	pf.StringVar(&flags.caCert, "ca-cert", "", "PEM file to verify the server against (or set TANDEM_ADMIN_CA_CERT)")
	pf.BoolVar(&flags.insecure, "insecure", false, "skip TLS certificate verification")
	pf.DurationVar(&flags.timeout, "timeout", auth.DefaultTimeout, "per-request timeout; 0 disables it")
	pf.StringVar(&flags.output, "output", "human", "output format: human or json")
	pf.BoolVar(&flags.yes, "yes", false, "confirm destructive actions without prompting")

	// Unsorted, so the help listing follows insertion order below instead of cobra's
	// default alphabetical sort - a process-wide cobra setting, but every NewRootCmd()
	// call wants the same order, so setting it repeatedly is harmless.
	cobra.EnableCommandSorting = false
	root.AddCommand(newOutboxCmd(), newRelayCmd())

	// The completion command is deliberately NOT created here (e.g. via
	// root.InitDefaultCompletionCmd()): its generated subcommands capture
	// cmd.OutOrStdout() at creation time, so creating them before a caller's own
	// root.SetOut() (as every test in this package does) would silently pin their
	// output to the real os.Stdout instead. Left to cobra's own lazy creation inside
	// ExecuteC(), which runs after SetOut. See prepareCompletionCmd below for the
	// text/ordering customization, applied lazily for the same reason.

	// cobra.Command.ExecuteC unconditionally re-appends its "help" command last on every
	// real invocation (Command.InitDefaultHelpCmd, called from ExecuteC) - re-append
	// completion last again, right before help/usage text actually renders, so it reliably
	// sorts after "help" too. completion is genuinely the odd one out among the top-level
	// commands: the only one that never talks to the Admin API at all.
	defaultHelpFunc := root.HelpFunc()
	defaultUsageFunc := root.UsageFunc()
	root.SetHelpFunc(func(c *cobra.Command, args []string) {
		prepareCompletionCmd(c.Root())
		defaultHelpFunc(c, args)
	})
	root.SetUsageFunc(func(c *cobra.Command) error {
		prepareCompletionCmd(c.Root())
		return defaultUsageFunc(c)
	})

	return root
}

// PrepareForDocGeneration finalizes root's command tree for tooling that walks
// root.Commands() directly - cobra/doc's GenMarkdownTree (cmd/gendocs, LLD-cli.md §8) -
// rather than going through cobra's own lazy help/usage rendering path used at runtime.
// Without this, completion would be missing from the generated reference (it is
// otherwise only created just-in-time, see prepareCompletionCmd) and every page would
// carry a "generated on <date>" footer that changes on every regeneration even when
// nothing else did, defeating CI's regenerate-and-diff drift check (same pattern as the
// generated Admin API client, §8).
func PrepareForDocGeneration(root *cobra.Command) {
	root.InitDefaultHelpCmd()
	prepareCompletionCmd(root)
	disableAutoGenTag(root)
}

func disableAutoGenTag(c *cobra.Command) {
	c.DisableAutoGenTag = true
	for _, child := range c.Commands() {
		disableAutoGenTag(child)
	}
}

// prepareCompletionCmd ensures the completion command exists (idempotent - a no-op if
// cobra already created it earlier in this same Execute call), gives it a description of
// what it actually does instead of cobra's generic default text, and moves it to sort
// last. Called lazily, from the help/usage rendering hooks only, never at construction
// time - see the comment in NewRootCmd for why.
func prepareCompletionCmd(root *cobra.Command) {
	root.InitDefaultCompletionCmd()
	for _, c := range root.Commands() {
		if c.Name() != "completion" {
			continue
		}
		c.Short = "Generate a shell autocompletion script"
		c.Long = "Generates a script that adds tab-completion for tandem-cli's commands, " +
			"subcommands, and flags to your shell. Run the subcommand for your shell " +
			"(bash, zsh, fish, or powershell) and see its own --help for how to load the " +
			"script. Editor/shell convenience only - it never talks to the Admin API."
		root.RemoveCommand(c)
		root.AddCommand(c)
		return
	}
}

// needsApp reports whether cmd (or an ancestor) is "help" or "completion" - the two
// commands with no RunE that touches App, so setupApp must not require a connection for
// them.
func needsApp(cmd *cobra.Command) bool {
	for c := cmd; c != nil; c = c.Parent() {
		if c.Name() == "help" || c.Name() == "completion" {
			return false
		}
	}
	return true
}

// setupApp resolves the connection (auth.Resolve), builds the generated client around
// it, and stores the result as an *App on cmd's context for every subcommand's RunE to
// read via appFrom. Runs once, in the root command's PersistentPreRunE, before any
// subcommand executes.
func setupApp(cmd *cobra.Command, flags *rootFlags) error {
	// PersistentPreRunE runs before every command in the tree, including cobra's own
	// built-in "help" and the "completion" subtree - neither talks to the Admin API, so
	// neither should be blocked on --base-url (or any other connection flag).
	if !needsApp(cmd) {
		return nil
	}

	outputMode, operr := output.ParseMode(flags.output)
	if operr != nil {
		return exitcode.New(exitcode.UsageError, "%s", operr.Error())
	}

	resolved, aerr := auth.Resolve(auth.Options{
		BaseURL:    flags.baseURL,
		Token:      flags.token,
		APIKey:     flags.apiKey,
		Headers:    flags.headers,
		CACert:     flags.caCert,
		Insecure:   flags.insecure,
		Timeout:    flags.timeout,
		TimeoutSet: cmd.Flags().Changed("timeout"),
	}, func(msg string) { fmt.Fprintln(cmd.ErrOrStderr(), msg) })
	if aerr != nil {
		return aerr
	}

	c, cerr := client.NewClient(resolved.BaseURL,
		client.WithHTTPClient(resolved.HTTPClient),
		client.WithRequestEditorFn(resolved.RequestEditor))
	if cerr != nil {
		return exitcode.Wrap(exitcode.UnexpectedError, cerr, "building Admin API client")
	}

	app := &App{
		Client: c,
		Output: outputMode,
		Yes:    flags.yes,
		Stdout: cmd.OutOrStdout(),
		Stderr: cmd.ErrOrStderr(),
	}
	cmd.SetContext(withApp(cmd.Context(), app))
	return nil
}
