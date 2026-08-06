package cmd

import (
	"context"
	"io"
	"os"

	"github.com/spf13/cobra"

	"github.com/alirux/tandem/tandem-cli/internal/client"
	"github.com/alirux/tandem/tandem-cli/internal/output"
)

// IOStreams bundles the CLI's three streams with the two terminal capabilities every
// rendering decision depends on. They travel together on purpose: a command that writes
// to an injected buffer must also see TTY/Color as false, or it would colorize a file and
// try to redraw a dashboard in place on something with no cursor. Deciding both from the
// same writer the output actually goes to is what keeps those answers consistent.
type IOStreams struct {
	In  io.Reader
	Out io.Writer
	Err io.Writer

	// TTY reports whether Out is an interactive terminal.
	TTY bool
	// Color reports whether Out should carry ANSI color: a TTY with NO_COLOR unset
	// (https://no-color.org), the gate LLD-cli.md §3.1/§6 specifies.
	Color bool
}

// CanPrompt reports whether an interactive y/N question can be asked at all - piped or
// redirected, there is nobody to answer it and the caller must require --yes instead.
func (s IOStreams) CanPrompt() bool { return s.TTY }

// systemIOStreams builds the streams around a cobra command's own writers, so a caller
// that redirected them (every test in this package) moves the TTY and color decisions
// along with the output rather than leaving them pinned to the process's real stdout.
func systemIOStreams(cmd *cobra.Command) IOStreams {
	out := cmd.OutOrStdout()
	tty := isTerminalWriter(out)
	return IOStreams{
		In:    cmd.InOrStdin(),
		Out:   out,
		Err:   cmd.ErrOrStderr(),
		TTY:   tty,
		Color: tty && os.Getenv("NO_COLOR") == "",
	}
}

// App is the resolved, per-invocation state every subcommand reads: the generated
// client, the output mode, the --yes flag, and the IO streams. Populated once in the root
// command's PersistentPreRunE and threaded through cmd.Context() - never package-level
// state, so repeated NewRootCmd() calls (one per test case, per LLD-cli.md §10) never
// leak into each other.
type App struct {
	Client *client.Client
	Output output.Mode
	Yes    bool
	IO     IOStreams
}

type appKeyType struct{}

var appKey = appKeyType{}

func withApp(ctx context.Context, app *App) context.Context {
	return context.WithValue(ctx, appKey, app)
}

// appFrom retrieves the App a PersistentPreRunE stored on the command's context. Every
// leaf command's RunE runs after root's PersistentPreRunE, so the value is always
// present; a missing value is a wiring bug in this package, not a user-facing condition,
// hence the panic rather than a returned error.
func appFrom(cmd *cobra.Command) *App {
	app, ok := cmd.Context().Value(appKey).(*App)
	if !ok {
		panic("tandem-cli: no App in context - PersistentPreRunE did not run")
	}
	return app
}
