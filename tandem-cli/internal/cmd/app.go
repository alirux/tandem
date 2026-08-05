package cmd

import (
	"context"
	"io"

	"github.com/spf13/cobra"

	"github.com/alirux/tandem/tandem-cli/internal/client"
	"github.com/alirux/tandem/tandem-cli/internal/output"
)

// App is the resolved, per-invocation state every subcommand reads: the generated
// client, the output mode, and the --yes flag. Populated once in the root command's
// PersistentPreRunE and threaded through cmd.Context() - never package-level state, so
// repeated NewRootCmd() calls (one per test case, per LLD-cli.md §10) never leak into
// each other.
type App struct {
	Client *client.Client
	Output output.Mode
	Yes    bool
	Stdout io.Writer
	Stderr io.Writer
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
