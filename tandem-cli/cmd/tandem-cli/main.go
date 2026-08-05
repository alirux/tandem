// Command tandem-cli is a convenience command-line frontend over the Tandem Admin API
// (docs/LLD-cli.md). It holds no direct database access; every command is a thin wrapper
// around one operationId in the committed OpenAPI contract.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/alirux/tandem/tandem-cli/internal/cmd"
	"github.com/alirux/tandem/tandem-cli/internal/exitcode"
)

func main() {
	root := cmd.NewRootCmd()
	err := root.ExecuteContext(context.Background())
	if err != nil && exitcode.CodeOf(err) != exitcode.Success {
		fmt.Fprintln(os.Stderr, "Error:", err)
	}
	os.Exit(exitcode.CodeOf(err))
}
