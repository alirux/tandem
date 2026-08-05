// Command gendocs regenerates the Markdown command reference under docs/cli/ from the
// live cobra command tree - the user manual, kept in sync with the actual flags and
// descriptions by generation rather than by hand (LLD-cli.md §8). Not shipped; run via
// `make docs`, from the module root so the output path below resolves correctly. CI's
// regenerate-and-diff step is the drift gate, the same pattern already used for the
// generated Admin API client.
package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra/doc"

	"github.com/alirux/tandem/tandem-cli/internal/cmd"
)

const outDir = "docs/cli"

func main() {
	root := cmd.NewRootCmd()
	cmd.PrepareForDocGeneration(root)

	if err := os.MkdirAll(outDir, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "gendocs:", err)
		os.Exit(1)
	}
	if err := doc.GenMarkdownTree(root, outDir); err != nil {
		fmt.Fprintln(os.Stderr, "gendocs:", err)
		os.Exit(1)
	}
}
