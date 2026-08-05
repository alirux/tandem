//go:build tools

// A separate module (its own go.mod) so oapi-codegen's own dependency floor never
// raises the go directive of the main tandem-cli module — see internal/client/generate.go.
package tools

import (
	_ "github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen"
)
