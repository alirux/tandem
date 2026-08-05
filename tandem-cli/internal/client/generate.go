// Package client is the generated Admin API client. Regenerate with `go generate ./...`
// after the contract at docs/admin-api.openapi.yaml changes; the CI drift gate re-runs
// generation and fails the build if generated.go is not committed up to date.
package client

//go:generate go run -C ../../tools github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen -config ../internal/client/oapi-codegen.yaml -o ../internal/client/generated.go ../../docs/admin-api.openapi.yaml
