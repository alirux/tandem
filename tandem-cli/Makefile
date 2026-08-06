.PHONY: generate docs build test lint fmt

# Pinned, so a local run and CI lint with exactly the same rule set - an unpinned linter
# turns a routine version bump into a surprise red build on an untouched file.
GOLANGCI_LINT_VERSION := v2.6.2

# Regenerates internal/client/generated.go from ../docs/admin-api.openapi.yaml (LLD-cli.md
# §8). Run after the contract changes; CI re-runs this and fails the build on any diff.
generate:
	go generate ./...

# Regenerates the Markdown command reference under docs/cli/ from the live cobra command
# tree (LLD-cli.md §8). Run after a command/flag/description changes; CI re-runs this and
# fails the build on any diff, same as `generate` above.
docs:
	go run ./cmd/gendocs

build:
	go build -o bin/tandem-cli ./cmd/tandem-cli

test:
	go test ./... -race -coverprofile=coverage.out -covermode=atomic

# golangci-lint covers what `go vet` alone does not: dead code, unchecked errors,
# staticcheck. Configured in .golangci.yml, and run identically by CI.
#
# Two invocations on purpose: `run` does not report formatting - in golangci-lint v2 the
# formatters moved out of the analysis pass - so `fmt --diff` is what actually fails the
# build on unformatted code. It prints the diff rather than rewriting; `make fmt` applies it.
lint:
	go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION) fmt --diff
	go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION) run

fmt:
	go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION) fmt
