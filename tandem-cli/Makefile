.PHONY: generate docs build test lint fmt

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
	go test ./... -race

lint:
	go vet ./...
	gofmt -l . | (! grep .)

fmt:
	gofmt -w .
