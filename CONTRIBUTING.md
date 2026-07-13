# Contributing to Tandem

Thanks for your interest in Tandem. This document covers what you need to build, test, and
propose changes.

## Before you start

Tandem is developed spec-first: the [HLD](docs/HLD.md) and the per-module `docs/LLD-*.md`
documents are the source of truth for design decisions. For anything beyond a small fix, please
open an issue to discuss the change before writing code — it avoids wasted work on a design that
doesn't fit the project's principles (per-aggregate ordering, backward/forward compatibility of
every contract, minimal footprint on the client's write path, opt-in complexity for the
non-default cases).

## Prerequisites

- **JDK 17** (the Gradle toolchain plugin auto-provisions it if not already installed).
  `tandem-benchmark` additionally requires JDK 25, provisioned the same way.
- **Docker**, running and reachable — most integration/e2e tests use
  [Testcontainers](https://testcontainers.com) to spin up real PostgreSQL and Kafka instances.
  There are no mocks standing in for the database or the broker in this project.

## Building and testing

```bash
./gradlew check
```

This compiles every module, runs unit tests, and runs the Testcontainers-backed
integration/e2e tests. It's the same command CI runs, so a green `check` locally is the bar for
a pull request.

To also produce the aggregated coverage report CI publishes to Codecov:

```bash
./gradlew check :tandem-coverage:aggregatedCoverageReport
```

`tandem-benchmark` is intentionally excluded from `check` (it's a load-testing harness, not a
published module). Run it explicitly if your change touches the relay's performance
characteristics:

```bash
./gradlew :tandem-benchmark:loadTest
```

## Project layout

| Module | Purpose |
|---|---|
| `tandem-core` | Ports and domain types — no I/O, no external dependencies. |
| `tandem-jdbc` | Write-side outbox INSERT, relay polling/claiming, PostgreSQL adapter. |
| `tandem-kafka` | CloudEvents publication to Kafka. |
| `tandem-test` | Test helpers (`InMemoryOutbox`, `RecordingDispatcher`, `TandemTestContainer`). |
| `tandem-sample` | Example application, not published. |
| `tandem-benchmark` | Load-testing harness, not published. |
| `tandem-coverage` | Aggregates JaCoCo coverage across modules for CI. |

The project follows a hexagonal (ports & adapters) style: `tandem-core` defines the ports,
adapter modules depend on `tandem-core`, never the reverse. See
[docs/HLD.md](docs/HLD.md) for the full architecture.

## Making a change

1. Fork the repository and create a branch from `main`.
2. Make your change, keeping it scoped — unrelated cleanup makes a PR harder to review.
3. Add or update tests. New behavior needs test coverage; bug fixes should include a test that
   would have caught the bug.
4. If the change affects a documented contract (REST API, DB schema, Kafka message format),
   update the relevant HLD/LLD alongside the code — see
   [docs/HLD.md §1.4](docs/HLD.md) for the compatibility rules any such change must satisfy.
5. Run `./gradlew check` locally before opening the PR.
6. Open a pull request against `main` with a clear description of the change and why it's
   needed.

## Code style

There's no auto-formatter enforced yet; match the style of the surrounding code. Keep comments
to the "why", not the "what" — well-named identifiers should make the "what" obvious.

## Reporting bugs and proposing features

Use [GitHub Issues](https://github.com/alirux/tandem/issues). For bugs, include Tandem version,
database/Kafka versions, and a minimal repro if possible. For feature proposals, a short
description of the use case is more useful up front than a full design.

## License

By contributing, you agree that your contributions will be licensed under the
[Apache License 2.0](LICENSE), the same license as the rest of the project.
