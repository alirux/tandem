# Tandem — `tandem-cli` LLD

**Version:** 1.0 (Implemented)
**Module:** `tandem-cli` · Go, **not** a Gradle subproject
**Depends on:** [admin-api.openapi.yaml](admin-api.openapi.yaml) (the only interface it talks to)
**Companion to:** [HLD-admin-api.md](HLD-admin-api.md) §4 (names the CLI as a future consumer of the contract);
backlog item 4 (`project_backlog_topics` memory) for the product requirements this LLD implements
**Toolchain:** Go 1.25+ (raised from 1.24 — `cobra`'s own `go.mod` floor — to match
`tandem-cli/tools/`'s own higher floor, so the two modules build with the same toolchain in
CI; `tools/` stays a separate module regardless, so a future bump there still doesn't force
one here), `cobra` (command tree), `oapi-codegen` (generated client, run from
`tandem-cli/tools/`)
**Published:** Not a JVM artifact, not on Maven Central. Distributed as cross-compiled binaries
attached to GitHub Releases (§9).
**Versioned independently of the library**, tagged `cli-v<semver>`; its compatibility contract is
the Admin API's major version, not Tandem's version (§9.1).

This document specifies `tandem-cli`, a convenience command-line frontend over the Admin API.
**It is a frontend over REST, never a second control path** — every command is a thin wrapper
around one `operationId` in the committed OpenAPI contract; the CLI holds no direct DB or JDBC
access, by the same architectural decision that put the Admin API itself at the top of the
backlog (production DB access is normally unavailable to the people handling an incident, and
mutating operations need the audit trail only the API provides).

---

## 1. Purpose & scope

Ops can already drive the Admin API with `curl`/Postman. `tandem-cli` adds nothing new to *what*
can be done, only ergonomics: discoverable verbs, typed flags instead of hand-built JSON, no
remembering paths — using the same authentication and leaving the same audit trail as a raw HTTP
call, because it *is* a raw HTTP call.

**In scope:** one command per `operationId` currently in the contract (13 today, §3), covering
Outbox read/act and Relay control/observability.
**Out of scope:** anything the Admin API itself doesn't expose — no JDBC fallback, no bulk
discard (the contract has none, `IMPLEMENTATION-PLAN-admin-api.md` records this as deliberately
out of scope), no attempt-level forensic history (designed but not built, and absent from the
contract — [HLD-attempt-archive.md](HLD-attempt-archive.md); the CLI gains those two commands
if and when the endpoints land).

**Language: Go, chosen over Rust.** For a thin, low-throughput HTTP wrapper invoked
interactively or from a script, Rust's distinctive
value — memory safety without a GC, zero-cost abstractions — pays for nothing here, while its
async ceremony (`tokio`+`reqwest` for what is, per invocation, a single sequential request) and
steeper ramp-up for a team with no existing Rust code are real costs. Go matches the actual usage
shape (`net/http` synchronous, no async runtime needed), has the strongest category precedent
(`kubectl`, `gh`, `docker cli`, `terraform` are all Go), compiles fast, and cross-compiles to a
static binary with no extra toolchain. Kotlin/GraalVM native-image and Python were also compared
and rejected: the former adds a second build toolchain (native-image's reflection/resource
config) for a marginal "same language as the rest of the repo" win; the latter cannot produce a
single dependency-free binary, which conflicts directly with §9's distribution requirement.

---

## 2. Module layout

`tandem-cli/` is a **top-level directory, sibling to the Gradle modules, not a Gradle
subproject** — it is not registered in `settings.gradle.kts`, carries its own `go.mod`, and is
therefore **out of scope for LLD-base.md's subproject table** (that table is specifically the
Gradle/Maven-publication surface) and for AGENTS.md's Gradle-specific module-registration
entries (BOM, `tandem-coverage`, `unpublishedModules`). Three of that checklist's entries still
apply in spirit and are called out explicitly rather than silently skipped:

- **README.md module table / CONTRIBUTING.md project layout** — carry a `tandem-cli` entry
  noting it is Go and pointing here.
- **THIRD-PARTY-NOTICES.md** — **does apply**, contrary to first instinct. The Gradle checklist
  scopes this to "published modules" because that's what a JVM consumer inherits transitively;
  for `tandem-cli` the equivalent inheritance event is **static linking into the distributed
  binary** (§9) — every dependency compiled into the binary ships to whoever downloads a
  release, so their licenses get the same treatment as a published JAR's transitive tree, in
  their own section (the "reaches the classpath" framing doesn't apply to a static binary).
- **`settings.gradle.kts` / `tandem-bom` / `tandem-coverage`** — genuinely don't apply (Gradle-
  and Maven-Central-specific).

```
tandem-cli/
  go.mod / go.sum                   // go 1.25 - matches tools/'s own floor (§1)
  LICENSE                           // copy of the repo's Apache-2.0 text - this module is
                                    //   distributed as standalone binaries (§9), independent of
                                    //   the JVM modules' Maven Central publication, so it carries
                                    //   its own copy rather than relying on the repo root's
  .goreleaser.yaml                  // cross-compile + GitHub Release config, run by
                                    //   ../.github/workflows/cli-release.yml on cli-v* (§9)
  .gitignore                        // /bin/, /dist/, __pycache__/ (build and goreleaser output)
  Makefile                          // generate, docs, build, test, lint targets
  cmd/tandem-cli/main.go            // entrypoint; builds and executes the cobra root command
  cmd/gendocs/main.go               // dev tool: regenerates docs/cli/*.md (§8), not shipped -
                                    //   goreleaser's builds: names cmd/tandem-cli explicitly, so
                                    //   this second main package is never cross-compiled/released
  docs/cli/*.md                     // committed; regenerated via `make docs`, drift-checked in CI -
                                    //   the user manual, one page per command (§8)
  .golangci.yml                     // lint config: the standard linter set plus bodyclose/
                                    //   predeclared/unconvert/misspell; run by `make lint` (§8)
  hack/fake-admin-api.py            // stdlib-only fake Admin API for trying bin/tandem-cli by
                                    //   hand (CONTRIBUTING.md); not part of the Go module
  tools/                            // separate module: pins oapi-codegen's own version so its
    go.mod / go.sum                //   higher Go-version floor never raises the main module's
    tools.go                       //   (`//go:build tools`, blank-imports the generator)
  internal/
    client/
      oapi-codegen.yaml             // package: client, generate: models+client (no `output:` -
                                    //   passed explicitly via -o on the go:generate line instead,
                                    //   since a config-file output overrides a command-line one)
      generate.go                  // //go:generate go run -C ../../tools oapi-codegen ...
      generated.go                  // committed; regenerated via `go generate`, drift-checked in CI
    cmd/
      app.go                       // App (client/output-mode/--yes) plus IOStreams (the three
                                    //   streams and the TTY/color decisions taken off them),
                                    //   threaded via cmd.Context()
      exec.go                      // do() classifies every response into (body, error);
                                    //   confirm()/promptYesNo() implement the --yes/TTY gate (§5)
      render.go                    // renderObject/renderList: the one json-vs-human branch every
                                    //   command shares, generic over the response type (§6)
      parse.go                     // positional-argument parsing, as usage errors (§7)
      root.go                       // persistent flags: --base-url, --token, --api-key, --header,
                                    //   --ca-cert, --insecure, --timeout, --output, --yes
      outbox.go                     // summary, search, get, replay, discard, replay-bulk
      dashboard.go                  // outbox summary --watch: the poll loop and its renderer (§3.1)
      relay.go                      // status, pause, resume, buckets (list + single), release-bucket, workers
      testdata/                    // canned Admin API response fixtures for the tests below (§10)
      *_test.go                     // one file per resource area, httptest-backed (§10)
    auth/                           // token/api-key resolution (flag > env > error), §4
    output/                         // human (tabwriter) vs --output json renderer, §6
    exitcode/                       // exit-code table + ProblemDetail.type → code mapping, §7
```

---

## 3. Command tree

Resource-based (`tandem-cli <resource> <verb>`), the `kubectl`/`gh` idiom — matches how the
contract itself groups operations (`tags: Outbox / Relay`, §2 of HLD-admin-api). One subcommand
per `operationId`, no CLI-invented aggregate operations — with the single exception noted under
the table:

| `operationId` | Command |
|---|---|
| `getOutboxSummary` | `tandem-cli outbox summary [--watch] [--interval <dur>]` |
| `searchOutboxMessages` | `tandem-cli outbox search [--status] [--aggregate-id] [--aggregate-type] [--type] [--created-from] [--created-to] [--correlation-id] [--limit] [--cursor]` |
| `getOutboxMessage` | `tandem-cli outbox get <id>` |
| `replayMessage` | `tandem-cli outbox replay <id>` |
| `discardMessage` | `tandem-cli outbox discard <id> --reason <text> [--yes]` |
| `replayBulk` | `tandem-cli outbox replay-bulk [--aggregate-id] [--aggregate-type] [--from-id] [--to-id] [--status ...] [--dry-run] [--yes]` |
| `getRelayStatus` | `tandem-cli relay status` |
| `pauseRelay` | `tandem-cli relay pause [--bucket <n>]` |
| `resumeRelay` | `tandem-cli relay resume [--bucket <n>]` |
| `getRelayBuckets` · `getRelayBucket` | `tandem-cli relay buckets [<bucket>] [--uncovered-only]` — no argument lists every bucket, an argument shows that one |
| `releaseBucket` | `tandem-cli relay release-bucket <bucket>` |
| `getRelayWorkers` | `tandem-cli relay workers` |

**One command, two operations — the table's only exception.** `relay buckets` calls
`getRelayBuckets` when given no argument and `getRelayBucket` when given one. Two subcommands
whose names differ by a single letter (`buckets` / `bucket`) would make a typo silently run the
wrong one, and "list the collection" versus "show one member" is the same resource at two
arities, not a CLI-invented aggregate operation. `--uncovered-only` applies only to the list
form; passing it alongside a bucket argument is a usage error (exit 2).

**LEASE-only endpoints.** `relay buckets` (either form), `relay release-bucket`, `relay
workers`, and `relay pause`/`relay resume` **with** `--bucket` all require `LEASE`
coordination — under `SINGLE` the relay never writes `tandem_bucket_lease`/
`tandem_relay_member`, so these return `409 relay-coordination-unsupported` rather than a
misleading empty/zero reading (HLD-admin-api §4.1/§6.1). The CLI does not special-case this
locally (no client-side mode check, no cached "last known mode") — it sends the request and
maps the `409` through the normal exit-code table (§7) like any other conflict. `relay status`
is the one relay read that works under both modes.

Flag names are the kebab-case form of the contract's query/body field names (`aggregateId` →
`--aggregate-id`), so a user who has read the OpenAPI doc already knows the CLI's vocabulary and
vice versa — no separate naming scheme to learn.

### 3.1 `--watch`: a live dashboard for `outbox summary`

The one command with behavior beyond a single request/response, and deliberately scoped to just
this one: `summary`'s five per-status counts share a natural comparison ("which status
dominates right now"), which none of the other commands' responses do — a table row or a
single scalar doesn't gain anything from a refresh loop the way a multi-count snapshot does.

- **`--watch`** re-polls `getOutboxSummary` every **`--interval`** (default `2s`) until the
  process receives an interrupt (Ctrl+C), instead of taking one reading and exiting.
- **`--output human` (default): a bar-chart dashboard, not the plain `key: value` block
  `summary` otherwise renders — but bar-charting only the three *live* states, `PENDING` /
  `IN_FLIGHT` / `FAILED`, scaled against the largest of those three.** `DONE` and `DISCARDED`
  are deliberately plain numbers, never bars sharing that scale, alongside `lagCount`/
  `lagAgeSeconds`. Reasoning found by review, not anticipated up front: a row leaves `DONE`
  only when the relay's periodic cleanup prunes it (LLD-jdbc §3.7), so `DONE`/`DISCARDED`
  accumulate for the outbox's whole lifetime and, in any system with real traffic, quickly
  dwarf the live states. Scaling all five together would pin `DONE`'s bar full and collapse
  `PENDING`/`FAILED` — the two that actually indicate a problem — into invisible slivers,
  defeating the dashboard's purpose exactly when it matters most.
- **On a real terminal, each refresh redraws in place — never a full-screen clear.**
  `dashboardWriter` (`isTerminal`, the same stdlib character-device check `outbox discard`'s
  TTY gate uses, §5) moves the cursor up by exactly the previous frame's line count and erases
  from there to the end of the screen (ANSI "cursor up" + "erase in display, from cursor"), then
  prints the new frame. Deliberately not `\033[2J\033[H` (clear entire screen + cursor to
  absolute origin): that resets the whole terminal, including whatever was on screen above the
  dashboard before `--watch` started, causing a visible flash/jump on every single refresh for
  no reason — a redraw only needs to touch the lines it owns. Redirected to a file or a pipe,
  there is no cursor to move, so frames print one after another instead — an ANSI escape
  sequence in a log file is noise, not a redraw.
- **`--output json` streams one raw response line per interval — still no CLI-invented
  envelope** (§6's rule holds under `--watch` too): each line is `GetOutboxSummary`'s response
  body exactly as received, nothing added, no synthetic timestamp field. A consumer that wants
  one pipes through its own timestamping (`| ts`) rather than the CLI inventing a wire shape the
  contract doesn't have.
- **A request failure during the loop keeps the loop going rather than exiting** — the loop's
  whole point is staying up through a transient blip (a relay restart, a brief network hiccup).
  In human mode it prints `Error: ...` in place of that frame, through the same
  `dashboardWriter`. **In JSON mode it goes to stderr, never stdout** — stdout is an NDJSON
  stream a consumer parses line by line, and the human frame text is not valid JSON; writing it
  to stdout would corrupt the stream on the very first hiccup (found by a flaky test: the error
  path didn't originally check the output mode, so a request that failed near a JSON-mode test's
  time budget intermittently broke JSON decoding — fixed by branching on `app.Output` in the
  error case, same as the success case already did). This is the one command where a
  classified error is deliberately *not* returned up to `main` on every failure — only a real
  setup problem (e.g. `--interval 0`) is, and only before the loop starts.
- **`--interval` must be greater than zero** — a local usage error (exit 2) before any HTTP
  call, same shape as `outbox discard`'s missing `--reason` (§5).

---

## 4. Connection & auth

Flag beats env var throughout. Only the base URL is mandatory:

| Setting | Flag | Env var | Notes |
|---|---|---|---|
| Base URL | `--base-url` | `TANDEM_ADMIN_URL` | **Required** — no default, since the Admin API's base path is itself configurable (HLD-admin-api §3), so the CLI cannot guess it |
| Bearer token | `--token` | `TANDEM_ADMIN_TOKEN` | Sent as `Authorization: Bearer <token>` |
| API key | `--api-key` | `TANDEM_ADMIN_API_KEY` | Sent as `X-API-Key`, the contract's other declared scheme |
| Extra header | `--header 'K: V'` | — | Repeatable; the escape hatch for any scheme the two above don't cover |
| CA bundle | `--ca-cert <file>` | `TANDEM_ADMIN_CA_CERT` | PEM bundle to verify the server against, for a private CA |
| Skip TLS verification | `--insecure` | — | Warns on stderr each time it is used |
| Request timeout | `--timeout <dur>` | — | Per request, default `60s`; `0` disables |

**Credentials are optional, and the CLI never enforces an authentication policy of its own.**
Tandem ships the endpoints, not the authentication — wiring it is the host application's job
(HLD-admin-api §3), so a real deployment may present bearer tokens, an API key, Basic auth, mTLS,
a gateway that injects its own header, or nothing at all when the API is isolated on an internal
network (HLD-admin-api §4.1). With no credential given the CLI **sends the request
unauthenticated and lets the server decide**: refusing locally would be the client inventing a
rule the library deliberately delegates, and would make a legitimate no-auth deployment
unreachable. A `401` comes back through the normal exit-code path (§7) like any other response.
`--header` covers the schemes the contract does not declare — `--header 'Authorization: Basic …'`
and the like — without the CLI having to model each one.

**TLS.** The standalone Admin API is expected to sit on an internal network, where private-CA and
self-signed certificates are common, so `--ca-cert` supplies a bundle and `--insecure` skips
verification outright. `--insecure` is a deliberate, loud escape hatch — it prints a warning to
stderr on every use — and it exists because an ops tool that refuses to reach a self-signed
internal endpoint does not make anyone safer; it just sends the operator back to `curl -k`.

**No config file / profile support in v1** — Pareto: the common case is one Admin API endpoint per
invocation context (a laptop, a CI job), fully covered by the env vars; multi-environment profile
switching is deferred to when an adopter actually asks for it (§11).

---

## 5. Mutating and destructive commands

The backlog requirement is explicit: *"a confirmation (or `--yes`) for the destructive verbs,
since discard is irreversible."* Applied per-command rather than blanket, because not every
mutation is equally consequential:

| Command | Confirmation? | Why |
|---|---|---|
| `outbox discard` | **Yes** | Irreversible — `DISCARDED` has no path back. The contract itself requires `acknowledgeOrderingBreak: true` in the body (`DiscardRequest`); the CLI's `--yes` **is** that acknowledgement, not a second, separate confirmation of the same fact. Without `--yes` and without a TTY to prompt on, the command fails locally (exit 7, §7) rather than sending `acknowledgeOrderingBreak: false` and surfacing the API's own 400. |
| `outbox replay-bulk` (non-`--dry-run`) | **Yes** | Can match an unbounded number of rows. Without `--yes`: if attached to a TTY, the CLI first calls the API with `dryRun: true`, prints the matched count, and prompts; otherwise (piped/CI) it requires `--yes` up front and fails if absent. `--dry-run` itself never needs confirmation — it changes nothing. |
| `relay pause` / `relay resume` / `relay buckets/{bucket}/release` | No | All three are reversible within one poll cycle (pause/resume are each other's undo; a released bucket is just reclaimed by the next heartbeat) — closer to `kubectl cordon` than to a delete. Revisit if real incidents show this was wrong (§11). |
| Everything else | No | Read-only. |

TTY detection: `isatty` on stdout — matches the backlog's "no interactive prompt unless attached
to a TTY," so a script piping `tandem-cli` output never blocks on stdin.

**`--reason` is required on `discard`, deliberately stricter than the contract**, where
`DiscardRequest.reason` is optional (only `acknowledgeOrderingBreak` is required). Discard is the
one irreversible operation and the reason is what a later investigation has to go on, so the CLI
refuses to send one without it — a local usage error (exit 2), before any HTTP call. The contract
cannot impose this: making an existing optional field required would be a breaking change under
the additive-only rule (§1.4, HLD.md), and the API must keep serving clients that omit it. A
frontend tightening its own ergonomics above the contract's floor is free to do so; the reverse —
the CLI relaxing something the contract requires — would not be.

---

## 6. Output

`--output human` (default) or `--output json`:

- **`json`** — the raw response body, unmodified, straight to stdout. No CLI-invented envelope:
  the API's own JSON shapes (`OutboxEntry`, `OutboxEntryPage`, …) are already the contract, so
  reprinting them verbatim keeps `jq` usage predictable and needs no separate schema to document.
- **`human`** (default) — list/page responses (`search`, `buckets`, `workers`) render as a
  left-aligned table; single-object responses (`summary`, `get`, `status`) render as a `key:
  value` block via the stdlib `text/tabwriter` (no extra dependency, consistent with keeping the
  binary's third-party surface small, §2). The table renderer (`output.Table`) is **hand-rolled,
  not `text/tabwriter`**: column width is computed from each cell's ANSI-stripped *visible*
  width, not its raw byte length. `tabwriter` has no concept of an invisible escape sequence, so
  handing it a colorized cell in a *middle* column (COVERED/PAUSED in `relay buckets`, not just a
  trailing cell) would count the escape bytes as visible characters and misalign every column
  after it, on exactly the rows where the color differs from its neighbors — found while adding
  the coloring below. `KeyValue` stays on `tabwriter` safely, since its colored value is always a
  line's last cell, which `tabwriter` never pads.
- **Color, gated by `isTerminal(stdout) && NO_COLOR unset`** (https://no-color.org, same gate
  `--watch`'s dashboard uses, §3.1) — restricted to the field(s) that answer "is this normal or
  worth a second look," never a whole row: labels, counts, and unfilled bar cells always stay
  plain.
  - `relay status`/`pause`/`resume`: `state` — green `RUNNING`, yellow `PAUSED`, red `DOWN` (no
    relay instance has heartbeated recently, HLD-admin-api §4.1 — the same "something needs a
    human" meaning as the outbox's `FAILED`). `DOWN` takes priority over `PAUSED` server-side, so
    the CLI never has to choose between them. A future additive enum value (§1.4) falls through
    uncolored.
  - `relay buckets`/`release-bucket`: `covered` — green `true` / red `false` (both values are
    worth flagging, since an uncovered bucket under `LEASE` means nothing is draining it).
    `paused` — yellow `true` only; `false` is the boring default and stays plain, same rule as
    `state`'s missing red.
- **Cursor pagination is never auto-followed.** A page ending in a non-null `nextCursor` prints a
  `next page: --cursor=<value>` hint in human mode and leaves `nextCursor` as-is in JSON mode. No
  `--all`/auto-follow flag in v1 — deliberately matches the contract's own pagination decision
  (HLD-admin-api §6.1: cursor-only, no jump-to-page, operators page forward through time) rather
  than building a client-side feature the API design explicitly chose not to offer server-side.
  Deferred to §11 if real usage shows it's wanted.

---

## 7. Exit codes

The backlog's "usable from a script or by hand" requirement: **distinct non-zero exit codes per
failure mode**, keyed off the RFC 9457 `ProblemDetail.type` **slug** (AGENTS.md: the stable
identifier), never the raw HTTP status alone — two problems can share a status (`400` covers both
`invalid-parameter` and `ordering-break-not-acknowledged`) and a script needs to tell them apart
without parsing prose.

| Exit code | Meaning | Trigger |
|---|---|---|
| `0` | Success | 2xx |
| `1` | Unexpected error | `internal-error` (500), or any response the client can't otherwise classify |
| `2` | Usage error | Bad flags/args — cobra's own usage-error path; a missing base URL (§4); `discard` without `--reason` (§5); `--uncovered-only` passed alongside a bucket argument (§3). **Never** a missing credential: none is required (§4) |
| `3` | Unauthorized | `unauthorized` (401) |
| `4` | Not found | `not-found` (404) |
| `5` | Invalid parameter | `invalid-parameter` (400) |
| `6` | Conflict / precondition failed | `message-not-replayable`, `message-not-discardable`, `relay-coordination-unsupported` (409) |
| `7` | Confirmation required or declined | `ordering-break-not-acknowledged` (400), or a local `--yes`/TTY gate (§5) that stopped the CLI before any HTTP call |
| `8` | Connection failure | Couldn't reach `--base-url` at all — DNS, TCP, TLS verification (see `--ca-cert`/`--insecure`, §4), or the `--timeout` elapsing — so there is no HTTP response to classify |

Every code is documented in `--help` and, once implemented, the module's README section — a
script author should never need to read this LLD to branch on the result.

Carried in-process by `exitcode.Error`, but **every function signature says plain `error`** —
`New`/`Wrap` return the interface, never the concrete `*Error`. A helper typed `*Error` that
returns nil produces a non-nil `error` the moment a caller passes it up, silently turning a
success into an exit code; `exitcode.CodeOf` recovers the code with `errors.As`, so it keeps
working through any wrapping a caller adds.

---

## 8. Generated client (`oapi-codegen`)

**Client generated, command layer hand-written.**
`oapi-codegen -generate types,client` produces `internal/client/generated.go` directly from the
committed [admin-api.openapi.yaml](admin-api.openapi.yaml) — the same file `tandem-admin` itself
implements against. This is the opposite choice from HLD-admin-api §6.1 ("hand-write and
validate in CI, no codegen") for `tandem-admin`'s *server* — deliberately, not by oversight: that
decision was about a Java code generator fighting this project's Java-specific conventions
(doclint rules, the ban on inline comments, and especially the `toString()`-never-prints-payload
rule with its dedicated unit test, AGENTS.md Logging §5). None of that applies to a **Go client**:
there is no Java doclint to fight, and `oapi-codegen`'s output is typed request/response structs
and an HTTP wrapper, not business logic a Java convention would object to. Generating it removes
a structural drift risk (client types silently diverging from the contract) that hand-writing
would otherwise need its own contract test to catch — for the command layer sitting on top,
which *is* hand-written, exactly like `tandem-admin`'s handlers.

- The generated file is **committed**, not gitignored — a consumer building `tandem-cli` from
  source needs only `go build`, not `oapi-codegen` on their machine.
- CI's `tandem-cli` job (`.github/workflows/ci.yml`, separate from the Gradle build) adds a
  **regenerate-and-diff** step (`make generate && git diff --exit-code`) as the drift gate — the
  Go-toolchain equivalent of Specmatic's conformance check for `tandem-admin` — followed by
  `make lint` (**golangci-lint**, pinned in the Makefile so a local run and CI apply the same
  rules — the standard linter set, which `go vet` alone does not cover, plus `bodyclose` /
  `predeclared` / `unconvert` / `misspell`, and a separate `fmt --diff` pass because v2's
  analysis run does not report formatting) and `make test` (`go test ./... -race`), which also emits
  `coverage.out` (`-coverprofile=coverage.out -covermode=atomic`), uploaded to Codecov under the
  `cli` flag — separate from the Java modules' aggregated JaCoCo report (`java` flag), since the
  two live in different CI jobs with no shared build.
- **Forward compatibility applies to the client too** (§1.4, HLD.md): the generated types must
  tolerate unknown fields and unknown enum values in responses — `oapi-codegen`'s default struct
  generation already does this (unrecognized JSON object fields are simply dropped by
  `encoding/json`, and open-schema responses per the contract carry no `additionalProperties:
  false` to fight). `ProblemDetail.additionalProperties: true` needs no special handling for the
  same reason.

### 8.1 Generated user manual (`cobra/doc`)

**The user manual (`docs/cli/*.md`) is generated from the live cobra command tree, not
hand-written** — same reasoning as the client above: a hand-maintained manual drifts from the
actual flags/descriptions the moment either changes, and this project's own convention treats
stale docs as a defect (AGENTS.md, commit-message checklist), not a follow-up. `cobra/doc`'s
`GenMarkdownTree` walks `NewRootCmd()`'s tree and writes one Markdown page per command plus a
root index, straight from each command's `Short`/`Long`/flags — the same content `--help` shows,
just captured to a browsable file instead of a terminal.

- **`cmd/gendocs`** is a small, unshipped `main` package (`go run ./cmd/gendocs`, wired to `make
  docs`) that builds the real root command, calls the exported
  `cmd.PrepareForDocGeneration(root)`, and hands it to `GenMarkdownTree`. It never appears in a
  release: `.goreleaser.yaml`'s `builds:` names `cmd/tandem-cli` explicitly, so this second
  `main` package is simply never cross-compiled, and its own extra dependencies
  (`go-md2man`, `blackfriday`, `go.yaml.in/yaml` — pulled in transitively by `cobra/doc`, unused
  by `GenMarkdownTree` itself) never link into the actual `tandem-cli` binary either, so they are
  out of scope for THIRD-PARTY-NOTICES.md's binary-footprint table (§2) the same way `tools/`'s
  own dependencies already are.
- **`cmd.PrepareForDocGeneration`** (exported from `internal/cmd/root.go`) exists because
  `GenMarkdownTree` walks `root.Commands()` directly, bypassing the lazy
  completion-command creation `NewRootCmd()` otherwise relies on (§3, the `SetHelpFunc`/
  `SetUsageFunc` hooks) — without it, `completion` would simply be missing from the generated
  reference. It also recursively sets `DisableAutoGenTag` tree-wide, since `cobra/doc` otherwise
  stamps every page with "generated on `<date>`", which would make CI's regenerate-and-diff gate
  fail on every single run regardless of whether anything actually changed.
- **The generated pages are committed**, not gitignored, for the same reason as
  `generated.go` — readable straight from GitHub, no build step required to browse the manual.
- CI's `tandem-cli` job adds its own **regenerate-and-diff** step (`make docs && git diff
  --exit-code -- docs/cli`), alongside the existing one for the client — the drift gate applies
  identically to both generated artifacts.

---

## 9. Packaging & distribution

- **`goreleaser`** cross-compiles for **`darwin/arm64`, `linux/amd64`, `windows/amd64`** and
  produces checksums, from its **own** `cli-release.yml` workflow triggered on `cli-v*` (§9.1) —
  not a job in the library's `release.yml`, whose trigger and payload are unrelated. One
  architecture per OS, not the full `{darwin,linux,windows} × {amd64,arm64}` cross product:
  Apple Silicon for macOS, `amd64` for Linux/Windows — the platforms this CLI is actually run
  from, not every combination Go's cross-compiler happens to support. Revisit if an adopter
  needs Intel macOS or ARM Linux/Windows (§11).
- **Binaries attach to the GitHub Release**, not Maven Central (`tandem-cli` is not a JVM
  artifact — Central publication in `LLD-base.md` §1 doesn't apply to it).
- **GoReleaser builds; `gh` publishes.** GoReleaser OSS parses the current tag as plain semver
  and hard-errors on anything else (`invalid semantic version`) — honouring a prefixed tag is
  `monorepo.tag_prefix`, a **Pro-only** feature, so the `cli-v*` scheme (§9.1) cannot be handed
  to it directly. Rather than give up the tag scheme (which is what actually keeps the two
  release paths from colliding), the workflow strips the prefix into `GORELEASER_CURRENT_TAG`
  and runs `release --skip=publish,validate`: goreleaser cross-compiles, stamps the version, and
  archives, then `gh release create` publishes those artifacts against the **real** `cli-v*` tag.
  `validate` is skipped because the semver it is told about is deliberately not a tag that
  exists here. The release is created with `--latest=false`: "Latest" is one repo-wide badge and
  belongs to the library's own newest release, not to a CLI release that happens to be more
  recent.
- **No package-manager distribution in v1** (no Homebrew tap, no `apt`/`scoop` recipe) — a raw
  binary download plus `go install` (when a Go toolchain is already present) covers the common
  case; revisit if adoption asks for it (§11).
- **The copyright/license/no-warranty notice (short form; `LICENSE` carries the full text)
  prints as part of `tandem-cli --help`'s `Long` description, not `--version`.** A binary a
  user downloaded and is running standalone, with no repository context in view, is exactly
  where that notice needs to be self-contained rather than only living in a file they may
  never open — but `--version` is what a script runs to check compatibility (§9.1) or what
  gets pasted into a bug report, and a script parsing that output has no use for four lines of
  legal text on every single invocation. `--version` prints just the version line
  (`{{.Version}}`, via `SetVersionTemplate`); the notice lives where a human actually reads it,
  the first screen they see.

### 9.1 Versioning — independent of the library

**`tandem-cli` carries its own version and its own release cadence**, tagged `cli-v<semver>`
(e.g. `cli-v0.1.0`). It is never bumped to match a library release, and a library release never
implies a CLI one.

**Why independent, and not "one version for the whole project":** the CLI's compatibility
contract is the **Admin API's major version** (`/v1`), not the library's version. A Tandem
release that leaves the OpenAPI untouched cannot affect the CLI at all; conversely the CLI can
gain flags, output modes, or better errors with no library change whatsoever. A shared version
number would assert a coupling that does not exist — an operator seeing `tandem-cli 0.6.0`
beside `tandem 0.6.0` would reasonably infer the two must be upgraded together, and be wrong.
Independent versioning states the real relationship: **the CLI tracks the contract, not the
implementation behind it.**

**Tag separation is mechanical, not conventional.** The library's release workflow triggers on
`v*`, which does not match `cli-v*` (the glob anchors at the start), so a CLI tag never fires
the Maven Central publication and a library tag never fires `goreleaser`. Neither workflow needs
a guard against the other.

**What the CLI's semver governs** — its own user-facing surface, so "breaking" is well defined:

| Part of the CLI | In the semver contract? |
|---|---|
| Command / subcommand names, flag names and semantics (§3, §4) | **Yes** — renaming or removing either is breaking |
| **Exit codes** (§7) | **Yes** — scripts branch on them; changing a code's meaning is breaking, adding a new one for a new failure mode is additive |
| `--output json` payloads (§6) | **No** — raw passthrough of the API's own response shapes, so their stability is the *contract's* promise (additive within `/v1`), not the CLI's to make |
| Human (`--output human`) rendering | **No** — for people, not scripts; column layout and wording may change in any release |

**The CLI declares the contract it speaks, not the library it was built beside.**
`tandem-cli --version` reports its own version *and* the Admin API major version it targets, so
an operator can tell at a glance whether a binary can drive a given deployment. Supporting a
future `/v2` is a CLI major bump, since the endpoints it calls change underneath the same
commands.

---

## 10. Testing

Same classical, no-mocks philosophy as the rest of the project (AGENTS.md Testing §2), translated
to Go: stand up a real `httptest.NewServer` returning canned Admin API responses (fixtures under
`testdata/`) rather than mocking the generated client's interface — the Go equivalent of using
`InMemoryOutbox`/`TandemTestContainer` instead of a mock. Each cobra command is exercised end to
end (`cmd.Execute()` against the `httptest` base URL), asserting stdout, stderr, and the exit
code from §7 — the CLI-level analogue of `tandem-admin`'s MockMvc conformance tests.

**Deferred (not this round):** a real integration test running the built `tandem-cli` binary
against a live `tandem-admin` instance (Testcontainers-style). Blocked on `tandem-admin` having a
runnable standalone distribution to boot in a test — a gap this project doesn't currently close
for `tandem-admin` any more than it does for `tandem-relay` (backlog item 6 notes the same
"runnable distribution" work is still open there). Revisit once that exists.

---

## 11. Decisions

### 11.1 Resolved

- **Language: Go**, over Rust/Kotlin+GraalVM/Python — §1.
- **Client binding: generated (`oapi-codegen`) client + hand-written `cobra` command layer** —
  §8. Explicitly not the same choice as `tandem-admin`'s server (HLD-admin-api §6.1); the
  reasoning that rejected codegen there (Java-convention friction) doesn't transfer to a Go
  client.
- **User manual: generated (`cobra/doc`) from the live command tree**, not hand-written — §8.1.
  Same drift concern and same fix as the client binding above; `cmd/gendocs` is a dev-only tool,
  never cross-compiled into a release binary.
- **Command tree: resource-based, one subcommand per `operationId`**, flag names mirroring the
  contract's field names — §3. One exception: `relay buckets [<bucket>]` serves both
  `getRelayBuckets` and `getRelayBucket`, since two names a letter apart (`buckets`/`bucket`)
  invite a typo that runs the wrong command.
- **`outbox summary --watch`: a live bar-chart dashboard, scoped to this one command** — §3.1.
  Not a general pattern applied to every command; `summary`'s five counts are the one response
  shape with a natural shared scale worth watching refresh. A mid-loop request failure is shown
  inline and the loop keeps going rather than exiting, since that would defeat the point.
- **Auth: no credential is required** — the CLI sends unauthenticated when none is given and lets
  the server decide, because authentication is the host's to wire, not the library's or the
  CLI's; `--header` is the escape hatch for schemes the contract doesn't declare. Env var + flag,
  no config file/profiles in v1 — §4.
- **TLS: `--ca-cert` for a private CA, `--insecure` (warning on stderr) to skip verification** —
  the standalone Admin API is expected on an internal network — §4.
- **Confirmation gating: `discard` and non-dry-run `replay-bulk` only; `pause`/`resume`/
  `release-bucket` ungated** (reversible-within-a-cycle) — §5. `discard` additionally requires
  `--reason`, deliberately stricter than the contract, which cannot make an optional field
  required without a breaking change.
- **Output: `human` default (tabwriter for `key: value`, a hand-rolled ANSI-aware renderer for
  tables), `--output json` raw passthrough, cursor pagination never auto-followed** — §6.
- **Color scoped to the field that signals severity, never a whole row, gated on a real terminal
  and `NO_COLOR`** — `relay status` state and `relay buckets` covered/paused — §6.
- **Exit codes keyed off `ProblemDetail.type` slug, not raw HTTP status** — §7.
- **Distribution: `goreleaser` + GitHub Release binaries; no Maven Central, no package manager in
  v1** — §9.
- **Versioning: independent of the library**, tagged `cli-v<semver>` from its own workflow — the
  CLI's compatibility contract is the Admin API's major version, not the library's version
  (§9.1).
- **Module location: top-level `tandem-cli/` directory, outside the Gradle build**, with a
  pointer entry in both README's module table and CONTRIBUTING's project layout;
  THIRD-PARTY-NOTICES.md **does** apply, scoped to the statically-linked binary's dependency
  footprint — §2.

### 11.2 Open

- **Config-file / multi-profile support** — deferred until an adopter needs more than one
  Admin API endpoint per invocation context (§4).
- **Auto-pagination (`--all`)** — deferred; current design deliberately mirrors the API's own
  forward-only, no-jump pagination stance (§6).
- **Integration test against a live `tandem-admin`** — blocked on a runnable `tandem-admin`
  distribution existing at all (§10).
- **Package-manager distribution** (Homebrew, etc.) — deferred to real adoption signal (§9).
