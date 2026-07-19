# Tandem — Logging Strategy (Design Note)

**Version:** 1.1
**Status:** Reviewed
**Companion to:** AGENTS.md, [docs/HLD-tracing.md](HLD-tracing.md)

Give Tandem's published modules (`tandem-core`, `tandem-jdbc`, `tandem-kafka`, `tandem-test`)
and its leaf applications (`tandem-sample`, `tandem-benchmark`) a consistent, low-risk logging
posture — without breaking the **minimal client footprint** invariant (AGENTS.md, HLD §1.3).

---

## 1. Current state (baseline)

An audit of `src/main` across all modules found:

- No logging configuration exists anywhere in the repo (no `logback.xml`, `log4j2.xml`,
  `logging.level.*`).
- Only `tandem-jdbc` logs at all, via the JDK's `System.Logger` — and only at `ERROR`. No
  `INFO`/`DEBUG`/`WARN`/`TRACE` exists anywhere in the library.
- `tandem-core` and `tandem-kafka` have **zero** log statements, including inside `catch`
  blocks that swallow Kafka send failures (`KafkaRelay`) — the single most operationally
  important failure mode in the relay has no log trail today.
- Two `tandem-jdbc` `ERROR` calls drop the original `Throwable` and log only
  `exception.getMessage()`, losing the stack trace.
- `tandem-sample` and `tandem-benchmark` use `System.out.println` as their only output,
  including printing local demo credentials.
- No PII/payload logging risk was found — but only because so little is logged. There is no
  redaction utility and no rule preventing it going forward.

This note defines the target logging posture and the rules to keep it that way.

---

## 2. Principles (derived from existing architecture rules)

**2.1 — No logging framework on the client write-side (Minimal client footprint, §1.3).**
`tandem-jdbc` ships **both** the client-imported write path (`OutboxRepository.insert`) and
the relay engine in one module (`tandem-jdbc/build.gradle.kts`: *"write-side insert and the
relay engine"*). Any dependency added to `tandem-jdbc` lands on the client's classpath too.
So `tandem-core` and `tandem-jdbc` must keep using the JDK's built-in **`java.lang.System.Logger`**
(JEP 264) — zero dependency, already the current practice for `tandem-jdbc`'s `ERROR` logs.
This is a constraint to keep, not a mistake to fix.

**2.2 — Relay-only modules may follow the ecosystem norm.** `tandem-kafka` is explicitly
"Relay-side only — never on the client write-side" (its own `build.gradle.kts` comment) and
already pulls `kafka-clients`, which itself requires `slf4j-api` on the compile classpath.
Adding an SLF4J-based logger to `tandem-kafka` therefore adds **no new footprint** — the
dependency is already there transitively. `tandem-kafka` should log via **SLF4J**
(`org.slf4j.Logger`), consistent with how the wrapped Kafka client itself logs, so a consumer
sees relay and Kafka-client logs through one pipeline.

**2.3 — Leaf applications are not libraries.** `tandem-sample` and `tandem-benchmark` are
end-user-facing CLI programs, not something another project depends on. They may take a full
logging backend (both use SLF4J + `slf4j-simple`, §10) as a `runtimeOnly` dependency without
violating §1.3 — but see §8 for what should stay as plain console output versus what should be
a log line.

**2.4 — Don't impose a logging config on the consumer.** Same reasoning as "no shipped
`logback.xml`" today: a library dictating log format/destination is exactly the kind of
opinionated default the Pareto rule (§1.1) warns against — it's the consuming application's
job to route logs, not Tandem's. Keep it that way; only `tandem-sample`/`tandem-benchmark`,
as leaf apps, own a concrete logging config.

| Module | Logging API | Rationale |
|---|---|---|
| `tandem-core` | `System.Logger` (used sparingly — see §4) | Dependency-free by design (§1.2); on the write path |
| `tandem-jdbc` | `System.Logger` | Carries the client write-side; must stay dependency-free (§2.1) |
| `tandem-kafka` | SLF4J (`org.slf4j.Logger`) | Relay-only; `slf4j-api` already transitive via `kafka-clients` (§2.2) |
| `tandem-test` | `System.Logger` or none | Test-support scaffolding, not production logging |
| `tandem-sample`, `tandem-benchmark` | SLF4J + `slf4j-simple` binding | Leaf apps, free to pick a backend (§2.3); zero-config is enough since their real output is report text (§7, §10) |

---

## 3. Log level policy

| Level | Meaning in Tandem | Examples |
|---|---|---|
| `ERROR` | Unrecoverable within the current attempt; needs operator attention or will surface as a failed delivery | Kafka send failed after retries exhausted; lease table unreadable at startup; worker thread died and is restarting |
| `WARN` | Recoverable automatically, but abnormal — worth surfacing if it recurs | A single publish attempt failed and will be retried; a bucket lease reclaim raced with another instance and lost (expected under `LEASE` coordination, but noisy if constant) |
| `INFO` | Lifecycle and coordination-boundary events — low frequency, operator-relevant, one line per occurrence | Relay started/stopped; worker pool size and coordination mode (`SINGLE`/`LEASE`) resolved at startup; bucket lease acquired/released; config validated |
| `DEBUG` | Per-cycle / per-batch operational detail — useful when troubleshooting, too chatty for normal operation | A claim/dispatch/flush cycle ran and claimed N rows; a batch was sent to Kafka; heartbeat renewed for bucket X |
| `TRACE` | Per-record detail | Individual outbox row dispatched, with its id — reserve for the rare case DEBUG isn't enough |

**Rule of thumb:** if the statement runs once per relay lifecycle or once per coordination
event, it's `INFO`. If it runs once per poll cycle or per batch, it's `DEBUG`. If it runs once
per row, it's `TRACE`, not `DEBUG` — a busy outbox would otherwise flood DEBUG output and make
it useless for actual troubleshooting.

Today's gap is not "wrong balance," it's **absence**: `tandem-jdbc`'s `WorkerPool` has no
`INFO` on start/stop, and no `DEBUG` on the claim/dispatch cycle. Close that gap per §9 before
tuning it further.

---

## 4. Message content rules

1. **Always pass the `Throwable` to the logger, never just `.getMessage()`.** Use
   `logger.log(Level.ERROR, "message", throwable)` (`System.Logger`) or
   `logger.error("message", throwable)` (SLF4J) so the stack trace survives. Concatenating
   `e.getMessage()` into the log string and dropping the exception object is the single most
   common way to make an `ERROR` log undebuggable — fix the two existing instances of this in
   `BucketLeaseManager`/`RelayConfig` (§9).
2. **Every `ERROR`/`WARN` includes the identifiers needed to find the affected record** without
   re-running the failing operation: bucket id, worker index, aggregate id, Kafka topic/
   partition, outbox row id. A bare `"cleanup failed"` with no id is nearly as useless as no
   log at all once there is more than one relay instance or worker.
3. **Fixed message text first, then runtime data as a flat, machine-parseable tail** — never
   interleave a variable into the middle of the sentence. The message is a constant string
   (good for grepping/aggregating identical failures across occurrences); everything that
   varies goes **after** it, as `name:value` pairs separated by `, `, in the same order every
   time. E.g. `"Relay worker died; restarting workerIndex:3"`, not `"Relay worker 3 died;
   restarting"`. This applies whether the data comes from SLF4J `{}` placeholders (`tandem-kafka`)
   or `System.Logger` string concatenation (`tandem-jdbc`) — the placeholder/concatenation only
   changes *how* the value is inserted, never *where*: still after the fixed text, still
   `name:value`. Never wrap the tail in brackets and never use `=` — just `name:value` pairs.
4. **Prefer parameterized logging over string concatenation** where the API supports it —
   SLF4J's `{}` placeholders (`tandem-kafka`) so the message is only formatted when the level
   is enabled. `System.Logger` has no `{}` form; string concatenation is acceptable there since
   volume is low (`ERROR`/`INFO`), but avoid concatenation-heavy `DEBUG` statements — guard
   them with `if (logger.isLoggable(Level.DEBUG))` or build the message lazily so a disabled
   `DEBUG` level costs nothing on the hot claim/dispatch path.
5. **Every log message starts with a capital letter** ("Relay worker died; restarting", not
   "relay worker died; restarting"), consistent regardless of level or logging API. Purely a
   style rule — no functional reason, just consistency across `System.Logger` and SLF4J call
   sites.
6. **`FATAL` is not used.** Neither `System.Logger` nor SLF4J define it as a distinct level;
   an unrecoverable startup failure is an `ERROR` plus a thrown exception that stops the
   process, not a separate log level.

---

## 5. Sensitive data — what may and may not be logged

Tandem's own surface area for sensitive data is narrow (it moves outbox rows and Kafka
messages, not user sessions), but the rule must be explicit so it doesn't erode as logging is
added.

| Never log | Always safe to log |
|---|---|
| Full message/event **payload bodies** (`data` field) — may contain business PII the relay has no business inspecting | Structural identifiers: outbox row id, aggregate id/type, bucket id, worker index, Kafka topic/partition/offset |
| Credentials, JDBC URLs with embedded passwords, tokens, secrets | Counts and timings: batch size, rows claimed, lease duration, retry attempt number |
| Full header maps if headers might carry app-defined sensitive extensions | The `correlation-id`/`trace_id` values themselves (opaque ids, not PII by design — see HLD-tracing.md) |
| Bound SQL parameter values in query-failure logs (log the statement shape/name, not the values) | The error `type` slug / exception class name |

`tandem-sample`'s current printing of its own Testcontainers-generated demo credentials
(`SampleApplication.java`) is out of scope for this rule — it's a local, ephemeral,
throwaway credential meant to be copy-pasted by the developer running the demo — but must
stay a `System.out` **console message**, clearly not routed through the logging pipeline,
and never become a pattern copied into `tandem-jdbc`/`tandem-kafka`.

**`toString()` on `tandem-core` types is part of the log surface.** Anyone who logs
`"..." + record` or `"..." + record.message()` gets whatever `toString()` produces, so the same
rule applies there, not just to explicit log statements. `OutboxMessage.toString()` prints
`payloadBytes=<length>`, never the payload bytes, and `headerNames=<keySet()>`, never the header
*values* (a header map is app-defined and could otherwise carry something sensitive).
`OutboxRecord.toString()` (added — it previously had none, so it fell back to
`Object.toString()`, useless for debugging though not a leak) prints only `id`, `aggregateType`,
`aggregateId`, `seq`, `status`, `attempts` — structural identifiers per the table above, never
`payload()`/`headers()`/`lastError()` (the latter excluded defensively even though it is
currently just a classifier-derived message, not raw business data).

---

## 6. Correlation with tracing (ties to HLD-tracing.md §8)

When trace/correlation propagation (HLD-tracing.md) is enabled, the captured `correlation-id`
and `traceparent` should be restored into the relay's logging context (SLF4J MDC in
`tandem-kafka`) around the publish call, so a log line for a given send failure can be
correlated with the originating business operation without any Tandem-specific log parsing.
This is opt-in and follows the same "zero cost when off" rule as propagation itself (HLD-tracing.md
§7) — no MDC population happens unless propagation is enabled.

---

## 7. Diagnostic logging vs. CLI/report output

`tandem-benchmark`'s scenario pass/fail summary and `tandem-sample`'s walkthrough narration are
**user-facing report output**, not operational logs — a load-test result table is a *product*
of the tool, meant to be read on a terminal or piped to a file, not filtered by log level. Keep
these as `System.out.println`/`printf` deliberately; do not migrate them to a logger. Reserve
the logging framework in these two modules for genuine diagnostics: connection failures,
retries, anything an operator would want to filter by level or route to a file separately from
the report itself.

---

## 8. Consumer guidance (documentation, not code)

Since Tandem ships no logging configuration (§2.4), the README/troubleshooting docs should
tell a consumer how to see Tandem's logs in their own stack:

- `tandem-jdbc`/`tandem-core` log via `System.Logger` (`java.lang.System.Logger`, JEP 264), which
  is redirectable to any backend **purely through the consumer's own dependencies/config** — no
  code or config on Tandem's side:
  - **Direct route (recommended):** add `org.slf4j:slf4j-jdk-platform-logging` as a runtime
    dependency. It self-registers as a `System.LoggerFinder` via `ServiceLoader` — no code, no
    properties file — and every `System.Logger` call (Tandem's included) is routed straight to
    SLF4J/whatever backend the app already has bound.
  - **Indirect route:** if no custom `LoggerFinder` is registered, `System.Logger` falls back to
    the JDK's built-in `java.util.logging`-backed default. Add `org.slf4j:jul-to-slf4j` and set
    `handlers=org.slf4j.bridge.SLF4JBridgeHandler` in `logging.properties` (or call
    `SLF4JBridgeHandler.install()`) to redirect from there — one extra hop through JUL, but
    config-only.
  - **Caveat:** `System.LoggerFinder` is a JVM-wide singleton selected via `ServiceLoader` —
    if more than one bridge (e.g. SLF4J's and Log4j2's `log4j-jpl`) ends up on the classpath,
    which one wins is undefined. The consuming app must keep exactly one on the classpath.
- `tandem-kafka` logs via SLF4J directly → picked up by whatever SLF4J binding the consumer
  already has for the Kafka client itself; no extra bridge needed.
- To see `DEBUG`-level relay tracing, the consumer sets their backend's level for the
  `com.codingful.tandem.jdbc` / `com.codingful.tandem.kafka` logger names.

This is documentation work, not a code change — track it alongside §9 item 6.

---

## 9. Action plan (priority order)

1. ~~**`tandem-kafka`: log at the Kafka send-failure catch sites** (`KafkaRelay`) at `ERROR`,
   with the `Throwable`, topic, and the outbox row id — currently silent.~~ **Done.**
2. ~~**Fix the two exception-swallowing `ERROR` logs** in `BucketLeaseManager` and `RelayConfig`
   to pass the `Throwable` instead of `e.getMessage()`.~~ **Done** (`RelayConfig.checkRowLeaseSafe`
   had no caught exception to begin with — nothing to fix there; the two `BucketLeaseManager`
   catches now pass their `SQLException`).
3. ~~**Add `INFO` lifecycle logging to `WorkerPool`**: start (worker count, coordination mode),
   clean stop, bucket lease acquired/released.~~ **Done** for start/stop; per-lease
   acquire/release stays at the existing `heartbeat()`/`release()` call sites, not logged
   individually (would be per-cycle noise at `INFO`, see item 4 for the `DEBUG` equivalent).
4. ~~**Add `DEBUG` cycle logging to the claim/dispatch/flush path** in `tandem-jdbc`, guarded so
   it costs nothing when disabled.~~ **Done** for claim count and lease-reclaim count; `flushDone`
   itself stays unlogged (its count is implicit in the claim count already logged).
5. **Introduce SLF4J in `tandem-kafka`** (`build.gradle.kts`: promote the already-transitive
   `slf4j-api` to an explicit `api`/`implementation` dependency) and log around
   publish/ack/retry. **Done** for `KafkaRelay`'s dispatch path (encode/send failures); no
   retry logic exists in `tandem-kafka` itself (retry is `tandem-jdbc`'s `RelayWorker`, which
   stays on `System.Logger`).
6. ~~**Write the consumer-facing "how to see Tandem's logs" doc** (§8) — README or a
   troubleshooting page.~~ **Done** — README "Logging" section: per-module logging API table,
   the `slf4j-jdk-platform-logging` bridge as the recommended route, the `jul-to-slf4j`
   fallback, the `LoggerFinder`-singleton caveat, and the logger names to set levels on. The
   README carries the consumer-facing subset; this document stays the full policy and is now
   linked from the README's Documentation table.
7. **Audit new logging as it's added against §5** — no payload bodies, no credentials. Ongoing.

**Plan closed 2026-07-19.** Items 1–6 are done; item 7 is a standing rule, not a task — every
new log statement is reviewed against §5 as it is written. Further logging work is now driven by
the features that need it (trace/MDC correlation with HLD-tracing.md §8, and whatever
`tandem-spring`/`tandem-admin` add), not by this plan.

---

## 10. Decisions

Resolved 2026-07-19; each was previously an open question with a recommendation, now ratified.

| Area | Decision | Rationale |
|---|---|---|
| `tandem-core` logging | **Keep silent.** No logging API in `tandem-core` (nor `tandem-test`). | It has no I/O to report and its errors already surface as exceptions to the caller; adding a logger there buys nothing and puts a log surface on the write path. Revisit only if a concrete diagnostic need appears. |
| MDC population scope | **Narrow** — populate MDC only around the publish call in `tandem-kafka`, tied to the row being sent, and only when trace propagation is enabled. | Broader per-cycle population is cheaper to wire but carries stale-context risk across rows, which is exactly the failure mode that makes correlated logs untrustworthy. Forward-looking: no MDC code exists yet — trace propagation (HLD-tracing.md) is unimplemented, so this binds the implementation when it lands. |
| `tandem-jdbc` future SLF4J migration | **Stay on `System.Logger`.** | `tandem-jdbc` ships the client-imported write path and the relay engine in one jar, so any dependency added there lands on the client's classpath (§2.1). Do not split the module just for logging; if it is ever split for another reason, revisit then. Consumers bridge to their own backend with zero Tandem-side change (§8). |
| Sample/benchmark logging backend | **`slf4j-simple`** — already in place in both modules (`tandem-sample/build.gradle.kts`, `tandem-benchmark/build.gradle.kts`), ratified rather than changed. | The earlier recommendation of Logback assumed these modules were on `slf4j-nop`; they are not. Their real deliverable is `System.out` report output (§7) and the logger only carries incidental diagnostics, so a zero-config backend is the right weight — Logback would add a `logback.xml` per module to maintain for no gain. Both must keep tracking `tandem-kafka`'s slf4j-api major version (a 1.x binding is not loadable against a 2.x provider). |
