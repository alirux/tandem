# Tandem — LLD: Spring write-side ergonomics (`tandem-spring-producer`)

**Version:** 1.0
**Status:** Implemented and released — all three tiers built and tested (Boot 3.x and 4.x)
**Companion to:** [HLD.md](HLD.md) §3.1; [LLD-spring-config.md](LLD-spring-config.md) (module + autoconfig foundation); [LLD-core.md](LLD-core.md) §2 (ports)

Resolves **Q22**. Specifies the three write-side *convenience tiers* the Spring producer adds on top
of the plain `OutboxRepository`: the **Template**, the **`@TransactionalOutbox` annotation**, and the
**Spring application-events** tier — plus the optional object-payload serialization they share. The
*Plain* tier (inject `OutboxRepository`, call `.insert()` inside `@Transactional`) needs nothing beyond
the autoconfiguration already specified in [LLD-spring-config.md](LLD-spring-config.md) §4.3 and is not
repeated here.

**Out of scope.** Micrometer-Tracing wiring is part of the tracing increment ([HLD-tracing.md](HLD-tracing.md)
§8): trace capture happens at the `OutboxRepository.insert` chokepoint, so all three tiers inherit it
transparently and it needs no per-tier design. This LLD only cross-references it.

---

## 1. What HLD §3.1 already fixes (not re-decided here)

Carried in from the HLD as constraints, not re-opened:

- **Four tiers** exist, from lowest to highest abstraction (Plain, Template, Annotation, Spring events).
- **`@TransactionalOutbox` is a composed annotation** meta-annotated with `@Transactional`, exposing
  every `@Transactional` attribute via `@AliasFor`. A transaction is always present; the user never
  writes both annotations.
- **Annotation-tier extraction is via `TandemAggregate`** — the `tandem-core` interface
  (`Collection<OutboxMessage> pendingOutboxMessages()`) the returned aggregate implements.
- **The Spring-events listener is synchronous** (`@EventListener`, inline in the publisher's thread and
  transaction) — **never** `@TransactionalEventListener(AFTER_COMMIT)`, which would insert in a
  separate transaction and break atomicity.
- **`seq` always comes from the aggregate's `version`** (HLD §4.2). Tandem reads it, never invents it —
  every tier below takes `seq` from the caller/aggregate and passes it through unchanged.

This LLD pins the **signatures and mechanics** that realise those decisions.

---

## 2. The shared serialization model (optional, Jackson, never forced)

The higher tiers let the caller hand Tandem a **payload object** instead of pre-serialized `byte[]`.
That convenience rests on the `PayloadSerializer` core port (Object → bytes + a content type), and it
must not violate the minimal-client-footprint invariant (HLD §1.3): **no JSON library is forced onto
the classpath.**

- **A Jackson `PayloadSerializer` is auto-configured only when Jackson is present** —
  `@ConditionalOnClass(ObjectMapper)` `@ConditionalOnMissingBean(PayloadSerializer)`. It reuses the
  application's `ObjectMapper` bean when one exists, else constructs a plain one; its `contentType()`
  is `application/json`. An application on a different format supplies its own `PayloadSerializer` bean
  and the conditional backs off.
- **Object payloads require a serializer; the absence is loud, not silent.** When a tier is handed an
  object payload and **no** `PayloadSerializer` bean exists, it fails fast at the call site with a
  `PayloadSerializationException` naming the three ways out — add a JSON library, supply a
  `PayloadSerializer` bean, or record a pre-built `OutboxMessage`. It never guesses an encoding.
- **`byte[]` always works with zero dependencies.** Every tier also accepts a pre-built `OutboxMessage`
  (or raw bytes), so the whole footprint-free path stays open — the serializer is a convenience, not a
  requirement.

Serialization always produces a `byte[]` that is stored in `OutboxMessage.payload`, and the serializer's
`contentType()` is written to `headers["content-type"]` (the same slot the relay reads, HLD §6). The core
`OutboxMessage` model is unchanged — it stays bytes-carrying; object payloads are serialized *before* the
message is built, never stored as objects.

---

## 3. Tier 2 — `TransactionalOutboxTemplate` (collector)

The template wraps *transaction + collect + insert* in one call. The unit of work receives an
**`OutboxCollector`** and records what to emit; the template owns the transaction and inserts everything
collected within it, after the work returns and before commit.

```java
public interface OutboxCollector {
    /** Full control: a pre-built message (footprint-free path). */
    void add(OutboxMessage message);

    /** Object payload: serialized via the configured PayloadSerializer, then built into an OutboxMessage.
     *  seq MUST be the aggregate's version (HLD §4.2). */
    void record(String aggregateType, AggregateId aggregateId, long seq, Object payload);

    /** As record(...), with a String aggregate id. */
    void record(String aggregateType, String aggregateId, long seq, Object payload);
}

public interface TransactionalOutboxTemplate {
    <T> T execute(Function<OutboxCollector, T> work);
    default void executeWithoutResult(Consumer<OutboxCollector> work) { execute(c -> { work.accept(c); return null; }); }
}
```

Usage:

```java
Order order = outboxTemplate.execute(outbox -> {
    Order o = orderRepository.findById(id);
    o.place();                                             // domain logic: version++
    orderRepository.save(o);                               // business state
    outbox.record("Order", o.id(), o.version(), new OrderPlaced(o));  // POJO payload
    return o;                                              // Order stays a plain domain type
});
```

**Mechanics.**

- The template owns the transaction **programmatically** — it wraps a Spring `TransactionTemplate`
  built from the application's `PlatformTransactionManager`. `execute` opens a transaction, runs `work`,
  then calls `OutboxRepository.insertAll(collected)` **in that same transaction**, then commits. A
  runtime exception from `work` (or from the insert) rolls the whole thing back — business state and
  outbox rows are atomic by construction.
- Insert order is **collection order**: `insertAll` preserves the order the work called `record`/`add`,
  which is the order the caller intends for a given aggregate's `seq` sequence.
- The collector is **not** thread-safe and is valid only for the duration of the `execute` call;
  escaping it is a programming error (documented, not defended against at runtime).

**Why a collector, not a `Supplier<TandemAggregate>` (Q22 decision).** The template has no reason to
constrain the return type, so keeping the domain object free of Tandem — and putting the object-payload
entry point (`record`) on a collector that owns the serializer — is cleaner than making every aggregate
implement `TandemAggregate` and carry a pending-messages list. The annotation tier (§4) *does* use
`TandemAggregate`, because there the aggregate **is** the return value; the two tiers legitimately have
different attachment points.

---

## 4. Tier 3 — `@TransactionalOutbox` annotation

```java
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Transactional                      // composed: a transaction is always present
public @interface TransactionalOutbox {

    /** Optional guard: if set, every extracted message must carry this aggregateType (fail-fast on
     *  mismatch). Left empty, extraction is unconstrained — the messages carry their own type. */
    String aggregateType() default "";

    // --- all aliased to @Transactional, so the user keeps full transaction control ---
    @AliasFor(annotation = Transactional.class, attribute = "propagation")   Propagation propagation() default Propagation.REQUIRED;
    @AliasFor(annotation = Transactional.class, attribute = "isolation")     Isolation isolation() default Isolation.DEFAULT;
    @AliasFor(annotation = Transactional.class, attribute = "timeout")       int timeout() default -1;
    @AliasFor(annotation = Transactional.class, attribute = "readOnly")      boolean readOnly() default false;
    @AliasFor(annotation = Transactional.class, attribute = "rollbackFor")   Class<? extends Throwable>[] rollbackFor() default {};
    @AliasFor(annotation = Transactional.class, attribute = "noRollbackFor") Class<? extends Throwable>[] noRollbackFor() default {};
}
```

Usage:

```java
@TransactionalOutbox                                 // = @Transactional + outbox extraction
public Order place(OrderId id) {
    Order o = orderRepository.findById(id);
    o.place();                                        // records its own event + seq = version
    return orderRepository.save(o);                   // Order implements TandemAggregate
}
```

**Extraction.** After the method body returns (still inside the transaction), an AOP aspect reads the
return value:

- a `TandemAggregate` → its `pendingOutboxMessages()` are inserted;
- an `Iterable<? extends TandemAggregate>` → each element's messages are inserted, in iteration order;
- anything else (including `void`/`null`) → **nothing is extracted** (the method opted into the
  transaction but produced no aggregate; this is allowed, not an error);
- if `aggregateType` is set on the annotation, every extracted message is asserted to carry it —
  a mismatch fails fast (`OutboxInsertException`), turning the attribute into a real guard rather than
  decoration.

Because `TandemAggregate.pendingOutboxMessages()` returns fully-built `OutboxMessage`s, the annotation
tier is **byte-oriented**: the aggregate builds its own messages (it may inject a `PayloadSerializer` to
do so). The object-payload convenience of §2/§3 lives on the collector, not here — an aggregate that
wants it uses the Template tier instead.

**Atomicity and advice ordering (the one subtlety).** The extraction+insert must run **inside** the
transaction the composed `@Transactional` opened, before commit. Concretely:

1. the aspect is an `@Around` advisor that `proceed()`s the method, then extracts and inserts;
2. it must be **inner** to Spring's transaction advisor (which by default sits at
   `Ordered.LOWEST_PRECEDENCE`), so that when control returns from `proceed()` the transaction is still
   open. No explicit `@Order` is declared — Spring's default ordering already places the aspect there
   (§8), and the backstop below is what keeps a wrong order from ever being silent;
3. **backstop (loud, not silent):** immediately after `proceed()` the aspect asserts
   `TransactionSynchronizationManager.isActualTransactionActive()` and throws if it is not — so any
   misordering surfaces as an error, never as a non-atomic insert. The invariant is pinned by an
   integration test: a business-logic rollback must also roll back the outbox rows.

---

## 5. Tier 4 — Spring application events

The domain publishes ordinary Spring events inside its own `@Transactional` method; Tandem maps each to
an `OutboxMessage` and inserts it in the **same** transaction. This gives Spring-Modulith-style
ergonomics with Tandem's per-aggregate ordering.

```java
public interface OutboxEventMapper<T> {
    /** Map one domain event to the outbox rows it produces (0..n; usually 1). seq comes from the
     *  aggregate's version, carried on the event. */
    Collection<OutboxMessage> map(T event);
}
```

```java
@Transactional
public void place(OrderId id) {
    Order o = orderRepository.findById(id);
    o.place();
    orderRepository.save(o);
    events.publishEvent(new OrderPlaced(o));          // ordinary Spring event
}
```

**Mapping.** For each intercepted event:

- an event that already **is** an `OutboxMessage` is inserted directly;
- otherwise the registered `OutboxEventMapper<T>` whose `T` matches the event's runtime type produces
  the message(s);
- a mapper returning an empty collection is allowed (the app chose not to emit); returning `null` is a
  bug and fails fast.

**Mapper resolution.** Mappers are Spring beans. At startup the module builds a registry keyed by each
mapper's resolved `T` (via `ResolvableType` over the bean type). Two mappers registered for the **same**
`T` fail fast **at startup** — the collision is visible without an event. Lookup is otherwise by the
event's runtime class, walking to the most specific registered supertype; an event that matches two
*equally specific* supertypes is ambiguous and fails **on the first such event**, not at startup: which
mappers compete depends on the runtime type published, which the registry cannot enumerate in advance.
Both failures are loud (`OutboxInsertException`), and the per-event one rolls back the caller's
transaction rather than emitting a guessed row.

**Which events are intercepted — the mapper registration *is* the opt-in.** The listener is scoped to
**exactly** the types it can handle — `OutboxMessage` and every type with a registered mapper (realised
via a `SmartApplicationListener`/`GenericApplicationListener` whose `supportsEventType` consults the
registry). Framework events (`ContextRefreshedEvent`, …) are therefore **not** intercepted, so there is
no catch-all-`Object` listener that would fire on them. The app opts a domain event into the outbox by
**registering an `OutboxEventMapper` for it** (or publishing an `OutboxMessage` directly); a published
event with no mapper is simply not Tandem's concern and is ignored.

Two reasons this is the right model: (1) a catch-all listener that fails fast on any unmapped event is
unimplementable — it would fire and throw on every framework lifecycle event, breaking startup; and
(2) "an event with no listener does nothing" is Spring's own native event-bus semantics, so the scoped
model behaves like every other `@EventListener` rather than surprising users. Crucially this does
**not** weaken any Tandem delivery guarantee: loudness applies once a row is in the outbox (it is
delivered or loudly stalls), not to whether the application wired its Spring events — that stays
ordinary application configuration.

**The scoping works on payload events only.** The listener resolves events through Spring's
`PayloadApplicationEvent` wrapper — the one `publishEvent(Object)` creates for an ordinary POJO. A
domain event that itself extends `ApplicationEvent` is published unwrapped and is therefore **never**
intercepted, even with a mapper registered for it. Publish plain objects (the idiom this tier is built
around), or an `OutboxMessage` directly.

**Fail-fasts (atomicity, loud).**

- The listener asserts an **active transaction** (`isActualTransactionActive()`) and throws if absent —
  a synchronous listener fires even under autocommit, which would insert non-atomically. Never a silent
  insert, never a silent drop.
- Insert failures propagate as `OutboxInsertException`, rolling back the caller's transaction.

---

## 6. Autoconfiguration additions

These extend `tandem-spring-producer`'s autoconfiguration ([LLD-spring-config.md](LLD-spring-config.md)
§4.3), which already contributes the `OutboxRepository` and runs the bucket-count guard. Class declared
`@AutoConfiguration(after = { DataSourceAutoConfiguration.class, TransactionAutoConfiguration.class })`
so a `PlatformTransactionManager` is available. Every contributed bean is `@ConditionalOnMissingBean`.

| Bean | Condition | Needs |
|---|---|---|
| `PayloadSerializer` (Jackson) | `@ConditionalOnClass(ObjectMapper)` | the app's `ObjectMapper` if present |
| `TransactionalOutboxTemplate` | `@ConditionalOnBean(PlatformTransactionManager)` | `OutboxRepository`, `PlatformTransactionManager`, optional `PayloadSerializer` |
| `@TransactionalOutbox` advisor + aspect | `@ConditionalOnClass` AOP present | `OutboxRepository` |
| Spring-events listener + mapper registry | always (registry may be empty) | `OutboxRepository`, the `OutboxEventMapper` beans, optional `PayloadSerializer` |

The higher tiers are **independent** — an application uses any subset. The serializer is shared by the
Template and events tiers when object payloads are used; its absence only matters at an object-payload
call site (§2), so the tiers wire fine without it.

---

## 7. The `seq` = version invariant (all tiers)

One rule, stated once: **`seq` is the aggregate's `version` and originates in the domain.** The Template
takes it as an explicit `record(..., seq, ...)` argument; the annotation tier reads it from the messages
the aggregate built; the events tier reads it from the mapper's output (carried on the event). No tier
generates or mutates `seq`. The `UNIQUE(aggregate_id, seq)` constraint remains the safety net (HLD §4.2),
surfaced as `DuplicateSeqException` if a bug produces a collision.

> **With a JPA `@Version`, that source is stale by default — and it is not a per-tier bug, so no
> choice of tier here avoids it.** Hibernate advances `@Version` at *flush*, and every tier in this
> document runs inside the caller's already-open transaction, so `entity.getVersion()` (however it
> reaches the Template's `seq` argument, the annotation-built message, or the mapped event) is by
> default the *pre-increment* value. Two mutations in one transaction then read the same version and
> collide on `UNIQUE(aggregate_id, seq)` (`DuplicateSeqException`, aborting the business transaction);
> a mutation that dirties no persistent field advances no version at all, so the *next* transaction
> reuses the stale `seq`. Measured across all three Spring tiers plus the plain `OutboxRepository`
> path, identically: [HLD.md](HLD.md#42-ordering-established-at-write-time) §4.2,
> [HLD-managed-seq.md](HLD-managed-seq.md) §3.2. Two workarounds, neither tier-specific: build the
> outbox row after an explicit flush, or take `seq` from a source that advances per *event* rather
> than per entity state change (HLD-managed-seq.md §4 designs the latter — not yet built).

---

## 8. Compatibility & open points

- **Public API surface:** `TransactionalOutboxTemplate`, `OutboxCollector`, `@TransactionalOutbox`,
  `OutboxEventMapper<T>` (all `com.codingful.tandem.spring.producer`), plus the reuse of the
  `TandemAggregate` and `PayloadSerializer` core ports. These evolve under the project's
  additive-compatibility rule (HLD §1.4).
- **AOP advice ordering** (§4) rests on Spring's defaults rather than an explicit `@Order`: the
  active-transaction backstop plus the rollback integration test make any ordering error loud rather than
  silent, and a runtime test on both Spring generations pins that the advice intercepts at all
  (LLD-spring-config §1.2). Set an explicit order only if a real conflict with another advisor appears.
- **Micrometer-Tracing** — cross-referenced only (HLD-tracing §8); trace capture is at the insert
  chokepoint and needs no per-tier work.
- **`@TransactionalOutbox` on a non-`TandemAggregate` return** is treated as "no extraction", not an
  error (§4). If experience shows silent no-ops confuse users, a future opt-in strict mode could warn —
  deferred, not built.
- **Future: an optional `OutboxEvent` marker for loud, guaranteed events (evaluate later).** The events
  tier (§5) settled on *scoped listening* — an unmapped published event is silently ignored, matching
  Spring's native event bus. A future **additive** enhancement could reintroduce loudness *selectively*:
  an optional marker interface (e.g. `OutboxEvent`) that an application puts on the events it insists
  must reach the outbox. The listener would additionally intercept marker-implementing types, and a
  published `OutboxEvent` with **no** registered mapper would then **fail fast** instead of being ignored.
  - *Advantages:* restores the project's loud-failure posture for the events a team deems critical —
    a forgotten mapper on a marked event becomes an error, not a silent drop; purely opt-in, so the
    ordinary "publish a POJO, register a mapper" path (B) is unchanged; additive and backward-compatible
    (existing events keep their current behaviour, no marker required).
  - *Disadvantages:* adds a second opt-in signal (mapper registration *and* a marker), so "is this event
    for the outbox?" now has two answers to hold in mind; a marked event couples the domain type to a
    Tandem interface, eroding the "ordinary Spring events" appeal exactly where it is used; and the
    guarantee is still only as good as remembering to *mark* — it cannot catch an event the developer
    forgot to mark *and* forgot to map. Realising it also means the scoped `supportsEventType` must widen
    to marker-assignable types and the handler must distinguish "marked-but-unmapped" (fail) from
    "unmarked-and-unmapped" (never intercepted).
  - *Decision:* not built now; the B baseline stays the default. Revisit if real usage shows the silent
    no-op is a recurring footgun for critical events.
