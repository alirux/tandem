package com.codingful.tandem.core.port;

/**
 * Optional relay-side span-emission port (LLD-core §2.5, HLD-tracing.md §5–§6, §9's "instrumented
 * mode"). Emits {@code tandem.relay.publish} — one span per outbox record, parented to the
 * {@code traceparent}/{@code tracestate} captured on that record, never a batch-wide span (§6.1). The
 * default is a no-op: {@link #isEnabled()} returns {@code false} and callers guard on it so
 * propagation mode (no relay-side span) costs nothing. A real adapter ships in
 * {@code tandem-spring-relay} / {@code tandem-tracing-otel}.
 *
 * <p>Deliberately explicit parameters, not a generic header map: an adapter is structurally unable to
 * attach the payload or an arbitrary header value to a span, only the identifiers listed here (§6.4).
 * {@code bucket} and {@code partition}/{@code offset} are not included in this first cut — the former
 * is not available at {@link OutboxDispatcher#dispatch}'s call site today, the latter only after the
 * send completes — both are additive to add later.
 */
public interface TandemSpanRecorder {

    /** A no-op recorder — the default when instrumented mode is off. */
    TandemSpanRecorder NOOP = new TandemSpanRecorder() {
    };

    /** {@code true} once a real adapter is wired; callers guard on this so the off-path costs nothing. */
    default boolean isEnabled() {
        return false;
    }

    /**
     * Starts the {@code tandem.relay.publish} span for one outbox record. The relay dispatches
     * asynchronously and does not await per record (§6.2), so the returned handle must be carried
     * explicitly alongside the in-flight record and ended from the send's completion callback — never
     * via a thread-local scope, which would close at the wrong time.
     *
     * @param rowId         the outbox row id
     * @param aggregateType the record's aggregate type
     * @param aggregateId   the record's aggregate id
     * @param attempts      delivery attempts so far, before this one
     * @param topic         the resolved destination topic
     * @param traceparent   the row's captured W3C trace parent; {@code null} when absent (untraced or
     *                      unsampled row)
     * @param tracestate    the row's captured W3C trace state; {@code null} when absent
     * @param correlationId the row's correlation id; {@code null} when absent. Carried as a span
     *                      attribute so an investigation that starts in the tracing backend can cross
     *                      over to the Admin API's search by the same id — and the reverse. It is a
     *                      structural identifier, safe to export by the same rule that already allows
     *                      it in logs (§6.4, HLD-logging.md)
     * @return a handle to end exactly once when the send completes; {@link Span#NOOP} when disabled
     */
    default Span startPublishSpan(long rowId, String aggregateType, String aggregateId, int attempts,
            String topic, String traceparent, String tracestate, String correlationId) {
        return Span.NOOP;
    }

    /** A single in-flight span handle (§6.2) — ended exactly once, from the dispatch completion callback. */
    interface Span {

        /** A no-op handle — {@link #end()}/{@link #end(Throwable)} do nothing. */
        Span NOOP = new Span() {
        };

        /** Ends the span successfully. */
        default void end() {
        }

        /** Ends the span, recording {@code failure} — never the message body it may reference (§6.4). */
        default void end(Throwable failure) {
        }
    }
}
