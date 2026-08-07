package com.codingful.tandem.spring.relay;

import com.codingful.tandem.core.TandemHeaders;
import com.codingful.tandem.core.port.TandemSpanRecorder;
import io.micrometer.tracing.propagation.Propagator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * The relay-side {@link TandemSpanRecorder} for applications running Micrometer Tracing — instrumented
 * mode (HLD-tracing.md §6). Emits one {@code tandem.relay.publish} span per outbox record, parented to
 * the trace context captured on <i>that</i> record at insert time, so the trace shows the outbox dwell
 * and the real send instant instead of an unexplained gap between the write and the consumer.
 *
 * <p>The parent comes from the row, never from the calling thread: the relay claims rows in batches
 * whose members belong to unrelated business transactions (§6.1), and it dispatches asynchronously, so
 * no ambient context on the worker thread could be correct. The span is started here and ended from the
 * dispatch completion callback — never inside a thread-local scope, which would close when the dispatch
 * call returns rather than when the send completes (§6.2).
 *
 * <p><b>A row carrying no trace context gets no span.</b> Starting one anyway would make the relay the
 * root of a brand-new trace holding a single publish span and nothing about the business operation that
 * produced the event — the same orphan-span outcome §9 rejected when it decided the relay must never
 * force-sample. Instrumented mode therefore builds on propagation mode: with capture off at the write
 * side, or on a row whose trace was not sampled, this recorder is silent by design.
 *
 * <p>Attributes are structural identifiers only (§6.4) and are named uniformly under {@code tandem.*} —
 * never the payload, a header value, or an error body.
 */
public final class MicrometerTandemSpanRecorder implements TandemSpanRecorder {

    /** The span name, fixed so it aggregates across occurrences in the tracing backend. */
    static final String PUBLISH_SPAN_NAME = "tandem.relay.publish";

    private final Propagator propagator;

    /**
     * @param propagator reads the row's captured context back into a span parent, in the same format
     *                   the write side captured it, and builds the span through the application's own
     *                   tracer — which is why no {@code Tracer} is needed here
     * @throws NullPointerException if {@code propagator} is {@code null}
     */
    public MicrometerTandemSpanRecorder(Propagator propagator) {
        this.propagator = Objects.requireNonNull(propagator, "propagator");
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public TandemSpanRecorder.Span startPublishSpan(long rowId, String aggregateType, String aggregateId,
            int attempts, String topic, String traceparent, String tracestate, String correlationId) {
        if (traceparent == null) {
            return TandemSpanRecorder.Span.NOOP;
        }
        Map<String, String> carrier = new HashMap<>(2);
        carrier.put(TandemHeaders.TRACEPARENT, traceparent);
        if (tracestate != null) {
            carrier.put(TandemHeaders.TRACESTATE, tracestate);
        }
        // Fully qualified throughout: the inherited TandemSpanRecorder.Span shadows any import of
        // Micrometer's same-named type, so an import here would silently resolve to the wrong Span.
        io.micrometer.tracing.Span.Builder builder = propagator.extract(carrier, Map::get)
                .name(PUBLISH_SPAN_NAME)
                .kind(io.micrometer.tracing.Span.Kind.PRODUCER)
                .tag("tandem.outbox.row_id", rowId)
                .tag("tandem.aggregate.type", aggregateType)
                .tag("tandem.aggregate.id", aggregateId)
                .tag("tandem.attempts", attempts)
                .tag("tandem.topic", topic);
        if (correlationId != null) {
            builder.tag("tandem.correlation_id", correlationId);
        }
        io.micrometer.tracing.Span span = builder.start();
        return new TandemSpanRecorder.Span() {

            @Override
            public void end() {
                span.end();
            }

            @Override
            public void end(Throwable failure) {
                span.error(failure).end();
            }
        };
    }
}
