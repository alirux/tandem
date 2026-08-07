package com.codingful.tandem.tracing.otel;

import com.codingful.tandem.core.TandemHeaders;
import com.codingful.tandem.core.TandemSpanAttributes;
import com.codingful.tandem.core.port.TandemSpanRecorder;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * The OpenTelemetry relay-side {@link TandemSpanRecorder} — instrumented mode (HLD-tracing.md §6).
 * Emits one {@code tandem.relay.publish} span per outbox record, parented to the trace context
 * captured on <i>that</i> record at insert time, so the trace shows the outbox dwell and the real send
 * instant instead of an unexplained gap between the write and the consumer. The Micrometer-based
 * equivalent for Spring applications is {@code MicrometerTandemSpanRecorder}.
 *
 * <p>The parent comes from the row, never from the calling thread: the relay claims rows in batches
 * whose members belong to unrelated business transactions (§6.1), and it dispatches asynchronously, so
 * no ambient context on the worker thread could be correct. For the same reason the span is started
 * here and ended from the dispatch completion callback, never inside a {@code Scope} made current on
 * this thread — that would close when the dispatch call returns rather than when the send completes
 * (§6.2).
 *
 * <p><b>A row whose context this application's propagator cannot read gets no span</b> — one carrying
 * no {@code traceparent} at all, or one written in a format the configured propagator does not extract.
 * Starting a span anyway would make the relay the root of a brand-new trace holding a single publish
 * and nothing about the business operation that produced the event — the same orphan outcome §9
 * rejected when it decided the relay must never force-sample. Instrumented mode therefore builds on
 * propagation mode.
 *
 * <p>Attributes are the structural identifiers of {@link TandemSpanAttributes} only (§6.4) — never the
 * payload, a header value or an error body.
 */
public final class OtelTandemSpanRecorder implements TandemSpanRecorder {

    /** Identifies Tandem as the instrumentation that emitted the span, per OpenTelemetry convention. */
    static final String INSTRUMENTATION_SCOPE = "com.codingful.tandem";

    /**
     * Reads the two-entry carrier assembled per record; the carrier is never {@code null} here.
     * Package-private, not {@code private}, so {@code keys()} — required by the {@link TextMapGetter}
     * contract but never called by the W3C extractor this module tests against directly — has a unit
     * test of its own rather than relying on some other configured propagator to exercise it.
     */
    static final TextMapGetter<Map<String, String>> CARRIER_GETTER = new TextMapGetter<>() {

        @Override
        public Iterable<String> keys(Map<String, String> carrier) {
            return carrier.keySet();
        }

        @Override
        public String get(Map<String, String> carrier, String key) {
            return carrier.get(key);
        }
    };

    private final Tracer tracer;
    private final TextMapPropagator propagator;

    /**
     * @param openTelemetry the application's OpenTelemetry instance, supplying both the tracer that
     *                      builds the span and the propagator that reads the row's captured context
     *                      back into its parent — in the same format the write side captured it.
     *                      Passing it explicitly, rather than reading a global, is what makes
     *                      instrumented mode opt-in (§9)
     * @throws NullPointerException if {@code openTelemetry} is {@code null}
     */
    public OtelTandemSpanRecorder(OpenTelemetry openTelemetry) {
        Objects.requireNonNull(openTelemetry, "openTelemetry");
        this.tracer = openTelemetry.getTracer(INSTRUMENTATION_SCOPE);
        this.propagator = openTelemetry.getPropagators().getTextMapPropagator();
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
        // Extracted from the root, not from Context.current(): whatever context this worker thread
        // happens to carry belongs to the claim cycle, not to the record being published.
        Context parent = propagator.extract(Context.root(), carrier, CARRIER_GETTER);
        // Fully qualified throughout: the inherited TandemSpanRecorder.Span shadows any import of
        // OpenTelemetry's same-named type, so an import here would silently resolve to the wrong Span.
        if (!io.opentelemetry.api.trace.Span.fromContext(parent).getSpanContext().isValid()) {
            return TandemSpanRecorder.Span.NOOP;
        }
        io.opentelemetry.api.trace.SpanBuilder builder = tracer.spanBuilder(PUBLISH_SPAN_NAME)
                .setParent(parent)
                .setSpanKind(SpanKind.PRODUCER)
                .setAttribute(TandemSpanAttributes.OUTBOX_ROW_ID, rowId)
                .setAttribute(TandemSpanAttributes.AGGREGATE_TYPE, aggregateType)
                .setAttribute(TandemSpanAttributes.AGGREGATE_ID, aggregateId)
                .setAttribute(TandemSpanAttributes.ATTEMPTS, attempts)
                .setAttribute(TandemSpanAttributes.TOPIC, topic);
        if (correlationId != null) {
            builder.setAttribute(TandemSpanAttributes.CORRELATION_ID, correlationId);
        }
        io.opentelemetry.api.trace.Span span = builder.startSpan();
        return new TandemSpanRecorder.Span() {

            @Override
            public void end() {
                span.end();
            }

            @Override
            public void end(Throwable failure) {
                span.recordException(failure);
                span.setStatus(StatusCode.ERROR);
                span.end();
            }
        };
    }
}
