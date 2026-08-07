package com.codingful.tandem.tracing.otel;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.port.TandemSpanRecorder;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.data.StatusData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Runs against a real OpenTelemetry SDK, exporting into memory, so what is asserted is a genuinely
 * exported span with a genuinely resolved parent — the one thing a hand-written stand-in could not
 * prove (HLD-tracing.md §6).
 */
class OtelTandemSpanRecorderTest {

    private static final String TRACE_ID = "0af7651916cd43dd8448eb211c80319c";
    private static final String PARENT_SPAN_ID = "b7ad6b7169203331";
    private static final String TRACEPARENT = "00-" + TRACE_ID + "-" + PARENT_SPAN_ID + "-01";

    private final InMemorySpanExporter exporter = InMemorySpanExporter.create();
    private final OpenTelemetry openTelemetry = OpenTelemetrySdk.builder()
            .setTracerProvider(SdkTracerProvider.builder()
                    .addSpanProcessor(SimpleSpanProcessor.create(exporter))
                    .build())
            .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
            .build();

    private final OtelTandemSpanRecorder recorder = new OtelTandemSpanRecorder(openTelemetry);

    private TandemSpanRecorder.Span startSpanForTracedRow() {
        return recorder.startPublishSpan(42L, "Order", "order-1", 2, "order-topic", TRACEPARENT, null, "corr-1");
    }

    @Test
    void GIVEN_a_row_carrying_the_producing_trace_WHEN_it_is_published_THEN_the_publish_joins_that_same_trace() {
        startSpanForTracedRow().end();

        assertThat(recorder.isEnabled()).isTrue();
        assertThat(exporter.getFinishedSpanItems()).singleElement().satisfies(span -> {
            // The whole point of the feature: the publish is attached to the business operation that
            // produced the event, not to a trace of the relay's own making.
            assertThat(span.getTraceId()).isEqualTo(TRACE_ID);
            assertThat(span.getParentSpanId()).isEqualTo(PARENT_SPAN_ID);
            assertThat(span.getName()).isEqualTo(TandemSpanRecorder.PUBLISH_SPAN_NAME);
        });
    }

    @Test
    void GIVEN_a_published_row_WHEN_the_span_is_exported_THEN_it_carries_the_identifiers_and_no_business_data() {
        startSpanForTracedRow().end();

        SpanData span = exporter.getFinishedSpanItems().get(0);
        assertThat(span.getAttributes().asMap()).satisfies(attributes -> assertThat(
                attributes.entrySet().stream().map(entry -> entry.getKey().getKey() + "=" + entry.getValue()))
                .containsExactlyInAnyOrder(
                        "tandem.outbox.row_id=42",
                        "tandem.aggregate.type=Order",
                        "tandem.aggregate.id=order-1",
                        "tandem.attempts=2",
                        "tandem.topic=order-topic",
                        "tandem.correlation_id=corr-1"));
    }

    @Test
    void GIVEN_a_row_with_no_correlation_id_WHEN_it_is_published_THEN_no_empty_attribute_is_exported() {
        recorder.startPublishSpan(42L, "Order", "order-1", 0, "order-topic", TRACEPARENT, null, null).end();

        assertThat(exporter.getFinishedSpanItems().get(0).getAttributes().asMap().keySet())
                .noneSatisfy(key -> assertThat(key.getKey()).isEqualTo("tandem.correlation_id"));
    }

    @Test
    void GIVEN_a_delivery_that_fails_WHEN_the_span_ends_THEN_it_records_the_failure() {
        startSpanForTracedRow().end(new IllegalStateException("broker unreachable"));

        assertThat(exporter.getFinishedSpanItems()).singleElement().satisfies(span -> {
            assertThat(span.getStatus().getStatusCode()).isEqualTo(StatusData.error().getStatusCode());
            assertThat(span.getEvents()).isNotEmpty();
        });
    }

    @Test
    void GIVEN_a_row_carrying_no_trace_context_WHEN_it_is_published_THEN_no_orphan_trace_is_created() {
        TandemSpanRecorder.Span span =
                recorder.startPublishSpan(42L, "Order", "order-1", 0, "order-topic", null, null, "corr-1");
        span.end();

        assertThat(span).isSameAs(TandemSpanRecorder.Span.NOOP);
        assertThat(exporter.getFinishedSpanItems()).isEmpty();
    }

    @Test
    void GIVEN_a_row_carrying_an_unparseable_trace_context_WHEN_it_is_published_THEN_no_orphan_trace_is_created() {
        TandemSpanRecorder.Span span = recorder.startPublishSpan(
                42L, "Order", "order-1", 0, "order-topic", "not-a-valid-traceparent", null, "corr-1");
        span.end();

        assertThat(span).isSameAs(TandemSpanRecorder.Span.NOOP);
        assertThat(exporter.getFinishedSpanItems()).isEmpty();
    }

    @Test
    void GIVEN_a_row_carrying_vendor_trace_state_WHEN_it_is_published_THEN_that_state_is_carried_into_the_span() {
        recorder.startPublishSpan(42L, "Order", "order-1", 0, "order-topic", TRACEPARENT, "vendor=abc", null).end();

        assertThat(exporter.getFinishedSpanItems()).singleElement().satisfies(span -> {
            assertThat(span.getTraceId()).isEqualTo(TRACE_ID);
            assertThat(span.getSpanContext().getTraceState().get("vendor")).isEqualTo("abc");
        });
    }

    @Test
    void GIVEN_a_missing_instance_WHEN_constructed_THEN_it_fails_immediately() {
        org.assertj.core.api.Assertions.assertThatNullPointerException()
                .isThrownBy(() -> new OtelTandemSpanRecorder(null));
    }

    @Test
    void GIVEN_the_traceparent_and_tracestate_carrier_WHEN_its_keys_are_read_THEN_both_header_names_are_listed() {
        // Exercises the TextMapGetter contract's keys() method directly: no propagator this module is
        // tested against ever calls it (W3C looks up "traceparent"/"tracestate" by name), but another
        // configured propagator doing a case-insensitive header scan could.
        assertThat(OtelTandemSpanRecorder.CARRIER_GETTER.keys(Map.of("traceparent", TRACEPARENT, "tracestate", "vendor=abc")))
                .containsExactlyInAnyOrder("traceparent", "tracestate");
    }
}
