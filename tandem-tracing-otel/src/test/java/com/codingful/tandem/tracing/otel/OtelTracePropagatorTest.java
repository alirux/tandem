package com.codingful.tandem.tracing.otel;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.TandemHeaders;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Runs against a real OpenTelemetry SDK, so the captured header is a genuine W3C {@code traceparent}
 * produced by a real propagator rather than a formatted stand-in — the point of the bridge is interop
 * with what the application's own tracing stack emits (HLD-tracing.md §3).
 */
class OtelTracePropagatorTest {

    private final OpenTelemetry openTelemetry = OpenTelemetrySdk.builder()
            .setTracerProvider(SdkTracerProvider.builder().build())
            .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
            .build();
    private final Tracer tracer = openTelemetry.getTracer("test");
    private final OtelTracePropagator tracePropagator = new OtelTracePropagator(openTelemetry);

    @Test
    void GIVEN_an_active_span_WHEN_captured_THEN_the_row_carries_that_trace_as_a_w3c_traceparent() {
        Span span = tracer.spanBuilder("business-operation").startSpan();
        Map<String, String> captured;
        Scope scope = span.makeCurrent();
        try {
            captured = tracePropagator.capture();
        } finally {
            scope.close();
            span.end();
        }

        assertThat(tracePropagator.isEnabled()).isTrue();
        // The captured header must name the very trace and span the caller was in — otherwise the
        // consumer continues some other trace, which is worse than no propagation at all.
        assertThat(captured.get(TandemHeaders.TRACEPARENT)).isEqualTo(
                "00-" + span.getSpanContext().getTraceId() + "-" + span.getSpanContext().getSpanId() + "-01");
    }

    @Test
    void GIVEN_no_active_span_WHEN_captured_THEN_nothing_is_captured_and_no_trace_is_started() {
        assertThat(tracePropagator.capture()).isEmpty();
    }

    @Test
    void GIVEN_a_missing_instance_WHEN_constructed_THEN_it_fails_immediately() {
        org.assertj.core.api.Assertions.assertThatNullPointerException()
                .isThrownBy(() -> new OtelTracePropagator(null));
    }
}
