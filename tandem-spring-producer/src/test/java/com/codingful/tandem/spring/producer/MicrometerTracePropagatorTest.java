package com.codingful.tandem.spring.producer;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.TandemHeaders;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Runs against a real OpenTelemetry SDK behind the Micrometer Tracing bridge, so the captured header is
 * a genuine W3C {@code traceparent} produced by a real propagator rather than a formatted stand-in — the
 * point of the bridge is interop with what the application's own tracing stack emits (HLD-tracing.md §3).
 * The same suite re-runs on every line of the compatibility matrix, each with its own Boot generation's
 * micrometer-tracing and OpenTelemetry releases.
 */
class MicrometerTracePropagatorTest {

    private final TestTracing tracing = TestTracing.w3c();
    private final Tracer tracer = tracing.tracer();
    private final MicrometerTracePropagator tracePropagator =
            new MicrometerTracePropagator(tracer, tracing.propagator());

    @Test
    void GIVEN_an_active_trace_WHEN_captured_THEN_the_row_carries_that_trace_as_a_w3c_traceparent() {
        Span span = tracer.nextSpan().name("business-operation").start();
        Map<String, String> captured;
        Tracer.SpanInScope scope = tracer.withSpan(span);
        try {
            captured = tracePropagator.capture();
        } finally {
            scope.close();
            span.end();
        }

        assertThat(tracePropagator.isEnabled()).isTrue();
        // The captured header must name the very trace and span the caller was in — otherwise the
        // consumer continues some other trace, which is worse than no propagation at all. The trailing
        // trace-flags byte is deliberately not pinned: it belongs to the propagator, and W3C Trace
        // Context level 2 added a random-trace-id flag that newer OpenTelemetry releases set (the value
        // is 01 under the baseline line's SDK and 03 under Spring Boot 4's).
        assertThat(captured.get(TandemHeaders.TRACEPARENT))
                .startsWith("00-" + span.context().traceId() + "-" + span.context().spanId() + "-");
    }

    @Test
    void GIVEN_no_active_trace_WHEN_captured_THEN_nothing_is_captured_and_no_trace_is_started() {
        assertThat(tracePropagator.capture()).isEmpty();
    }
}
