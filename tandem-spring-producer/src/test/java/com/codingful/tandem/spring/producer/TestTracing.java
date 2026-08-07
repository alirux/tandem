package com.codingful.tandem.spring.producer;

import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.otel.bridge.OtelCurrentTraceContext;
import io.micrometer.tracing.otel.bridge.OtelPropagator;
import io.micrometer.tracing.otel.bridge.OtelTracer;
import io.micrometer.tracing.propagation.Propagator;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.trace.SdkTracerProvider;

/**
 * A real tracing stack for the tests — the OpenTelemetry SDK behind the Micrometer Tracing bridge,
 * configured with W3C propagation exactly as Spring Boot's own tracing autoconfiguration does. Real
 * collaborators throughout: what the tests assert on is a genuine {@code traceparent}, not a value
 * this project formatted itself.
 */
final class TestTracing {

    private final Tracer tracer;
    private final Propagator propagator;

    private TestTracing() {
        io.opentelemetry.api.trace.Tracer otelTracer = SdkTracerProvider.builder().build().get("tandem-test");
        this.tracer = new OtelTracer(otelTracer, new OtelCurrentTraceContext(), event -> {
        });
        this.propagator =
                new OtelPropagator(ContextPropagators.create(W3CTraceContextPropagator.getInstance()), otelTracer);
    }

    static TestTracing w3c() {
        return new TestTracing();
    }

    Tracer tracer() {
        return tracer;
    }

    Propagator propagator() {
        return propagator;
    }
}
