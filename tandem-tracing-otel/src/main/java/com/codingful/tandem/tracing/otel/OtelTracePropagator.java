package com.codingful.tandem.tracing.otel;

import com.codingful.tandem.core.port.TracePropagator;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapPropagator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * The OpenTelemetry write-side {@link TracePropagator} (HLD-tracing.md §3/§5): captures the trace
 * context live inside the domain transaction, so the row carries it across the asynchronous outbox
 * boundary and a consumer — or the relay's own publish span — continues the same trace instead of
 * starting an unrelated one. The Micrometer-based equivalent for Spring applications is
 * {@code MicrometerTracePropagator}; this is the same bridge for an application that instruments
 * itself with the OpenTelemetry SDK directly.
 *
 * <p>Capture goes through the application's own configured {@link TextMapPropagator} rather than
 * formatting a {@code traceparent} here, so whatever format the application propagates elsewhere is
 * what lands in the row — W3C under the default configuration, B3 under a B3 one, plus baggage where
 * that is configured too. Writing W3C unconditionally would produce a header the application's own
 * consumers do not read. Note that a B3-configured application therefore gets propagation but no
 * relay publish span: the relay parents that span from {@code traceparent} (§6).
 *
 * <p>Carries no correlation id — that comes from an independent source (§2) and needs no tracing
 * library at all: use {@link TracePropagator#fromTandemContext()} and merge the two with
 * {@link TracePropagator#composite}.
 *
 * <p>With no active span — a batch job, a call outside any instrumented entry point — capture returns
 * no headers rather than starting a span of its own: the outbox insert is a participant in the
 * caller's trace, never the reason a trace exists.
 */
public final class OtelTracePropagator implements TracePropagator {

    private final TextMapPropagator propagator;

    /**
     * @param openTelemetry the application's OpenTelemetry instance, supplying the propagator that
     *                      writes the current context into the row's headers. Passing it explicitly,
     *                      rather than reading a global, is what makes tracing opt-in (§9)
     * @throws NullPointerException if {@code openTelemetry} is {@code null}
     */
    public OtelTracePropagator(OpenTelemetry openTelemetry) {
        this.propagator = Objects.requireNonNull(openTelemetry, "openTelemetry")
                .getPropagators()
                .getTextMapPropagator();
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public Map<String, String> capture() {
        Context current = Context.current();
        if (!Span.fromContext(current).getSpanContext().isValid()) {
            return Map.of();
        }
        Map<String, String> headers = new LinkedHashMap<>();
        propagator.inject(current, headers, Map::put);
        return headers;
    }
}
