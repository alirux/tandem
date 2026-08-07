package com.codingful.tandem.spring.producer;

import com.codingful.tandem.core.port.TracePropagator;
import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * The distributed-trace-context {@link TracePropagator} (HLD-tracing.md §3/§5): captures the trace
 * context that is live inside the domain transaction, so the row carries it across the asynchronous
 * outbox boundary and a consumer continues the same trace. Carries no correlation id — that comes
 * from {@link MdcCorrelationTracePropagator}, and the two are merged via
 * {@link TracePropagator#composite}.
 *
 * <p>Capture delegates to the application's own {@link Propagator} bean rather than formatting a
 * {@code traceparent} here, so whatever propagation format the application configured is what lands
 * in the row — W3C ({@code traceparent}/{@code tracestate}) under Spring Boot's default, {@code b3}
 * under a B3 configuration. Writing W3C unconditionally would produce a header the application's own
 * consumers do not read. Note that a B3-configured application therefore gets propagation but no
 * relay publish span: the relay reads {@code traceparent} to parent that span (HLD-tracing.md §6).
 *
 * <p>With no active trace context — a batch job, a call outside any instrumented entry point — capture
 * returns no headers rather than starting a span of its own: the outbox insert is a participant in the
 * caller's trace, never the reason a trace exists.
 */
public final class MicrometerTracePropagator implements TracePropagator {

    private final Tracer tracer;
    private final Propagator propagator;

    /**
     * @param tracer     supplies the trace context active on the calling thread
     * @param propagator writes that context into the row's headers, in the application's own format
     * @throws NullPointerException if either argument is {@code null}
     */
    public MicrometerTracePropagator(Tracer tracer, Propagator propagator) {
        this.tracer = Objects.requireNonNull(tracer, "tracer");
        this.propagator = Objects.requireNonNull(propagator, "propagator");
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public Map<String, String> capture() {
        TraceContext context = tracer.currentTraceContext().context();
        if (context == null) {
            return Map.of();
        }
        Map<String, String> headers = new LinkedHashMap<>();
        propagator.inject(context, headers, Map::put);
        return headers;
    }
}
