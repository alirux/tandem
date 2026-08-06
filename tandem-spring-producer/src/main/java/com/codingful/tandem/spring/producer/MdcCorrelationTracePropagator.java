package com.codingful.tandem.spring.producer;

import com.codingful.tandem.core.TandemContext;
import com.codingful.tandem.core.TandemHeaders;
import com.codingful.tandem.core.port.TracePropagator;
import java.util.Map;
import java.util.Objects;
import org.slf4j.MDC;

/**
 * Correlation-id-only {@link TracePropagator} (HLD-tracing.md §5/§9): reads the configured MDC key,
 * falling back to the explicit {@link TandemContext} API for call sites with no active MDC (batch
 * jobs, Kafka listeners). Carries no distributed trace context — no {@code traceparent}/
 * {@code tracestate} — that requires a real tracing library, wired separately.
 */
public final class MdcCorrelationTracePropagator implements TracePropagator {

    private final String mdcKey;

    public MdcCorrelationTracePropagator(String mdcKey) {
        this.mdcKey = Objects.requireNonNull(mdcKey, "mdcKey");
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public Map<String, String> capture() {
        String correlationId = MDC.get(mdcKey);
        if (correlationId == null) {
            correlationId = TandemContext.currentCorrelationId();
        }
        return correlationId == null ? Map.of() : Map.of(TandemHeaders.CORRELATION_ID, correlationId);
    }
}
