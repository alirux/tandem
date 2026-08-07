package com.codingful.tandem.core.port;

import com.codingful.tandem.core.TandemContext;
import com.codingful.tandem.core.TandemHeaders;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Optional trace-capture port (LLD-core §2.5, §7.2). The default is a no-op returning {@code {}} and
 * {@link #isEnabled()} is {@code false}. Real adapters ship in {@code tandem-spring-producer} /
 * {@code tandem-tracing-otel}.
 */
public interface TracePropagator {

    /** A no-op propagator — the default when tracing is disabled. */
    TracePropagator NOOP = new TracePropagator() {
    };

    default boolean isEnabled() {
        return false;
    }

    /** Trace headers to attach to the outbox row; {@code {}} when disabled. */
    default Map<String, String> capture() {
        return Map.of();
    }

    /**
     * The dependency-free correlation-id capture: reads whatever the caller set through
     * {@link TandemContext}, so an application outside Spring gets the incident-time identifier of
     * HLD-tracing.md §4.1 without adding a tracing library, a logging binding, or any other
     * dependency to its write side (§1.3). Carries no distributed trace context — that needs a real
     * tracing library, wired separately and merged in through {@link #composite}.
     *
     * <p>A Spring application uses {@code MdcCorrelationTracePropagator} instead, which reads the same
     * id off the MDC key its logging already populates and falls back to {@link TandemContext} only
     * where no MDC is active.
     *
     * @return a propagator capturing {@code correlation-id} from the current thread's
     *         {@link TandemContext}, or nothing when none is set
     */
    static TracePropagator fromTandemContext() {
        return new TracePropagator() {

            @Override
            public boolean isEnabled() {
                return true;
            }

            @Override
            public Map<String, String> capture() {
                String correlationId = TandemContext.currentCorrelationId();
                return correlationId == null ? Map.of() : Map.of(TandemHeaders.CORRELATION_ID, correlationId);
            }
        };
    }

    /**
     * Merges several propagators into one, so a single capture chokepoint can carry contexts that come
     * from different sources — the distributed trace context from a tracing library and the correlation
     * id from MDC are captured independently and neither implies the other (HLD-tracing.md §2/§5).
     *
     * <p>Disabled delegates are skipped, and the merged propagator is enabled only when at least one
     * delegate is. On a key collision the <b>earlier</b> delegate wins, the same precedence the insert
     * path already applies when merging a capture into caller-supplied headers.
     *
     * @param delegates the propagators to merge, in precedence order
     * @return a propagator capturing the union of the delegates' headers
     * @throws NullPointerException if {@code delegates} or any element is {@code null}
     */
    static TracePropagator composite(TracePropagator... delegates) {
        List<TracePropagator> merged = List.of(delegates);
        return new TracePropagator() {

            @Override
            public boolean isEnabled() {
                return merged.stream().anyMatch(TracePropagator::isEnabled);
            }

            @Override
            public Map<String, String> capture() {
                Map<String, String> headers = new LinkedHashMap<>();
                for (TracePropagator delegate : merged) {
                    if (delegate.isEnabled()) {
                        delegate.capture().forEach(headers::putIfAbsent);
                    }
                }
                return headers;
            }
        };
    }
}
