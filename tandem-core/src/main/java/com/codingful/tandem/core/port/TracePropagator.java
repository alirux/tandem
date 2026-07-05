package com.codingful.tandem.core.port;

import java.util.Map;

/**
 * Optional trace-capture port (LLD-core §2.5, §7.2). The default is a no-op returning {@code {}} and
 * {@link #isEnabled()} is {@code false}. Real adapters ship in {@code tandem-spring} /
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
}
