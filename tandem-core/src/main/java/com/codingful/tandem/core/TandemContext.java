package com.codingful.tandem.core;

/**
 * Explicit correlation-id API (LLD-core §2.5, HLD-tracing.md §5/§9) for call sites with no active
 * MDC — batch jobs, Kafka listeners, anywhere a {@code TracePropagator} adapter cannot read one
 * automatically. Thread-local, mirroring the semantics of a logging MDC: a value set here is visible
 * only on the setting thread, so on a pooled thread callers must {@link #clear()} when done to avoid
 * leaking a stale id into whatever that thread handles next.
 */
public final class TandemContext {

    private static final ThreadLocal<String> CORRELATION_ID = new ThreadLocal<>();

    private TandemContext() {
    }

    public static void setCorrelationId(String correlationId) {
        CORRELATION_ID.set(correlationId);
    }

    public static String currentCorrelationId() {
        return CORRELATION_ID.get();
    }

    public static void clear() {
        CORRELATION_ID.remove();
    }
}
