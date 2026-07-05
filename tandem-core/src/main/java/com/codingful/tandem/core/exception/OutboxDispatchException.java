package com.codingful.tandem.core.exception;

/**
 * Publishing a record to the broker failed (LLD-core §3, Q17). Carries the dispatcher's
 * <strong>retriable-vs-permanent verdict</strong>: the {@code tandem-kafka} dispatcher classifies the
 * Kafka error (LLD-kafka §4) and sets it, so the {@code tandem-jdbc} relay routes the failure to
 * {@code markForRetry} (retriable) or {@code markFailed} (permanent) without knowing any Kafka type.
 */
public class OutboxDispatchException extends TandemException {

    private static final long serialVersionUID = 4090021064261249161L;

    private final boolean retriable;

    public OutboxDispatchException(String message, boolean retriable) {
        super(message);
        this.retriable = retriable;
    }

    public OutboxDispatchException(String message, boolean retriable, Throwable cause) {
        super(message, cause);
        this.retriable = retriable;
    }

    /** {@code true} → the relay should retry with backoff; {@code false} → fail the row permanently. */
    public boolean isRetriable() {
        return retriable;
    }
}
