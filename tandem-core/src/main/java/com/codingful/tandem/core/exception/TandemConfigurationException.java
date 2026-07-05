package com.codingful.tandem.core.exception;

/**
 * A configuration invariant was violated at startup (LLD-core §3) — e.g. an unsafe Kafka producer
 * override (LLD-kafka §1) or {@code rowLease <= delivery.timeout.ms} (LLD-jdbc §3.5). Tandem fails
 * fast rather than running with a config that could lose or reorder events.
 */
public class TandemConfigurationException extends TandemException {

    private static final long serialVersionUID = -6473895174481292662L;

    public TandemConfigurationException(String message) {
        super(message);
    }

    public TandemConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }
}
