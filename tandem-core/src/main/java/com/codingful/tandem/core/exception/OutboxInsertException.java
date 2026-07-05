package com.codingful.tandem.core.exception;

/** The write-side INSERT into {@code tandem_outbox} failed (LLD-core §3). */
public class OutboxInsertException extends TandemException {

    private static final long serialVersionUID = -6701746542224422890L;

    public OutboxInsertException(String message) {
        super(message);
    }

    public OutboxInsertException(String message, Throwable cause) {
        super(message, cause);
    }
}
