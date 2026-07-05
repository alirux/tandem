package com.codingful.tandem.core.exception;

/**
 * The {@code UNIQUE(aggregate_id, seq)} constraint was violated on insert (LLD-core §3). Callers
 * using optimistic locking catch this to detect a {@code seq} conflict and retry, without parsing
 * a SQL state.
 */
public class DuplicateSeqException extends OutboxInsertException {

    private static final long serialVersionUID = 7294406286944549598L;

    public DuplicateSeqException(String message) {
        super(message);
    }

    public DuplicateSeqException(String message, Throwable cause) {
        super(message, cause);
    }
}
