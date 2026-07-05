package com.codingful.tandem.core;

/**
 * Outcome of a single delivery attempt recorded by the (opt-in) attempt archive
 * (HLD-attempt-archive §2/§3). Mapped to {@code tandem_outbox_attempt.status SMALLINT}.
 */
public enum AttemptStatus {
    SUCCESS(1),
    FAILED(2);

    private final int code;

    AttemptStatus(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }
}
