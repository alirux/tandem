package com.codingful.tandem.core;

/**
 * Delivery state of an outbox row, mapped to the {@code status SMALLINT} column (LLD-core §1.2).
 *
 * <p>{@code DISCARDED} (4) is reachable only via the Admin API on a {@code FAILED} row; it is never
 * polled and does not block the aggregate (HLD §5.3).
 */
public enum OutboxStatus {
    PENDING(0),
    IN_FLIGHT(1),
    DONE(2),
    FAILED(3),
    DISCARDED(4);

    private final int code;

    OutboxStatus(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    /** Maps a stored {@code SMALLINT} back to the enum; throws on an unknown code. */
    public static OutboxStatus fromCode(int code) {
        return switch (code) {
            case 0 -> PENDING;
            case 1 -> IN_FLIGHT;
            case 2 -> DONE;
            case 3 -> FAILED;
            case 4 -> DISCARDED;
            default -> throw new IllegalArgumentException("Unknown OutboxStatus code: " + code);
        };
    }
}
