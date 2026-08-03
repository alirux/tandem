package com.codingful.tandem.admin.outbox;

/** Thrown by {@link OutboxAdminService#discardMessage} when {@code acknowledgeOrderingBreak} is not {@code true}. */
final class OrderingBreakNotAcknowledgedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    OrderingBreakNotAcknowledgedException() {
        super("Discarding a message breaks per-aggregate ordering; acknowledgeOrderingBreak must be true");
    }
}
