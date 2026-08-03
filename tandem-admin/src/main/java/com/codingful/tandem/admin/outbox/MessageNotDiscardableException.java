package com.codingful.tandem.admin.outbox;

/** Thrown by {@link OutboxAdminService#discardMessage} when the row is not {@code FAILED}. */
final class MessageNotDiscardableException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    MessageNotDiscardableException(long id) {
        super("Outbox message " + id + " is not in a discardable state (must be FAILED)");
    }
}
