package com.codingful.tandem.admin.outbox;

/** Thrown by {@link OutboxAdminService#findById} when no row exists with the given id. */
final class OutboxMessageNotFoundException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    OutboxMessageNotFoundException(long id) {
        super("No outbox message with id " + id);
    }
}
