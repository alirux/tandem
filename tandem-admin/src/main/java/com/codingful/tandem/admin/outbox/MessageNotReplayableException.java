package com.codingful.tandem.admin.outbox;

/** Thrown by {@link OutboxAdminService#replayMessage} when the row is neither {@code DONE} nor {@code FAILED}. */
final class MessageNotReplayableException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    MessageNotReplayableException(long id) {
        super("Outbox message " + id + " is not in a replayable state (must be DONE or FAILED)");
    }
}
