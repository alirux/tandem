package com.codingful.tandem.admin.outbox;

/**
 * Thrown by {@link OutboxAdminService#replayBulk} when the request carries no selector at all. Kept
 * distinct from the generic {@code invalid-parameter} 400 (thrown by
 * {@link com.codingful.tandem.admin.TandemAdminExceptionHandler}) so the response slug matches the
 * contract's {@code replay-no-selector}.
 */
final class ReplayNoSelectorException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    ReplayNoSelectorException() {
        super("At least one replay selector (aggregateId, aggregateType, an id range, or statuses) is required");
    }
}
