package com.codingful.tandem.core.port;

/**
 * Marks a {@code FAILED} outbox row {@code DISCARDED}, permanently abandoning delivery
 * (LLD-core §1.2; HLD-admin-api §4). Admin-only: never called by the relay, which is why this is
 * its own port rather than a method on {@link OutboxStore} — the same reasoning that keeps
 * {@link OutboxQuery} off {@link OutboxStore} (IMPLEMENTATION-PLAN-admin-api.md §3.3/§8.3).
 */
public interface DiscardService {

    /**
     * @param id     the outbox row id
     * @param reason free-text reason recorded for audit ({@code discard_reason}); may be {@code null}
     * @return {@code true} if the row was discarded; {@code false} if no row exists with this id, or
     *         it exists but is not currently {@code FAILED}
     */
    boolean discard(long id, String reason);
}
