package com.codingful.tandem.core;

import java.time.Instant;
import java.util.Objects;

/**
 * One row of a {@link com.codingful.tandem.core.port.OutboxQuery#search} result — the Admin API's
 * list view. Deliberately narrower than {@link OutboxRecord}: it carries no {@code payload} or
 * {@code headers}, because the list view must not read those columns at all (HLD-admin-api §4). Use
 * {@link com.codingful.tandem.core.port.OutboxQuery#findById} for the full row.
 *
 * <p>{@code correlationId} is the one exception to "no header values in the list view": it is read
 * from its own indexed column, not from {@code headers}, precisely so an incident search can return
 * it without touching the JSONB (HLD-tracing §4). {@code null} for rows written with tracing off.
 */
public record OutboxRowView(
        long id,
        AggregateId aggregateId,
        String aggregateType,
        String type,
        long seq,
        OutboxStatus status,
        int attempts,
        String lastError,
        String discardReason,
        Instant nextAttemptAt,
        String lockedBy,
        Instant lockedUntil,
        Instant createdAt,
        String correlationId) {

    public OutboxRowView {
        Objects.requireNonNull(aggregateId, "aggregateId");
        Objects.requireNonNull(aggregateType, "aggregateType");
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(createdAt, "createdAt");
    }
}
