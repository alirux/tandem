package com.codingful.tandem.core;

import java.time.Instant;
import java.util.Objects;

/**
 * One row of a {@link com.codingful.tandem.core.port.OutboxQuery#search} result — the Admin API's
 * list view. Deliberately narrower than {@link OutboxRecord}: it carries no {@code payload} or
 * {@code headers}, because the list view must not read those columns at all (HLD-admin-api §4). Use
 * {@link com.codingful.tandem.core.port.OutboxQuery#findById} for the full row.
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
        Instant nextAttemptAt,
        String lockedBy,
        Instant lockedUntil,
        Instant createdAt) {

    public OutboxRowView {
        Objects.requireNonNull(aggregateId, "aggregateId");
        Objects.requireNonNull(aggregateType, "aggregateType");
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(createdAt, "createdAt");
    }
}
