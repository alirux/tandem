package com.codingful.tandem.core;

import java.util.Set;

/**
 * Selection criteria for {@link com.codingful.tandem.core.port.ReplayService} (LLD-core §2.6, §8).
 * At least one selector ({@code aggregateId}, {@code aggregateType}, an id bound, or a status set)
 * must be present — a criteria-less replay would re-publish the entire outbox.
 *
 * @param dryRun when {@code true}, the service reports what would match without changing any row.
 */
public record ReplayCriteria(AggregateId aggregateId,
                             String aggregateType,
                             Long fromId,
                             Long toId,
                             Set<OutboxStatus> statuses,
                             boolean dryRun) {

    public ReplayCriteria {
        statuses = statuses == null ? Set.of() : Set.copyOf(statuses);
        boolean hasSelector = aggregateId != null
                || aggregateType != null
                || fromId != null
                || toId != null
                || !statuses.isEmpty();
        if (!hasSelector) {
            throw new IllegalArgumentException("ReplayCriteria requires at least one selector");
        }
    }
}
