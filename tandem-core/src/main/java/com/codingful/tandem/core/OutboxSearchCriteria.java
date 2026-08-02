package com.codingful.tandem.core;

import java.time.Instant;

/**
 * Selection criteria for {@link com.codingful.tandem.core.port.OutboxQuery#search} (HLD-admin-api §4).
 * Every filter is optional and combines with AND. Unlike {@link ReplayCriteria}, a criteria-less
 * search is legitimate — it is bounded by {@code limit}, and "show me the newest rows" needs no
 * selector.
 *
 * @param afterId the cursor: only rows with {@code id > afterId} are returned; {@code null} starts
 *                from the beginning
 * @param limit   the maximum number of rows to return, between {@value #MIN_LIMIT} and
 *                {@value #MAX_LIMIT}
 */
public record OutboxSearchCriteria(
        OutboxStatus status,
        AggregateId aggregateId,
        String aggregateType,
        String type,
        Instant createdFrom,
        Instant createdTo,
        Long afterId,
        int limit) {

    public static final int MIN_LIMIT = 1;
    public static final int MAX_LIMIT = 500;
    public static final int DEFAULT_LIMIT = 50;

    /** @throws IllegalArgumentException if {@code limit} is outside {@value #MIN_LIMIT}–{@value #MAX_LIMIT} */
    public OutboxSearchCriteria {
        if (limit < MIN_LIMIT || limit > MAX_LIMIT) {
            throw new IllegalArgumentException(
                    "limit must be between " + MIN_LIMIT + " and " + MAX_LIMIT + ", got " + limit);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private OutboxStatus status;
        private AggregateId aggregateId;
        private String aggregateType;
        private String type;
        private Instant createdFrom;
        private Instant createdTo;
        private Long afterId;
        private int limit = DEFAULT_LIMIT;

        private Builder() {
        }

        public Builder status(OutboxStatus status) {
            this.status = status;
            return this;
        }

        public Builder aggregateId(AggregateId aggregateId) {
            this.aggregateId = aggregateId;
            return this;
        }

        public Builder aggregateType(String aggregateType) {
            this.aggregateType = aggregateType;
            return this;
        }

        public Builder type(String type) {
            this.type = type;
            return this;
        }

        public Builder createdFrom(Instant createdFrom) {
            this.createdFrom = createdFrom;
            return this;
        }

        public Builder createdTo(Instant createdTo) {
            this.createdTo = createdTo;
            return this;
        }

        public Builder afterId(Long afterId) {
            this.afterId = afterId;
            return this;
        }

        /** Default {@value OutboxSearchCriteria#DEFAULT_LIMIT}. */
        public Builder limit(int limit) {
            this.limit = limit;
            return this;
        }

        public OutboxSearchCriteria build() {
            return new OutboxSearchCriteria(
                    status, aggregateId, aggregateType, type, createdFrom, createdTo, afterId, limit);
        }
    }
}
