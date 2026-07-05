package com.codingful.tandem.core;

import java.util.Objects;

/**
 * Typed value object for an aggregate identifier (LLD-core §1.1) — prevents accidentally mixing the
 * several string fields (`aggregateId`, `aggregateType`, `type`). The length bound mirrors the
 * {@code VARCHAR(255)} column.
 */
public record AggregateId(String value) {

    public AggregateId {
        Objects.requireNonNull(value, "aggregateId");
        if (value.isBlank()) {
            throw new IllegalArgumentException("aggregateId must not be blank");
        }
        if (value.length() > 255) {
            throw new IllegalArgumentException("aggregateId exceeds 255 chars");
        }
    }

    public static AggregateId of(String value) {
        return new AggregateId(value);
    }

    @Override
    public String toString() {
        return value;
    }
}
