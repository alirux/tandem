package com.codingful.tandem.spring.producer;

import com.codingful.tandem.core.AggregateId;
import com.codingful.tandem.core.OutboxMessage;

/**
 * The sink a {@link TransactionalOutboxTemplate} unit of work records outbox rows into
 * (LLD-spring-producer §3). Implementations collect the calls and the template inserts them, in
 * collection order, within the same transaction it opened.
 *
 * <p>A collector instance is valid only for the duration of the {@code execute} call that supplied it
 * and is not thread-safe.
 */
public interface OutboxCollector {

    /**
     * Record a pre-built message — the full-control, dependency-free path (no serializer involved).
     *
     * @param message the row to insert; must not be null
     */
    void add(OutboxMessage message);

    /**
     * Record a row from an object payload, serialized by the configured {@code PayloadSerializer}.
     *
     * @param aggregateType the aggregate type (topic routing, CloudEvents {@code type} fallback)
     * @param aggregateId   the aggregate the row belongs to
     * @param seq           the per-aggregate sequence number — must be the aggregate's {@code version}
     *                      (HLD §4.2); Tandem never invents it
     * @param payload       the payload object to serialize
     * @throws com.codingful.tandem.core.exception.PayloadSerializationException if no
     *                      {@code PayloadSerializer} is configured, or serialization fails
     */
    void record(String aggregateType, AggregateId aggregateId, long seq, Object payload);

    /**
     * As {@link #record(String, AggregateId, long, Object)}, with a String aggregate id.
     *
     * @param aggregateType the aggregate type
     * @param aggregateId   the aggregate id
     * @param seq           the per-aggregate sequence number (the aggregate's {@code version})
     * @param payload       the payload object to serialize
     */
    void record(String aggregateType, String aggregateId, long seq, Object payload);
}
