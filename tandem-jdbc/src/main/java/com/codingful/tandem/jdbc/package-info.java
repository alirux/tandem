/**
 * The PostgreSQL JDBC adapter (LLD-jdbc): the write-side {@link com.codingful.tandem.jdbc.JdbcOutboxRepository}
 * (inserts rows within the caller's transaction), the relay-side {@link com.codingful.tandem.jdbc.JdbcOutboxStore}
 * (claim/mark/reclaim/cleanup over {@code tandem_outbox}), the relay engine
 * {@link com.codingful.tandem.jdbc.WorkerPool}, and multi-instance bucket coordination
 * ({@link com.codingful.tandem.jdbc.BucketSource} / {@link com.codingful.tandem.jdbc.BucketLeaseManager}).
 *
 * <p>Depends only on {@code java.sql} (JDK): no Kafka, no JSON library, no metrics library. It talks to
 * the publish side purely through the {@link com.codingful.tandem.core.port.OutboxDispatcher} port, so it
 * never depends on {@code tandem-kafka}.
 */
package com.codingful.tandem.jdbc;
