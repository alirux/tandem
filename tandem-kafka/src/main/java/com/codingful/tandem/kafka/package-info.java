/**
 * The Kafka publish adapter (LLD-kafka): {@link com.codingful.tandem.kafka.KafkaRelay} implements the
 * {@link com.codingful.tandem.core.port.OutboxDispatcher} port by encoding each
 * {@link com.codingful.tandem.core.OutboxRecord} as a CloudEvent (binary mode) and sending it on one
 * async producer, completing the returned future on the broker ack.
 *
 * <p>The producer config is hardened to safe values (idempotence, {@code acks=all}), the destination
 * topic comes from a {@link com.codingful.tandem.core.port.TopicRouter}, and Kafka errors are mapped to
 * the retriable/permanent verdict by an {@link com.codingful.tandem.kafka.ErrorClassifier}. This module
 * is relay-side only — never on the client write-side (§1.3).
 */
package com.codingful.tandem.kafka;
