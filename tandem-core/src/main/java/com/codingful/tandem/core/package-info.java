/**
 * Tandem's dependency-free core: the transactional-outbox domain model shared by the write-side
 * client and the relay (LLD-core §1). Immutable value types ({@link com.codingful.tandem.core.OutboxMessage},
 * {@link com.codingful.tandem.core.OutboxRecord}, {@link com.codingful.tandem.core.AggregateId}), the
 * delivery-state enums, and the pure hashing logic ({@link com.codingful.tandem.core.BucketHash}) live
 * here — as does {@link com.codingful.tandem.core.LamportClock}, which is reserved API for the
 * unbuilt causal-ordering feature ({@code docs/HLD-causal-ordering.md} §0) rather than live logic.
 *
 * <p>This package depends only on the JDK — no Kafka, no JSON library, no framework — so importing it
 * on the client write-side forces nothing on the caller (§1.3). The ports (interfaces the adapters
 * implement) are in {@link com.codingful.tandem.core.port}; the exception hierarchy in
 * {@link com.codingful.tandem.core.exception}.
 */
package com.codingful.tandem.core;
