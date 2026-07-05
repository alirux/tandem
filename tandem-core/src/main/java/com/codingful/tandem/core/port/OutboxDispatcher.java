package com.codingful.tandem.core.port;

import com.codingful.tandem.core.OutboxRecord;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

/**
 * Publish port (LLD-core §2.3), implemented by {@code tandem-kafka}.
 *
 * <p>{@code dispatch} is <b>non-blocking by contract</b>: the returned future completes when the
 * broker acks ({@code acks=all}), or completes <b>exceptionally</b> with an {@link
 * com.codingful.tandem.core.exception.OutboxDispatchException} carrying the retriable/permanent
 * verdict (Q17). Per-aggregate ordering is enforced upstream (the relay only ever has one row per
 * aggregate — its head — in flight; LLD-jdbc §3.3/§3.4), not by blocking here, so the relay can keep
 * many records of <b>distinct aggregates</b> in flight on a single producer.
 *
 * <p>{@link CompletableFuture} is {@code java.util.concurrent} (JDK), so the zero-dependency rule holds.
 */
public interface OutboxDispatcher {

    /**
     * @param record the head-of-chain row to publish
     * @return a future completing on broker ack, or exceptionally with {@link
     *     com.codingful.tandem.core.exception.OutboxDispatchException}
     */
    CompletableFuture<Void> dispatch(OutboxRecord record);

    /**
     * The effective end-to-end delivery timeout this dispatcher enforces per record, if it has one.
     *
     * <p>When present, the relay treats this as the <b>authoritative</b> value for the
     * {@code rowLease > deliveryTimeout} startup invariant (LLD-jdbc §3.5), overriding the value
     * configured on {@code RelayConfig} — so an operator cannot leave the two out of sync and
     * silently pass the safety check against a stale default (the Kafka dispatcher reports its own
     * {@code delivery.timeout.ms} here). Dispatchers without such a concept return
     * {@link OptionalLong#empty()} and the relay falls back to the configured value.
     *
     * @return the enforced delivery timeout in milliseconds, or empty when the dispatcher has none
     */
    default OptionalLong deliveryTimeoutMillis() {
        return OptionalLong.empty();
    }
}
