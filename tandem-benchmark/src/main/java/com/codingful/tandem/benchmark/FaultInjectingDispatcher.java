package com.codingful.tandem.benchmark;

import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.exception.OutboxDispatchException;
import com.codingful.tandem.core.port.OutboxDispatcher;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Wraps the real dispatcher and permanently fails any record {@link FaultInjector} currently flags
 * (S6, LLD-benchmark §8) — the same forced-fail capability {@code RecordingDispatcher} offers in
 * {@code tandem-test}, applied to the real {@code KafkaRelay} instead of an in-memory stand-in.
 */
final class FaultInjectingDispatcher implements OutboxDispatcher {

    private final OutboxDispatcher delegate;
    private final FaultInjector faultInjector;

    FaultInjectingDispatcher(OutboxDispatcher delegate, FaultInjector faultInjector) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.faultInjector = Objects.requireNonNull(faultInjector, "faultInjector");
    }

    @Override
    public CompletableFuture<Void> dispatch(OutboxRecord record) {
        if (faultInjector.test(record)) {
            CompletableFuture<Void> failure = new CompletableFuture<>();
            failure.completeExceptionally(
                    new OutboxDispatchException("injected permanent poison failure", false));
            return failure;
        }
        return delegate.dispatch(record);
    }
}
