package com.codingful.tandem.benchmark;

import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.exception.OutboxDispatchException;
import com.codingful.tandem.core.port.OutboxDispatcher;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Wraps the real dispatcher and applies whatever fault {@link FaultInjector} currently flags for a
 * record (S6, LLD-benchmark §8) — the same forced-fail capability {@code RecordingDispatcher} offers
 * in {@code tandem-test}, applied to the real {@code KafkaRelay} instead of an in-memory stand-in.
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
        return switch (faultInjector.faultFor(record)) {
            case NONE -> delegate.dispatch(record);
            case RETRIABLE -> CompletableFuture.failedFuture(
                    new OutboxDispatchException("injected retriable failure", true));
            case PERMANENT -> CompletableFuture.failedFuture(
                    new OutboxDispatchException("injected permanent poison failure", false));
            // Deliberately never completed and never cancelled: the row stays IN_FLIGHT until its lease
            // expires, which is the state the reclaim path needs to see.
            case STALL -> new CompletableFuture<>();
        };
    }
}
