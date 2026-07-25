package com.codingful.tandem.spring.producer;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * The Template tier (HLD §3.1, LLD-spring-producer §3): wraps <em>transaction + collect + insert</em> in
 * one call. The unit of work receives an {@link OutboxCollector}, records the rows to emit, and the
 * template inserts them within the transaction it owns — so the business state change and the outbox
 * rows commit or roll back atomically. The domain objects stay free of any Tandem type.
 */
public interface TransactionalOutboxTemplate {

    /**
     * Run {@code work} inside a new transaction, insert everything it recorded into the outbox in the
     * same transaction, and return the work's result. A runtime exception from {@code work} or from the
     * insert rolls the whole transaction back.
     *
     * @param work the unit of work; records outbox rows via the supplied {@link OutboxCollector}
     * @param <T>  the work's result type
     * @return the value {@code work} returned
     */
    <T> T execute(Function<OutboxCollector, T> work);

    /**
     * As {@link #execute(Function)} for a unit of work that returns no value.
     *
     * @param work the unit of work; records outbox rows via the supplied {@link OutboxCollector}
     */
    default void executeWithoutResult(Consumer<OutboxCollector> work) {
        execute(collector -> {
            work.accept(collector);
            return null;
        });
    }
}
