package com.codingful.tandem.spring.producer;

import com.codingful.tandem.core.OutboxMessage;
import java.util.Collection;

/**
 * SPI for the Spring application-events tier (HLD §3.1, LLD-spring-producer §5): converts a published
 * domain event into the outbox rows it produces. Registering a mapper bean is how a domain event opts
 * into the outbox — the listener is scoped to {@link OutboxMessage} plus the types that have a mapper,
 * so events without one are ignored (Spring's own no-listener-no-op semantics).
 *
 * @param <T> the domain event type this mapper handles
 */
public interface OutboxEventMapper<T> {

    /**
     * Map one domain event to the outbox rows it produces.
     *
     * @param event the published domain event
     * @return the rows to insert — usually one; an empty collection emits nothing; never {@code null}.
     *         Each message's {@code seq} must come from the aggregate's {@code version} (HLD §4.2)
     */
    Collection<OutboxMessage> map(T event);
}
