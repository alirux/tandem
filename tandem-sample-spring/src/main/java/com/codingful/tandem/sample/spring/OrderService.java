package com.codingful.tandem.sample.spring;

import com.codingful.tandem.spring.producer.TransactionalOutbox;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * The write side, in idiomatic Spring — the point of this sample.
 *
 * <p>The lifecycle methods carry only {@link TransactionalOutbox}: no explicit transaction, no explicit
 * outbox call. Mutate the aggregate, return it, and Tandem inserts its pending events in the <em>same</em>
 * transaction the annotation opened. {@link #noteViaEvent} shows the alternative Spring application-events
 * tier — publish a domain event and let a registered mapper turn it into an outbox row.
 */
@Service
public class OrderService {

    private final OrderStore store;
    private final ApplicationEventPublisher events;

    OrderService(OrderStore store, ApplicationEventPublisher events) {
        this.store = store;
        this.events = events;
    }

    @TransactionalOutbox
    public Order place(String id) {
        Order order = store.load(id);
        order.place();
        store.save(order);
        return order;
    }

    @TransactionalOutbox
    public Order confirm(String id) {
        Order order = store.load(id);
        order.confirm();
        store.save(order);
        return order;
    }

    @TransactionalOutbox
    public Order ship(String id) {
        Order order = store.load(id);
        order.ship();
        store.save(order);
        return order;
    }

    /**
     * The Spring application-events tier: publish a domain event inside a transaction; the registered
     * {@link OrderNotedMapper} turns it into an outbox row, inserted in this same transaction.
     */
    @Transactional
    public void noteViaEvent(String id, long seq) {
        events.publishEvent(new OrderNoted(id, seq));
    }
}
