package com.codingful.tandem.sample.spring;

import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.port.TandemAggregate;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A tiny domain aggregate that accumulates the outbox messages it produces — the annotation tier's
 * contract ({@link TandemAggregate}, HLD §3.1). Each mutation advances the aggregate's version and
 * records one event with {@code seq = version}; Tandem reads that seq, it never invents it.
 *
 * <p>[DEMO-ONLY] A real aggregate would be a JPA/domain entity; here it is loaded fresh per operation
 * from the in-memory {@link OrderStore}, so its pending list holds exactly the current operation's event.
 */
final class Order implements TandemAggregate {

    private final String id;
    private long version;
    private final List<OutboxMessage> pending = new ArrayList<>();

    Order(String id, long version) {
        this.id = id;
        this.version = version;
    }

    void place() {
        record("OrderPlaced");
    }

    void confirm() {
        record("OrderConfirmed");
    }

    void ship() {
        record("OrderShipped");
    }

    private void record(String eventType) {
        version++;
        pending.add(OutboxMessage.builder()
                .aggregateType("Order")
                .aggregateId(id)
                .type(eventType)
                .seq(version)
                .payload(("{\"order\":\"" + id + "\",\"event\":\"" + eventType + "\"}")
                        .getBytes(StandardCharsets.UTF_8))
                .contentType("application/json")
                .build());
    }

    @Override
    public Collection<OutboxMessage> pendingOutboxMessages() {
        return pending;
    }

    String id() {
        return id;
    }

    long version() {
        return version;
    }
}
