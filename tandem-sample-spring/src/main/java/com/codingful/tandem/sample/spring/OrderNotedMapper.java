package com.codingful.tandem.sample.spring;

import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.spring.producer.OutboxEventMapper;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import org.springframework.stereotype.Component;

/**
 * The {@link OutboxEventMapper} for the events tier: registering this bean is what opts {@link OrderNoted}
 * into the outbox. Without a mapper the listener simply ignores the event (Spring's no-listener-no-op
 * semantics, LLD-spring-producer §5).
 */
@Component
class OrderNotedMapper implements OutboxEventMapper<OrderNoted> {

    @Override
    public Collection<OutboxMessage> map(OrderNoted event) {
        return List.of(OutboxMessage.builder()
                .aggregateType("Order")
                .aggregateId(event.orderId())
                .type("OrderNoted")
                .seq(event.seq())
                .payload(("{\"order\":\"" + event.orderId() + "\",\"event\":\"OrderNoted\"}")
                        .getBytes(StandardCharsets.UTF_8))
                .contentType("application/json")
                .build());
    }
}
