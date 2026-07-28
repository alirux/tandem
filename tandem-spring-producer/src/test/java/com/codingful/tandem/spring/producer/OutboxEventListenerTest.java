package com.codingful.tandem.spring.producer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.OutboxStatus;
import com.codingful.tandem.core.exception.OutboxInsertException;
import com.codingful.tandem.test.InMemoryOutbox;
import java.util.Collection;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.PayloadApplicationEvent;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.core.ResolvableType;
import org.springframework.transaction.support.TransactionSynchronizationManager;

class OutboxEventListenerTest {

    private record OrderPlaced(String orderId) {
    }

    private static final class OrderPlacedMapper implements OutboxEventMapper<OrderPlaced> {
        @Override
        public Collection<OutboxMessage> map(OrderPlaced event) {
            return List.of(OutboxMessage.builder()
                    .aggregateType("Order").aggregateId(event.orderId()).seq(1L).payload(new byte[] {1}).build());
        }
    }

    private record Ignored(String id) {
    }

    private static final class SilentMapper implements OutboxEventMapper<Ignored> {
        @Override
        public Collection<OutboxMessage> map(Ignored event) {
            return List.of();
        }
    }

    private final InMemoryOutbox outbox = new InMemoryOutbox();
    private final OutboxEventListener listener = new OutboxEventListener(
            outbox, OutboxEventMapperRegistry.of(List.of(new OrderPlacedMapper())));

    private static void withActiveTransaction(Runnable action) {
        TransactionSynchronizationManager.setActualTransactionActive(true);
        try {
            action.run();
        } finally {
            TransactionSynchronizationManager.setActualTransactionActive(false);
        }
    }

    private static PayloadApplicationEvent<Object> event(Object payload) {
        return new PayloadApplicationEvent<>("test-source", payload);
    }

    @Test
    void GIVEN_a_payload_with_a_mapper_or_an_outbox_message_WHEN_asked_THEN_the_event_is_supported() {
        assertThat(listener.supportsEventType(
                ResolvableType.forClassWithGenerics(PayloadApplicationEvent.class, OrderPlaced.class))).isTrue();
        assertThat(listener.supportsEventType(
                ResolvableType.forClassWithGenerics(PayloadApplicationEvent.class, OutboxMessage.class))).isTrue();
    }

    @Test
    void GIVEN_an_unmapped_payload_or_a_framework_event_WHEN_asked_THEN_the_event_is_not_supported() {
        assertThat(listener.supportsEventType(
                ResolvableType.forClassWithGenerics(PayloadApplicationEvent.class, String.class))).isFalse();
        assertThat(listener.supportsEventType(ResolvableType.forClass(ContextRefreshedEvent.class))).isFalse();
    }

    /**
     * A domain event that extends {@code ApplicationEvent} is published unwrapped, so the tier does not
     * see it even with a mapper registered — the documented boundary of this tier (LLD-spring-producer §5).
     */
    @Test
    void GIVEN_a_mapped_event_published_as_its_own_application_event_WHEN_asked_THEN_it_is_not_supported() {
        assertThat(listener.supportsEventType(ResolvableType.forClass(SelfPublishingOrderPlaced.class))).isFalse();
    }

    private static final class SelfPublishingOrderPlaced extends ApplicationEvent {
        private SelfPublishingOrderPlaced() {
            super("test-source");
        }
    }

    @Test
    void GIVEN_an_active_transaction_WHEN_a_mapped_event_is_handled_THEN_the_mapped_rows_are_inserted() {
        withActiveTransaction(() -> listener.onApplicationEvent(event(new OrderPlaced("order-1"))));

        assertThat(outbox.byStatus(OutboxStatus.PENDING))
                .singleElement()
                .satisfies(record -> assertThat(record.aggregateId().value()).isEqualTo("order-1"));
    }

    @Test
    void GIVEN_an_active_transaction_WHEN_an_outbox_message_is_published_directly_THEN_it_is_inserted() {
        OutboxMessage message = OutboxMessage.builder()
                .aggregateType("Order").aggregateId("order-2").seq(1L).payload(new byte[] {2}).build();

        withActiveTransaction(() -> listener.onApplicationEvent(event(message)));

        assertThat(outbox.byStatus(OutboxStatus.PENDING))
                .singleElement()
                .satisfies(record -> assertThat(record.aggregateId().value()).isEqualTo("order-2"));
    }

    @Test
    void GIVEN_no_active_transaction_WHEN_a_mapped_event_is_handled_THEN_it_fails_fast() {
        assertThatThrownBy(() -> listener.onApplicationEvent(event(new OrderPlaced("order-3"))))
                .isInstanceOf(OutboxInsertException.class);
        assertThat(outbox.all()).isEmpty();
    }

    @Test
    void GIVEN_a_mapper_that_emits_nothing_WHEN_its_event_is_handled_THEN_nothing_is_inserted() {
        OutboxEventListener quietListener =
                new OutboxEventListener(outbox, OutboxEventMapperRegistry.of(List.of(new SilentMapper())));

        // No active transaction, yet no failure: an empty result short-circuits before the tx check.
        quietListener.onApplicationEvent(event(new Ignored("order-4")));

        assertThat(outbox.all()).isEmpty();
    }
}
