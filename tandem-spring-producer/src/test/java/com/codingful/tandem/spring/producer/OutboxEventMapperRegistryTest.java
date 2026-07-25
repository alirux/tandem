package com.codingful.tandem.spring.producer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.exception.OutboxInsertException;
import java.util.Collection;
import java.util.List;
import org.junit.jupiter.api.Test;

class OutboxEventMapperRegistryTest {

    private record OrderPlaced(String orderId) {
    }

    private static class BaseEvent {
    }

    private static final class SubEvent extends BaseEvent {
    }

    private static OutboxMessage message(String aggregateType) {
        return OutboxMessage.builder()
                .aggregateType(aggregateType).aggregateId("id").seq(1L).payload(new byte[] {1}).build();
    }

    private static final class OrderPlacedMapper implements OutboxEventMapper<OrderPlaced> {
        @Override
        public Collection<OutboxMessage> map(OrderPlaced event) {
            return List.of(message("Order"));
        }
    }

    private static final class BaseEventMapper implements OutboxEventMapper<BaseEvent> {
        @Override
        public Collection<OutboxMessage> map(BaseEvent event) {
            return List.of(message("Base"));
        }
    }

    private static final class SubEventMapper implements OutboxEventMapper<SubEvent> {
        @Override
        public Collection<OutboxMessage> map(SubEvent event) {
            return List.of(message("Sub"));
        }
    }

    @Test
    void GIVEN_a_registered_mapper_WHEN_asked_for_its_event_type_THEN_it_is_handled_and_mapped() {
        OutboxEventMapperRegistry registry = OutboxEventMapperRegistry.of(List.of(new OrderPlacedMapper()));

        assertThat(registry.handles(OrderPlaced.class)).isTrue();
        assertThat(registry.map(new OrderPlaced("order-1"))).containsExactly(message("Order"));
    }

    @Test
    void GIVEN_no_mapper_for_a_type_WHEN_asked_THEN_it_is_not_handled_and_mapping_fails_fast() {
        OutboxEventMapperRegistry registry = OutboxEventMapperRegistry.of(List.of(new OrderPlacedMapper()));

        assertThat(registry.handles(String.class)).isFalse();
        assertThatThrownBy(() -> registry.map("unmapped")).isInstanceOf(OutboxInsertException.class);
    }

    @Test
    void GIVEN_mappers_for_a_supertype_and_a_subtype_WHEN_the_subtype_is_published_THEN_the_subtype_mapper_wins() {
        OutboxEventMapperRegistry registry =
                OutboxEventMapperRegistry.of(List.of(new BaseEventMapper(), new SubEventMapper()));

        assertThat(registry.map(new SubEvent())).containsExactly(message("Sub"));
    }

    @Test
    void GIVEN_only_a_supertype_mapper_WHEN_a_subtype_is_published_THEN_it_maps_via_the_supertype() {
        OutboxEventMapperRegistry registry = OutboxEventMapperRegistry.of(List.of(new BaseEventMapper()));

        assertThat(registry.handles(SubEvent.class)).isTrue();
        assertThat(registry.map(new SubEvent())).containsExactly(message("Base"));
    }

    @Test
    void GIVEN_two_mappers_for_the_same_event_type_WHEN_the_registry_is_built_THEN_it_fails_fast() {
        assertThatThrownBy(() -> OutboxEventMapperRegistry.of(List.of(new OrderPlacedMapper(), new OrderPlacedMapper())))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void GIVEN_a_mapper_that_returns_null_WHEN_mapping_THEN_it_fails_fast() {
        OutboxEventMapper<OrderPlaced> nullMapper = new OutboxEventMapper<OrderPlaced>() {
            @Override
            public Collection<OutboxMessage> map(OrderPlaced event) {
                return null;
            }
        };
        OutboxEventMapperRegistry registry = OutboxEventMapperRegistry.of(List.of(nullMapper));

        assertThatThrownBy(() -> registry.map(new OrderPlaced("order-2"))).isInstanceOf(OutboxInsertException.class);
    }
}
