package com.codingful.tandem.spring.producer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.exception.OutboxInsertException;
import com.codingful.tandem.core.port.TandemAggregate;
import java.util.Collection;
import java.util.List;
import org.junit.jupiter.api.Test;

class TransactionalOutboxAspectTest {

    private record TestAggregate(Collection<OutboxMessage> pendingOutboxMessages) implements TandemAggregate {
    }

    private static OutboxMessage message(String aggregateType, long seq) {
        return OutboxMessage.builder()
                .aggregateType(aggregateType).aggregateId("id-" + seq).seq(seq).payload(new byte[] {(byte) seq})
                .build();
    }

    @Test
    void GIVEN_a_single_aggregate_WHEN_extracting_THEN_its_pending_messages_are_returned() {
        OutboxMessage m = message("Order", 1L);
        TestAggregate aggregate = new TestAggregate(List.of(m));

        assertThat(TransactionalOutboxAspect.extract(aggregate)).containsExactly(m);
    }

    @Test
    void GIVEN_an_iterable_of_aggregates_WHEN_extracting_THEN_all_messages_are_concatenated_in_order() {
        OutboxMessage first = message("Order", 1L);
        OutboxMessage second = message("Order", 2L);
        List<TestAggregate> aggregates = List.of(new TestAggregate(List.of(first)), new TestAggregate(List.of(second)));

        assertThat(TransactionalOutboxAspect.extract(aggregates)).containsExactly(first, second);
    }

    @Test
    void GIVEN_an_iterable_with_non_aggregate_elements_WHEN_extracting_THEN_they_are_ignored() {
        OutboxMessage m = message("Order", 1L);
        List<Object> mixed = List.of(new TestAggregate(List.of(m)), "not an aggregate");

        assertThat(TransactionalOutboxAspect.extract(mixed)).containsExactly(m);
    }

    @Test
    void GIVEN_a_return_value_that_is_not_an_aggregate_WHEN_extracting_THEN_nothing_is_extracted() {
        assertThat(TransactionalOutboxAspect.extract(null)).isEmpty();
        assertThat(TransactionalOutboxAspect.extract("plain result")).isEmpty();
    }

    @Test
    void GIVEN_a_declared_aggregate_type_WHEN_a_message_matches_THEN_the_guard_passes() {
        assertThatCode(() -> TransactionalOutboxAspect.guardAggregateType(List.of(message("Order", 1L)), "Order"))
                .doesNotThrowAnyException();
    }

    @Test
    void GIVEN_a_declared_aggregate_type_WHEN_a_message_diverges_THEN_the_guard_fails_fast() {
        assertThatThrownBy(() -> TransactionalOutboxAspect.guardAggregateType(List.of(message("Customer", 1L)), "Order"))
                .isInstanceOf(OutboxInsertException.class);
    }

    @Test
    void GIVEN_no_declared_aggregate_type_WHEN_guarding_THEN_any_message_is_accepted() {
        assertThatCode(() -> TransactionalOutboxAspect.guardAggregateType(List.of(message("Customer", 1L)), ""))
                .doesNotThrowAnyException();
    }
}
