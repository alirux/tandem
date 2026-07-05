package com.codingful.tandem.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.exception.OutboxDispatchException;
import com.codingful.tandem.core.port.TopicRouter;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

class KafkaRelayTest {

    private static final OutboxRecord RECORD = OutboxRecord.builder()
            .id(1)
            .message(OutboxMessage.builder()
                    .aggregateId("order-1").aggregateType("Order").seq(1).payload("{}".getBytes()).build())
            .createdAt(Instant.parse("2024-01-01T00:00:00Z"))
            .build();

    private static MockProducer<String, byte[]> mockProducer(boolean autoComplete) {
        return new MockProducer<>(autoComplete, new StringSerializer(), new ByteArraySerializer());
    }

    private static KafkaRelay relayOver(MockProducer<String, byte[]> producer) {
        return new KafkaRelay(producer, TopicRouter.kebabWithSuffix("-topic"),
                KafkaRelayConfig.of("/tandem/orders"), new DefaultErrorClassifier(), 30_000);
    }

    @Test
    void GIVEN_a_record_WHEN_the_broker_acks_THEN_the_future_completes_and_the_event_is_sent() {
        MockProducer<String, byte[]> producer = mockProducer(true);

        CompletableFuture<Void> ack = relayOver(producer).dispatch(RECORD);

        assertThat(ack).isCompleted();
        assertThat(producer.history()).hasSize(1);
        assertThat(producer.history().get(0).topic()).isEqualTo("order-topic");
    }

    @Test
    void GIVEN_an_in_flight_send_WHEN_the_broker_fails_transiently_THEN_the_future_carries_a_retriable_verdict() {
        MockProducer<String, byte[]> producer = mockProducer(false);
        CompletableFuture<Void> ack = relayOver(producer).dispatch(RECORD);

        assertThat(ack).isNotDone();   // async: not settled until the broker responds
        producer.errorNext(new TimeoutException("broker unavailable"));

        assertThat(catchDispatchException(ack).isRetriable()).isTrue();
    }

    @Test
    void GIVEN_an_in_flight_send_WHEN_the_broker_rejects_permanently_THEN_the_future_carries_a_permanent_verdict() {
        MockProducer<String, byte[]> producer = mockProducer(false);
        CompletableFuture<Void> ack = relayOver(producer).dispatch(RECORD);

        producer.errorNext(new RecordTooLargeException("too big"));

        assertThat(catchDispatchException(ack).isRetriable()).isFalse();
    }

    private static OutboxDispatchException catchDispatchException(CompletableFuture<Void> future) {
        try {
            future.join();
            throw new AssertionError("expected the future to complete exceptionally");
        } catch (CompletionException e) {
            assertThat(e.getCause()).isInstanceOf(OutboxDispatchException.class);
            return (OutboxDispatchException) e.getCause();
        }
    }
}
