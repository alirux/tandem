package com.codingful.tandem.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.exception.OutboxDispatchException;
import com.codingful.tandem.core.port.TopicRouter;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.clients.producer.BufferExhaustedException;
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

    @Test
    void GIVEN_an_event_that_cannot_be_encoded_WHEN_the_relay_publishes_THEN_it_fails_permanently_without_reaching_the_broker() {
        MockProducer<String, byte[]> producer = mockProducer(true);
        TopicRouter unroutable = record -> {
            throw new IllegalStateException("no topic for " + record.aggregateType());
        };
        KafkaRelay relay = new KafkaRelay(producer, unroutable, KafkaRelayConfig.of("/tandem/orders"),
                new DefaultErrorClassifier(), 30_000);

        CompletableFuture<Void> ack = relay.dispatch(RECORD);

        // Re-encoding the same row fails the same way, so a retry ladder would only keep the
        // aggregate's chain blocked; the verdict must be permanent even though the broker-error
        // classifier would call this unknown exception retriable.
        OutboxDispatchException failure = catchDispatchException(ack);
        assertThat(failure.isRetriable()).isFalse();
        assertThat(failure).hasRootCauseInstanceOf(IllegalStateException.class);
        assertThat(producer.history()).isEmpty();   // the row is never half-published
    }

    @Test
    void GIVEN_a_producer_out_of_buffer_space_WHEN_it_rejects_the_send_outright_THEN_the_failure_is_retriable() {
        CompletableFuture<Void> ack = relayOver(rejectingProducer(new BufferExhaustedException("buffer full")))
                .dispatch(RECORD);

        assertThat(catchDispatchException(ack).isRetriable()).isTrue();
    }

    @Test
    void GIVEN_an_unserializable_event_WHEN_the_producer_rejects_the_send_outright_THEN_the_failure_is_permanent() {
        CompletableFuture<Void> ack = relayOver(rejectingProducer(new SerializationException("not serializable")))
                .dispatch(RECORD);

        assertThat(catchDispatchException(ack).isRetriable()).isFalse();
    }

    /** A producer whose {@code send} throws instead of returning a future — the synchronous failure path. */
    private static MockProducer<String, byte[]> rejectingProducer(RuntimeException failure) {
        return new MockProducer<>(true, new StringSerializer(), new ByteArraySerializer()) {
            @Override
            public Future<RecordMetadata> send(ProducerRecord<String, byte[]> record, Callback callback) {
                throw failure;
            }
        };
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
