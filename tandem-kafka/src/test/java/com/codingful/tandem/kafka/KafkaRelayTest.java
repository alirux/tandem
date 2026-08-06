package com.codingful.tandem.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.TandemHeaders;
import com.codingful.tandem.core.exception.OutboxDispatchException;
import com.codingful.tandem.core.port.TandemSpanRecorder;
import com.codingful.tandem.core.port.TopicRouter;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
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

    private static final OutboxRecord RECORD_WITH_TRACE = OutboxRecord.builder()
            .id(2)
            .attempts(1)
            .message(OutboxMessage.builder()
                    .aggregateId("order-2").aggregateType("Order").seq(1).payload("{}".getBytes())
                    .header(TandemHeaders.TRACEPARENT, "00-trace-1")
                    .header(TandemHeaders.TRACESTATE, "vendor=1")
                    .header(TandemHeaders.CORRELATION_ID, "corr-1")
                    .build())
            .createdAt(Instant.parse("2024-01-01T00:00:00Z"))
            .build();

    private static MockProducer<String, byte[]> mockProducer(boolean autoComplete) {
        return new MockProducer<>(autoComplete, new StringSerializer(), new ByteArraySerializer());
    }

    private static KafkaRelay relayOver(MockProducer<String, byte[]> producer) {
        return new KafkaRelay(producer, TopicRouter.kebabWithSuffix("-topic"),
                KafkaRelayConfig.of("/tandem/orders"), new DefaultErrorClassifier(), 30_000);
    }

    private static KafkaRelay relayOver(MockProducer<String, byte[]> producer, TandemSpanRecorder spanRecorder) {
        return new KafkaRelay(producer, TopicRouter.kebabWithSuffix("-topic"),
                KafkaRelayConfig.of("/tandem/orders"), new DefaultErrorClassifier(), 30_000, spanRecorder);
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

    @Test
    void GIVEN_instrumented_mode_and_a_record_with_captured_trace_context_WHEN_dispatched_THEN_the_span_is_started_with_that_context() {
        RecordingSpanRecorder spanRecorder = new RecordingSpanRecorder();

        relayOver(mockProducer(true), spanRecorder).dispatch(RECORD_WITH_TRACE);

        assertThat(spanRecorder.started).hasSize(1);
        RecordingSpanRecorder.StartedSpan started = spanRecorder.started.get(0);
        assertThat(started.rowId()).isEqualTo(2);
        assertThat(started.aggregateType()).isEqualTo("Order");
        assertThat(started.aggregateId()).isEqualTo("order-2");
        assertThat(started.attempts()).isEqualTo(1);
        assertThat(started.topic()).isEqualTo("order-topic");
        assertThat(started.traceparent()).isEqualTo("00-trace-1");
        assertThat(started.tracestate()).isEqualTo("vendor=1");
        // The bridge between a tracing-backend investigation and the Admin API's search by the same id.
        assertThat(started.correlationId()).isEqualTo("corr-1");
    }

    @Test
    void GIVEN_instrumented_mode_and_a_record_with_no_captured_trace_context_WHEN_dispatched_THEN_the_span_is_started_without_it() {
        RecordingSpanRecorder spanRecorder = new RecordingSpanRecorder();

        relayOver(mockProducer(true), spanRecorder).dispatch(RECORD);

        RecordingSpanRecorder.StartedSpan started = spanRecorder.started.get(0);
        assertThat(started.traceparent()).isNull();
        assertThat(started.tracestate()).isNull();
        assertThat(started.correlationId()).isNull();
    }

    @Test
    void GIVEN_instrumented_mode_WHEN_the_broker_acks_THEN_the_span_ends_successfully() {
        RecordingSpanRecorder spanRecorder = new RecordingSpanRecorder();

        relayOver(mockProducer(true), spanRecorder).dispatch(RECORD);

        assertThat(spanRecorder.spans).hasSize(1);
        assertThat(spanRecorder.spans.get(0).succeeded).isTrue();
        assertThat(spanRecorder.spans.get(0).failure).isNull();
    }

    @Test
    void GIVEN_instrumented_mode_WHEN_the_broker_rejects_the_send_THEN_the_span_ends_with_the_failure() {
        RecordingSpanRecorder spanRecorder = new RecordingSpanRecorder();
        MockProducer<String, byte[]> producer = mockProducer(false);
        CompletableFuture<Void> ack = relayOver(producer, spanRecorder).dispatch(RECORD);

        TimeoutException failure = new TimeoutException("broker unavailable");
        producer.errorNext(failure);
        catchDispatchException(ack);

        assertThat(spanRecorder.spans.get(0).succeeded).isFalse();
        assertThat(spanRecorder.spans.get(0).failure).isSameAs(failure);
    }

    @Test
    void GIVEN_instrumented_mode_WHEN_the_producer_rejects_the_send_synchronously_THEN_the_span_ends_with_the_failure() {
        RecordingSpanRecorder spanRecorder = new RecordingSpanRecorder();
        RuntimeException failure = new BufferExhaustedException("buffer full");
        CompletableFuture<Void> ack = new KafkaRelay(rejectingProducer(failure), TopicRouter.kebabWithSuffix("-topic"),
                KafkaRelayConfig.of("/tandem/orders"), new DefaultErrorClassifier(), 30_000, spanRecorder)
                .dispatch(RECORD);

        catchDispatchException(ack);

        assertThat(spanRecorder.spans.get(0).failure).isSameAs(failure);
    }

    @Test
    void GIVEN_instrumented_mode_disabled_WHEN_dispatched_THEN_no_span_is_started() {
        RecordingSpanRecorder spanRecorder = new RecordingSpanRecorder();
        spanRecorder.enabled = false;

        relayOver(mockProducer(true), spanRecorder).dispatch(RECORD);

        assertThat(spanRecorder.started).isEmpty();
    }

    /** A real, non-NOOP {@link TandemSpanRecorder} standing in for tandem-spring-relay/tandem-tracing-otel. */
    private static final class RecordingSpanRecorder implements TandemSpanRecorder {

        record StartedSpan(long rowId, String aggregateType, String aggregateId, int attempts,
                String topic, String traceparent, String tracestate, String correlationId) {
        }

        boolean enabled = true;
        final List<StartedSpan> started = new ArrayList<>();
        final List<RecordingSpan> spans = new ArrayList<>();

        @Override
        public boolean isEnabled() {
            return enabled;
        }

        @Override
        public Span startPublishSpan(long rowId, String aggregateType, String aggregateId, int attempts,
                String topic, String traceparent, String tracestate, String correlationId) {
            started.add(new StartedSpan(rowId, aggregateType, aggregateId, attempts, topic, traceparent, tracestate,
                    correlationId));
            RecordingSpan span = new RecordingSpan();
            spans.add(span);
            return span;
        }

        static final class RecordingSpan implements Span {
            boolean succeeded;
            Throwable failure;

            @Override
            public void end() {
                succeeded = true;
            }

            @Override
            public void end(Throwable f) {
                failure = f;
            }
        }
    }
}
