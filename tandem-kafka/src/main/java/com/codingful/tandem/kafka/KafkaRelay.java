package com.codingful.tandem.kafka;

import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.TandemHeaders;
import com.codingful.tandem.core.exception.OutboxDispatchException;
import com.codingful.tandem.core.port.OutboxDispatcher;
import com.codingful.tandem.core.port.TandemSpanRecorder;
import com.codingful.tandem.core.port.TopicRouter;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The publish adapter (LLD-kafka §2): implements {@link OutboxDispatcher} by building a CloudEvent
 * from an {@link OutboxRecord} and sending it <b>asynchronously</b> on one Kafka producer. The
 * returned future completes on the broker ack ({@code acks=all}), or completes <b>exceptionally</b>
 * with an {@code OutboxDispatchException} carrying the retriable/permanent verdict (§4) — never
 * blocking, so the relay overlaps many records of distinct aggregates on a single producer.
 */
public final class KafkaRelay implements OutboxDispatcher, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaRelay.class);

    private final Producer<String, byte[]> producer;
    private final CloudEventEncoder encoder;
    private final ErrorClassifier classifier;
    private final long deliveryTimeoutMs;
    private final TandemSpanRecorder spanRecorder;

    /**
     * Builds a producer from {@code producerConfig}, hardening it to the mandated safe values (fails
     * fast on unsafe overrides, §1). Delegates to the 4-arg constructor with
     * {@link TandemSpanRecorder#NOOP} — instrumented mode disabled (HLD-tracing.md §7).
     *
     * @param producerConfig raw Kafka producer properties; {@code acks}/idempotence/etc. are hardened, not user-overridable
     * @param router         maps each record to its destination topic
     * @param cfg            CloudEvents binding settings ({@code source}, default content type/schema)
     * @throws com.codingful.tandem.core.exception.TandemConfigurationException if {@code producerConfig} overrides a mandated safe value
     */
    public KafkaRelay(Map<String, ?> producerConfig, TopicRouter router, KafkaRelayConfig cfg) {
        this(producerConfig, router, cfg, TandemSpanRecorder.NOOP);
    }

    /**
     * @param producerConfig raw Kafka producer properties; see {@link #KafkaRelay(Map, TopicRouter, KafkaRelayConfig)}
     * @param router         maps each record to its destination topic
     * @param cfg            CloudEvents binding settings ({@code source}, default content type/schema)
     * @param spanRecorder   emits the {@code tandem.relay.publish} span per record when
     *                       {@link TandemSpanRecorder#isEnabled()} (HLD-tracing.md §6, §9's "instrumented
     *                       mode"); real adapters ship in {@code tandem-spring-relay} / {@code tandem-tracing-otel}
     * @throws com.codingful.tandem.core.exception.TandemConfigurationException if {@code producerConfig} overrides a mandated safe value
     * @throws NullPointerException     if {@code spanRecorder} is {@code null}
     */
    public KafkaRelay(Map<String, ?> producerConfig, TopicRouter router, KafkaRelayConfig cfg,
            TandemSpanRecorder spanRecorder) {
        Map<String, Object> hardened = KafkaProducerConfig.harden(producerConfig);
        this.producer = new KafkaProducer<>(hardened);
        this.encoder = new CloudEventEncoder(router, cfg);
        this.classifier = new DefaultErrorClassifier();
        this.deliveryTimeoutMs = KafkaProducerConfig.deliveryTimeoutMs(hardened);
        this.spanRecorder = Objects.requireNonNull(spanRecorder, "spanRecorder");
    }

    /** For tests: inject a producer (e.g. Kafka's {@code MockProducer}) and classifier directly. */
    KafkaRelay(Producer<String, byte[]> producer, TopicRouter router, KafkaRelayConfig cfg,
               ErrorClassifier classifier, long deliveryTimeoutMs) {
        this(producer, router, cfg, classifier, deliveryTimeoutMs, TandemSpanRecorder.NOOP);
    }

    /** For tests: also inject a {@link TandemSpanRecorder}. */
    KafkaRelay(Producer<String, byte[]> producer, TopicRouter router, KafkaRelayConfig cfg,
               ErrorClassifier classifier, long deliveryTimeoutMs, TandemSpanRecorder spanRecorder) {
        this.producer = Objects.requireNonNull(producer, "producer");
        this.encoder = new CloudEventEncoder(router, cfg);
        this.classifier = Objects.requireNonNull(classifier, "classifier");
        this.deliveryTimeoutMs = deliveryTimeoutMs;
        this.spanRecorder = Objects.requireNonNull(spanRecorder, "spanRecorder");
    }

    @Override
    public CompletableFuture<Void> dispatch(OutboxRecord record) {
        CompletableFuture<Void> ack = new CompletableFuture<>();
        ProducerRecord<String, byte[]> producerRecord;
        try {
            producerRecord = encoder.encode(record);
        } catch (RuntimeException encodeFailure) {
            // Permanent by construction, and deliberately NOT routed through the classifier: that maps
            // *broker* errors and defaults an unknown exception to retriable, whereas re-encoding the
            // same row always fails the same way. Retrying would only keep the aggregate's chain
            // blocked for the whole backoff ladder while the row still looks merely PENDING (§4).
            LOG.error("Encoding outbox row failed rowId:{}, aggregateType:{}, aggregateId:{}", record.id(),
                    record.aggregateType(), record.aggregateId(), encodeFailure);
            ack.completeExceptionally(permanent(encodeFailure));
            return ack;
        }
        // §6.2: the relay dispatches asynchronously and does not await per record, so the span handle
        // is carried explicitly alongside the send rather than relying on a thread-local scope, and
        // ended from the completion callback below — never synchronously here.
        TandemSpanRecorder.Span span = spanRecorder.isEnabled()
                ? spanRecorder.startPublishSpan(record.id(), record.aggregateType(), record.aggregateId().value(),
                        record.attempts(), producerRecord.topic(),
                        record.headers().get(TandemHeaders.TRACEPARENT), record.headers().get(TandemHeaders.TRACESTATE),
                        record.headers().get(TandemHeaders.CORRELATION_ID))
                : TandemSpanRecorder.Span.NOOP;
        try {
            producer.send(producerRecord, (metadata, exception) -> {
                if (exception == null) {
                    span.end();
                    ack.complete(null);
                } else {
                    LOG.error("Publishing outbox row failed rowId:{}, topic:{}", record.id(),
                            producerRecord.topic(), exception);
                    span.end(exception);
                    ack.completeExceptionally(classifier.classify(exception));
                }
            });
        } catch (RuntimeException sendThrew) {
            // send() can throw synchronously (serialization, buffer exhaustion) — classify it too.
            LOG.error("Sending outbox row failed synchronously rowId:{}, topic:{}", record.id(),
                    producerRecord.topic(), sendThrew);
            span.end(sendThrew);
            ack.completeExceptionally(classifier.classify(sendThrew));
        }
        return ack;
    }

    private static OutboxDispatchException permanent(RuntimeException encodeFailure) {
        String detail = encodeFailure.getMessage() == null ? "" : " - " + encodeFailure.getMessage();
        return new OutboxDispatchException("Encoding the outbox row failed (permanent): "
                + encodeFailure.getClass().getSimpleName() + detail, false, encodeFailure);
    }

    /** The effective producer {@code delivery.timeout.ms} — the relay reads it for the rowLease invariant (LLD-jdbc §3.5). */
    public long deliveryTimeoutMs() {
        return deliveryTimeoutMs;
    }

    /**
     * Reports the producer's effective {@code delivery.timeout.ms} to the relay so it validates the
     * {@code rowLease > deliveryTimeout} invariant against the <b>real</b> value automatically, with
     * no separate config to keep in sync (LLD-jdbc §3.5).
     */
    @Override
    public OptionalLong deliveryTimeoutMillis() {
        return OptionalLong.of(deliveryTimeoutMs);
    }

    @Override
    public void close() {
        producer.close();
    }
}
