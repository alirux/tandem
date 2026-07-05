package com.codingful.tandem.kafka;

import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.port.OutboxDispatcher;
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

    /**
     * Builds a producer from {@code producerConfig}, hardening it to the mandated safe values (fails
     * fast on unsafe overrides, §1).
     *
     * @param producerConfig raw Kafka producer properties; {@code acks}/idempotence/etc. are hardened, not user-overridable
     * @param router         maps each record to its destination topic
     * @param cfg            CloudEvents binding settings ({@code source}, default content type/schema)
     * @throws com.codingful.tandem.core.exception.TandemConfigurationException if {@code producerConfig} overrides a mandated safe value
     */
    public KafkaRelay(Map<String, ?> producerConfig, TopicRouter router, KafkaRelayConfig cfg) {
        Map<String, Object> hardened = KafkaProducerConfig.harden(producerConfig);
        this.producer = new KafkaProducer<>(hardened);
        this.encoder = new CloudEventEncoder(router, cfg);
        this.classifier = new DefaultErrorClassifier();
        this.deliveryTimeoutMs = KafkaProducerConfig.deliveryTimeoutMs(hardened);
    }

    /** For tests: inject a producer (e.g. Kafka's {@code MockProducer}) and classifier directly. */
    KafkaRelay(Producer<String, byte[]> producer, TopicRouter router, KafkaRelayConfig cfg,
               ErrorClassifier classifier, long deliveryTimeoutMs) {
        this.producer = Objects.requireNonNull(producer, "producer");
        this.encoder = new CloudEventEncoder(router, cfg);
        this.classifier = Objects.requireNonNull(classifier, "classifier");
        this.deliveryTimeoutMs = deliveryTimeoutMs;
    }

    @Override
    public CompletableFuture<Void> dispatch(OutboxRecord record) {
        CompletableFuture<Void> ack = new CompletableFuture<>();
        ProducerRecord<String, byte[]> producerRecord;
        try {
            producerRecord = encoder.encode(record);
        } catch (RuntimeException encodeFailure) {
            // An encoding failure (e.g. a bad payload/header) will never succeed → permanent.
            LOG.error("Encoding outbox row failed rowId:{}, aggregateType:{}, aggregateId:{}", record.id(),
                    record.aggregateType(), record.aggregateId(), encodeFailure);
            ack.completeExceptionally(classifier.classify(encodeFailure));
            return ack;
        }
        try {
            producer.send(producerRecord, (metadata, exception) -> {
                if (exception == null) {
                    ack.complete(null);
                } else {
                    LOG.error("Publishing outbox row failed rowId:{}, topic:{}", record.id(),
                            producerRecord.topic(), exception);
                    ack.completeExceptionally(classifier.classify(exception));
                }
            });
        } catch (RuntimeException sendThrew) {
            // send() can throw synchronously (serialization, buffer exhaustion) — classify it too.
            LOG.error("Sending outbox row failed synchronously rowId:{}, topic:{}", record.id(),
                    producerRecord.topic(), sendThrew);
            ack.completeExceptionally(classifier.classify(sendThrew));
        }
        return ack;
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
