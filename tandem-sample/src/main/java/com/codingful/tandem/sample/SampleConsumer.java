package com.codingful.tandem.sample;

import com.codingful.tandem.core.CloudEventsHeaders;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;

/**
 * [DEMO-ONLY] Reads CloudEvents from Kafka and prints each one to the console.
 *
 * <p>This is a plain Kafka consumer, not a Tandem class — it exists only so the sample can
 * observe and verify what the relay published. A real application's Kafka consumers are
 * ordinary application code too; Tandem only owns the publish side.
 *
 * <h3>CloudEvents binary encoding on Kafka</h3>
 *
 * <p>Tandem publishes events using the CloudEvents Kafka Protocol Binding in
 * <em>binary content mode</em>:
 *
 * <ul>
 *   <li><b>Key</b> = {@code aggregateId} — used as the Kafka partition key so all events for one
 *       aggregate land on the same partition.</li>
 *   <li><b>Value</b> = raw payload bytes (whatever was passed to
 *       {@link com.codingful.tandem.core.OutboxMessage#payload()}).</li>
 *   <li><b>Headers</b> carry the CloudEvents attributes prefixed with {@code ce_}: {@code ce_id},
 *       {@code ce_type}, {@code ce_source}, {@code ce_time}, {@code ce_seq} (Tandem extension),
 *       {@code ce_partitionkey} (Tandem extension), …</li>
 * </ul>
 *
 * <h3>Ordering guarantee</h3>
 *
 * <p>The relay only publishes seq N+1 after the broker has acknowledged seq N. Therefore events
 * for the same {@code aggregateId} always arrive in strict {@code ce_seq} order.
 */
public final class SampleConsumer {

    private final KafkaConsumer<String, byte[]> consumer;

    public SampleConsumer(KafkaConsumer<String, byte[]> consumer) {
        this.consumer = consumer;
    }

    /**
     * Polls until {@code expectedCount} events have been received or {@code timeout} expires.
     *
     * @return map from {@code aggregateId} to the list of {@code ce_seq} values in arrival order
     */
    public Map<String, List<Long>> consumeAll(int expectedCount, Duration timeout) {
        Map<String, List<Long>> result = new LinkedHashMap<>();
        int total = 0;
        long deadline = System.nanoTime() + timeout.toNanos();

        while (total < expectedCount && System.nanoTime() < deadline) {
            for (ConsumerRecord<String, byte[]> rec : consumer.poll(Duration.ofMillis(300))) {
                long seq = longHeader(rec, CloudEventsHeaders.CE_SEQ);
                String type = stringHeader(rec, "ce_type");

                System.out.printf("  [kafka]  partition=%-2d  key=%-10s  ce_seq=%-3d  type=%s%n",
                        rec.partition(), rec.key(), seq, type);

                result.computeIfAbsent(rec.key(), k -> new ArrayList<>()).add(seq);
                total++;
            }
        }

        if (total < expectedCount) {
            System.out.printf("  ⚠ timeout after %ds — received %d/%d events%n",
                    timeout.toSeconds(), total, expectedCount);
        }

        return result;
    }

    private static long longHeader(ConsumerRecord<?, ?> record, String name) {
        Header h = record.headers().lastHeader(name);
        return h == null ? -1L : Long.parseLong(new String(h.value(), StandardCharsets.UTF_8));
    }

    private static String stringHeader(ConsumerRecord<?, ?> record, String name) {
        Header h = record.headers().lastHeader(name);
        return h == null ? "(none)" : new String(h.value(), StandardCharsets.UTF_8);
    }
}
