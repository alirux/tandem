package com.codingful.tandem.test;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.CloudEventsHeaders;
import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.port.TopicRouter;
import com.codingful.tandem.jdbc.RelayConfig;
import com.codingful.tandem.jdbc.WorkerPool;
import com.codingful.tandem.kafka.KafkaRelayConfig;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration")
class EndToEndIT {

    private static final String TOPIC = "order-topic";
    private static final int AGGREGATES = 10;
    private static final int PER_AGGREGATE = 20;
    private static final int TOTAL = AGGREGATES * PER_AGGREGATE;
    private static final int BUCKETS = 256;

    @Test
    void GIVEN_events_inserted_across_aggregates_WHEN_the_relay_runs_THEN_each_aggregate_lands_on_the_topic_in_order_with_no_loss() {
        try (TandemTestContainer tandem = new TandemTestContainer()) {
            tandem.start();
            // Insert in one transaction (a single batched insert), seq-ascending per aggregate.
            tandem.newRepository(BUCKETS).insertAll(buildMessages());
            tandem.createTopic(TOPIC, 4);   // multiple partitions so aggregate keys distribute

            try (KafkaConsumer<String, byte[]> consumer = tandem.newConsumer("e2e", TOPIC)) {
                consumer.poll(Duration.ofMillis(200));   // join the group before the relay publishes

                WorkerPool relay = tandem.newRelay(
                        RelayConfig.builder().bucketCount(BUCKETS).workersPerInstance(4)
                                .pollInterval(Duration.ofMillis(50)).build(),
                        TopicRouter.kebabWithSuffix("-topic"),
                        KafkaRelayConfig.of("/tandem/orders"));
                relay.start();
                try {
                    Map<String, List<Long>> deliveredSeqsByAggregate = consumeAll(consumer);

                    // Zero lost events: every (aggregate, seq) arrived exactly once.
                    long delivered = deliveredSeqsByAggregate.values().stream().mapToLong(List::size).sum();
                    assertThat(delivered).isEqualTo(TOTAL);
                    assertThat(deliveredSeqsByAggregate).hasSize(AGGREGATES);

                    // Zero ordering violations: each aggregate's events arrived in 1..N seq order.
                    List<Long> expected = expectedSeqs();
                    deliveredSeqsByAggregate.forEach((aggregate, seqs) ->
                            assertThat(seqs).as("delivery order for %s", aggregate).isEqualTo(expected));
                } finally {
                    relay.stop();
                }
            }
        }
    }

    private static List<OutboxMessage> buildMessages() {
        List<OutboxMessage> messages = new ArrayList<>(TOTAL);
        for (int a = 0; a < AGGREGATES; a++) {
            String aggregateId = "order-" + a;
            for (long seq = 1; seq <= PER_AGGREGATE; seq++) {
                messages.add(OutboxMessage.builder()
                        .aggregateId(aggregateId).aggregateType("Order").type("com.acme.order.changed").seq(seq)
                        .payload(("{\"agg\":\"" + aggregateId + "\",\"seq\":" + seq + "}").getBytes(StandardCharsets.UTF_8))
                        .contentType("application/json").build());
            }
        }
        return messages;
    }

    private static List<Long> expectedSeqs() {
        List<Long> expected = new ArrayList<>(PER_AGGREGATE);
        for (long seq = 1; seq <= PER_AGGREGATE; seq++) {
            expected.add(seq);
        }
        return expected;
    }

    /** Poll until all events arrive (or time out), recording each aggregate's seqs in arrival order. */
    private static Map<String, List<Long>> consumeAll(KafkaConsumer<String, byte[]> consumer) {
        Map<String, List<Long>> seqsByAggregate = new LinkedHashMap<>();
        long total = 0;
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (total < TOTAL && System.nanoTime() < deadline) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, byte[]> record : records) {
                seqsByAggregate.computeIfAbsent(record.key(), k -> new ArrayList<>()).add(seqHeader(record));
                total++;
            }
        }
        return seqsByAggregate;
    }

    private static long seqHeader(ConsumerRecord<String, byte[]> record) {
        Header header = record.headers().lastHeader(CloudEventsHeaders.CE_SEQ);
        return Long.parseLong(new String(header.value(), StandardCharsets.UTF_8));
    }
}
