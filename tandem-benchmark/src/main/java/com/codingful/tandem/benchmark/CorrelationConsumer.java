package com.codingful.tandem.benchmark;

import com.codingful.tandem.core.CloudEventsHeaders;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;

/**
 * A Kafka consumer co-located with the harness that does double duty (HLD-load-testing.md §2.2): the
 * relay records nothing, so this is the one place COMMIT→ack latency and per-aggregate correctness
 * (ordering, duplicates) are observed. Owns its {@link KafkaConsumer}; the caller {@link #start()}s
 * and {@link #stop()}s/closes it.
 */
public final class CorrelationConsumer implements AutoCloseable {

    private final KafkaConsumer<String, byte[]> consumer;
    private final LatencyRecorder latencyRecorder;
    private final CommitTimestamps commitTimestamps;   // nullable — PROXY-only mode

    private final Map<String, Long> lastSeqByAggregate = new ConcurrentHashMap<>();
    private final Set<String> receivedKeys = ConcurrentHashMap.newKeySet();
    private final AtomicLong orderingViolations = new AtomicLong();
    private final AtomicLong duplicates = new AtomicLong();
    private final List<String> violationSamples = new CopyOnWriteArrayList<>();

    private volatile boolean running;
    private Thread pollThread;

    public CorrelationConsumer(KafkaConsumer<String, byte[]> consumer, LatencyRecorder latencyRecorder,
                                CommitTimestamps commitTimestamps) {
        this.consumer = consumer;
        this.latencyRecorder = latencyRecorder;
        this.commitTimestamps = commitTimestamps;
    }

    public void start() {
        running = true;
        pollThread = new Thread(this::pollLoop, "bench-correlation-consumer");
        pollThread.setDaemon(true);
        pollThread.start();
    }

    public void stop() {
        running = false;
        consumer.wakeup();
        if (pollThread != null) {
            try {
                pollThread.join(TimeUnit.SECONDS.toMillis(10));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void close() {
        stop();
        consumer.close();
    }

    public long orderingViolations() {
        return orderingViolations.get();
    }

    public long duplicateCount() {
        return duplicates.get();
    }

    /** Sample of up to a few offending {@code (aggregateId, seq)} pairs, for diagnostics. */
    public List<String> violationSamples() {
        return List.copyOf(violationSamples);
    }

    /** Every {@code (aggregateId, seq)} received at least once, as {@code aggregateId + '#' + seq}. */
    public Set<String> receivedKeys() {
        return receivedKeys;
    }

    private void pollLoop() {
        while (running) {
            ConsumerRecords<String, byte[]> records;
            try {
                records = consumer.poll(Duration.ofMillis(200));
            } catch (WakeupException e) {
                continue;   // checked again against `running` at the top of the loop
            }
            long receiveNanos = System.nanoTime();
            for (ConsumerRecord<String, byte[]> record : records) {
                onRecord(record, receiveNanos);
            }
        }
    }

    private void onRecord(ConsumerRecord<String, byte[]> record, long receiveNanos) {
        String aggregateId = record.key();
        long seq = headerLong(record, CloudEventsHeaders.CE_SEQ);
        if (seq < 0) {
            return;   // malformed/unrelated record — nothing to correlate
        }
        String key = aggregateId + '#' + seq;
        if (!receivedKeys.add(key)) {
            duplicates.incrementAndGet();
        }
        checkOrdering(aggregateId, seq);
        recordLatency(aggregateId, seq, record, receiveNanos);
    }

    private void checkOrdering(String aggregateId, long seq) {
        lastSeqByAggregate.merge(aggregateId, seq, (previousSeq, newSeq) -> {
            // A redelivered duplicate has newSeq == previousSeq — expected under at-least-once
            // delivery (S5) and explicitly not fatal (duplicateCount handles it); only a strictly
            // *decreasing* seq is a genuine ordering violation.
            if (newSeq < previousSeq) {
                orderingViolations.incrementAndGet();
                violationSamples.add(aggregateId + ": seq " + newSeq + " arrived after " + previousSeq);
                return previousSeq;   // keep the high-watermark; don't regress on a violation
            }
            return newSeq;
        });
    }

    private void recordLatency(String aggregateId, long seq, ConsumerRecord<String, byte[]> record, long receiveNanos) {
        long t0 = -1;
        if (commitTimestamps != null) {
            t0 = commitTimestamps.takeCommitNanos(aggregateId, seq);
        }
        if (t0 < 0) {
            t0 = headerLong(record, BenchmarkHeaders.T0_NANOS);
        }
        if (t0 < 0) {
            return;   // header missing/unparseable — skip rather than record a bogus latency
        }
        latencyRecorder.record(Duration.ofNanos(Math.max(0, receiveNanos - t0)));
    }

    private static long headerLong(ConsumerRecord<String, byte[]> record, String headerName) {
        Header header = record.headers().lastHeader(headerName);
        if (header == null) {
            return -1;
        }
        try {
            return Long.parseLong(new String(header.value(), StandardCharsets.UTF_8).trim());
        } catch (NumberFormatException e) {
            return -1;
        }
    }
}
