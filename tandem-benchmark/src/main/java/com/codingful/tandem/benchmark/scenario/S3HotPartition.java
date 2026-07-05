package com.codingful.tandem.benchmark.scenario;

import com.codingful.tandem.benchmark.AggregateSelector;
import com.codingful.tandem.benchmark.BenchmarkConfig;
import com.codingful.tandem.benchmark.BenchmarkEnvironment;
import com.codingful.tandem.benchmark.CorrelationConsumer;
import com.codingful.tandem.benchmark.LagProbe;
import com.codingful.tandem.benchmark.LatencyRecorder;
import com.codingful.tandem.benchmark.LoadGenerator;
import com.codingful.tandem.core.BucketHash;
import java.time.Duration;
import java.util.Map;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * S3 — hot partition / skew (HLD-load-testing.md §4): a skewed aggregate distribution concentrates
 * most traffic on one aggregate. Because per-aggregate ordering caps that aggregate at one in-flight
 * dispatch regardless of arrival rate, its own bucket backs up while buckets holding only the
 * remaining, lightly-loaded aggregates should not. Isolation is reported, not pass/fail-gated —
 * {@code passed} stays correctness-only (ScenarioResult).
 *
 * <p>The active drive phase is capped at {@link #MAX_DRIVE} regardless of {@code cfg.duration()}: the
 * hot aggregate's own backlog is structurally serialized (one in-flight dispatch at a time), so its
 * drain time scales with {@code offered rate × hot fraction × drive time}, not with the milder,
 * throughput-bound scaling the other scenarios see. Left uncapped, a moderate multi-minute
 * {@code duration} (meant to give S1's ramp more room to explore, say) would make S3 alone take tens
 * of minutes to drain — found while trying exactly that. The drain-wait {@code timeout} must be sized
 * to {@link #MAX_DRIVE}'s worst case, not to {@code cfg.duration()} either — the two were briefly out
 * of sync (timeout still tracked {@code cfg.duration()} after the drive was capped, so a
 * multi-minute-duration run could offer 10s of a hot aggregate's backlog and then time out waiting for
 * it, since 10s at {@code OFFERED_RATE_PER_SECOND × HOT_FRACTION} takes longer than that to drain).
 */
public final class S3HotPartition implements Scenario {

    private static final double OFFERED_RATE_PER_SECOND = 200.0;
    private static final double HOT_FRACTION = 0.8;
    private static final Duration MAX_DRIVE = Duration.ofSeconds(10);
    private static final Duration DRAIN_TIMEOUT = Duration.ofMinutes(6);

    @Override
    public String id() {
        return "S3";
    }

    @Override
    public ScenarioResult run(ScenarioContext ctx) throws Exception {
        BenchmarkEnvironment env = ctx.environment();
        BenchmarkConfig cfg = ctx.config();
        env.relayPool().start();

        AggregateSelector selector = AggregateSelector.skewed(id(), cfg.aggregateCardinality(), HOT_FRACTION);
        String hotAggregateId = selector.universe().get(0);

        try (KafkaConsumer<String, byte[]> kafkaConsumer = env.newConsumer("bench-s3");
             CorrelationConsumer consumer = new CorrelationConsumer(
                     kafkaConsumer, new LatencyRecorder(Duration.ofSeconds(10)), null);
             LoadGenerator generator = new LoadGenerator(env.dataSource(), cfg.bucketCount(), cfg.maxConnections(),
                     selector, cfg.payloadBytes(), null)) {
            consumer.start();
            generator.start(OFFERED_RATE_PER_SECOND);
            Thread.sleep(ScenarioSupport.minDuration(cfg.duration(), MAX_DRIVE).toMillis());

            Map<Integer, LagProbe.Lag> perBucket = env.lagProbe().perBucket();
            int hotBucket = BucketHash.bucketFor(hotAggregateId, cfg.bucketCount());
            long hotPending = perBucket.getOrDefault(hotBucket, new LagProbe.Lag(0, Duration.ZERO)).pending();
            long coldBucketsWithBacklog = perBucket.entrySet().stream()
                    .filter(e -> e.getKey() != hotBucket && e.getValue().pending() > 0)
                    .count();

            generator.stop();
            ScenarioSupport.waitForDrain(env.lagProbe(), id(), DRAIN_TIMEOUT);
            ScenarioSupport.CorrectnessReport report = ScenarioSupport.verify(generator, consumer);

            boolean isolated = coldBucketsWithBacklog == 0;
            return new ScenarioResult(id(), report.passed(),
                    String.format("hot-bucket pending=%d, cold buckets with backlog=%d (isolated=%s); ordering violations=%d, missing=%d",
                            hotPending, coldBucketsWithBacklog, isolated, report.orderingViolations(), report.missingKeys().size()),
                    Map.of(
                            "hotBucketPending", hotPending,
                            "coldBucketsWithBacklog", coldBucketsWithBacklog,
                            "isolated", isolated,
                            "orderingViolations", report.orderingViolations(),
                            "missingCount", report.missingKeys().size()));
        } finally {
            env.relayPool().stop();
        }
    }
}
