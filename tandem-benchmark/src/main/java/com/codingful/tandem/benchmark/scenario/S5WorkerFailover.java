package com.codingful.tandem.benchmark.scenario;

import com.codingful.tandem.benchmark.AggregateSelector;
import com.codingful.tandem.benchmark.BenchmarkConfig;
import com.codingful.tandem.benchmark.BenchmarkEnvironment;
import com.codingful.tandem.benchmark.CorrelationConsumer;
import com.codingful.tandem.benchmark.LatencyRecorder;
import com.codingful.tandem.benchmark.LoadGenerator;
import java.time.Duration;
import java.util.Map;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * S5 — worker failover (HLD-load-testing.md §4): simulates an instance crash by stopping the whole
 * relay pool without a graceful drain (some in-flight dispatches may still land an ack after the
 * worker loop has already exited and stopped flushing DONE — LLD-benchmark §8), waits past the row
 * lease, forces the reclaim a crashed instance's own maintenance job would otherwise have run, then
 * restarts and confirms the backlog fully drains with duplicates bounded.
 *
 * <p>This kills the <b>whole instance</b>, not a single worker thread among several (WorkerPool
 * exposes no per-worker kill hook), so the observed duplicate bound is conservatively
 * {@code batchSize * workers} rather than {@code batchSize} for one worker.
 */
public final class S5WorkerFailover implements Scenario {

    private static final double OFFERED_RATE_PER_SECOND = 50.0;

    @Override
    public String id() {
        return "S5";
    }

    @Override
    public ScenarioResult run(ScenarioContext ctx) throws Exception {
        BenchmarkEnvironment env = ctx.environment();
        BenchmarkConfig cfg = ctx.config();
        env.relayPool().start();

        try (KafkaConsumer<String, byte[]> kafkaConsumer = env.newConsumer("bench-s5");
             CorrelationConsumer consumer = new CorrelationConsumer(
                     kafkaConsumer, new LatencyRecorder(Duration.ofSeconds(10)), null);
             LoadGenerator generator = new LoadGenerator(env.dataSource(), cfg.bucketCount(), cfg.maxConnections(),
                     AggregateSelector.uniform(id(), cfg.aggregateCardinality()), cfg.payloadBytes(), null)) {
            consumer.start();
            generator.start(OFFERED_RATE_PER_SECOND);
            Thread.sleep(cfg.duration().dividedBy(2).toMillis());

            env.relayPool().stop();
            Thread.sleep(cfg.rowLease().plusSeconds(1).toMillis());
            int reclaimed = env.store().reclaimExpiredLeases();
            env.relayPool().start();

            Thread.sleep(cfg.duration().dividedBy(2).toMillis());
            generator.stop();
            ScenarioSupport.waitForDrain(env.lagProbe(), id(), ScenarioSupport.maxDuration(Duration.ofMinutes(5), cfg.duration().multipliedBy(3)));

            ScenarioSupport.CorrectnessReport report = ScenarioSupport.verify(generator, consumer);
            long duplicateBound = (long) cfg.batchSize() * cfg.workers();
            boolean duplicatesBounded = consumer.duplicateCount() <= duplicateBound;

            return new ScenarioResult(id(), report.passed() && duplicatesBounded,
                    String.format("reclaimed=%d, duplicates=%d (bound=%d); ordering violations=%d, missing=%d",
                            reclaimed, consumer.duplicateCount(), duplicateBound, report.orderingViolations(), report.missingKeys().size()),
                    Map.of(
                            "reclaimed", reclaimed,
                            "duplicates", consumer.duplicateCount(),
                            "duplicateBound", duplicateBound,
                            "orderingViolations", report.orderingViolations(),
                            "missingCount", report.missingKeys().size()));
        } finally {
            env.relayPool().stop();
        }
    }
}
