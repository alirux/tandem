package com.codingful.tandem.benchmark.scenario;

import com.codingful.tandem.benchmark.AggregateSelector;
import com.codingful.tandem.benchmark.BenchmarkConfig;
import com.codingful.tandem.benchmark.BenchmarkEnvironment;
import com.codingful.tandem.benchmark.CorrelationConsumer;
import com.codingful.tandem.benchmark.LatencyRecorder;
import com.codingful.tandem.benchmark.LoadGenerator;
import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * S6 — poison message (HLD-load-testing.md §4): one aggregate's dispatches are permanently failed
 * (via {@link com.codingful.tandem.benchmark.FaultInjector}), which the structural poison gate
 * (LLD-jdbc §3.3: a {@code FAILED} row still counts as "unfinished" for its aggregate) should block
 * from ever advancing, while every other aggregate keeps flowing. The poisoned aggregate's own events
 * are deliberately never dispatched, so it is excluded from the generic zero-loss check.
 */
public final class S6PoisonMessage implements Scenario {

    private static final double OFFERED_RATE_PER_SECOND = 50.0;

    @Override
    public String id() {
        return "S6";
    }

    @Override
    public ScenarioResult run(ScenarioContext ctx) throws Exception {
        BenchmarkEnvironment env = ctx.environment();
        BenchmarkConfig cfg = ctx.config();
        env.relayPool().start();

        AggregateSelector selector = AggregateSelector.uniform(id(), cfg.aggregateCardinality());
        String poisonAggregateId = selector.universe().get(0);
        env.faultInjector().poisonAggregate(poisonAggregateId);

        try (KafkaConsumer<String, byte[]> kafkaConsumer = env.newConsumer("bench-s6");
             CorrelationConsumer consumer = new CorrelationConsumer(
                     kafkaConsumer, new LatencyRecorder(Duration.ofSeconds(10)), null);
             LoadGenerator generator = new LoadGenerator(env.dataSource(), cfg.bucketCount(), cfg.maxConnections(),
                     selector, cfg.payloadBytes(), null)) {
            consumer.start();
            generator.start(OFFERED_RATE_PER_SECOND);
            Thread.sleep(cfg.duration().toMillis());
            generator.stop();

            ScenarioSupport.waitForOthersToDrain(env.lagProbe(), id(), poisonAggregateId,
                    ScenarioSupport.maxDuration(Duration.ofMinutes(2), cfg.duration()));
            boolean poisonBlocked = env.lagProbe().hasFailedRow(poisonAggregateId);

            Set<String> missing = new HashSet<>(generator.insertedKeys());
            missing.removeIf(key -> key.startsWith(poisonAggregateId + '#'));   // excluded on purpose — never dispatched
            missing.removeAll(consumer.receivedKeys());
            boolean othersOk = missing.isEmpty() && consumer.orderingViolations() == 0;

            env.faultInjector().clear();
            return new ScenarioResult(id(), othersOk && poisonBlocked,
                    String.format("poison=%s blocked=%s; other aggregates: ordering violations=%d, missing=%d",
                            poisonAggregateId, poisonBlocked, consumer.orderingViolations(), missing.size()),
                    Map.of(
                            "poisonAggregateId", poisonAggregateId,
                            "poisonBlocked", poisonBlocked,
                            "orderingViolations", consumer.orderingViolations(),
                            "missingCount", missing.size()));
        } finally {
            env.faultInjector().clear();
            env.relayPool().stop();
        }
    }
}
