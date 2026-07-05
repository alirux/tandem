package com.codingful.tandem.benchmark.scenario;

import com.codingful.tandem.benchmark.AggregateSelector;
import com.codingful.tandem.benchmark.BenchmarkConfig;
import com.codingful.tandem.benchmark.BenchmarkEnvironment;
import com.codingful.tandem.benchmark.CommitTimestamps;
import com.codingful.tandem.benchmark.CorrelationConsumer;
import com.codingful.tandem.benchmark.LatencyRecorder;
import com.codingful.tandem.benchmark.LatencySnapshot;
import com.codingful.tandem.benchmark.LoadGenerator;
import com.codingful.tandem.benchmark.RampController;
import java.time.Duration;
import java.util.Map;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * S2 — latency at normal load (HLD-load-testing.md §4): holds ≈50% of a quick sustainable-rate
 * estimate for the measurement window and records COMMIT→ack p50/p95/p99/p99.9. The one scenario
 * that honours {@link BenchmarkConfig.LatencyMode#ACCURATE} (LLD-benchmark §5.1).
 */
public final class S2LatencyAtNormalLoad implements Scenario {

    @Override
    public String id() {
        return "S2";
    }

    @Override
    public ScenarioResult run(ScenarioContext ctx) throws Exception {
        BenchmarkEnvironment env = ctx.environment();
        BenchmarkConfig cfg = ctx.config();
        env.relayPool().start();

        // The one scenario that honours ACCURATE mode (LLD-benchmark §5.1): the same CommitTimestamps
        // instance is shared between the generator (writer, post-COMMIT) and the consumer (reader, on
        // receive). In PROXY mode this stays null and both sides fall back to the insert-time header.
        CommitTimestamps commitTimestamps =
                cfg.latencyMode() == BenchmarkConfig.LatencyMode.ACCURATE ? new CommitTimestamps() : null;
        LatencyRecorder latency = new LatencyRecorder(Duration.ofSeconds(10));

        try (KafkaConsumer<String, byte[]> kafkaConsumer = env.newConsumer("bench-s2");
             CorrelationConsumer consumer = new CorrelationConsumer(kafkaConsumer, latency, commitTimestamps);
             LoadGenerator generator = new LoadGenerator(env.dataSource(), cfg.bucketCount(), cfg.maxConnections(),
                     AggregateSelector.uniform(id(), cfg.aggregateCardinality()), cfg.payloadBytes(), commitTimestamps)) {
            consumer.start();

            // The sustain window a candidate rate must hold flat for, distinct from the (longer)
            // total search budget — mirrors S1's duration-vs-duration*2 split (RampController now
            // freezes the rate for the whole sustain window per hold, so the two must not be equal
            // or there is no time left for even one backoff-and-retry cycle).
            Duration quickRampSustainWindow = ScenarioSupport.maxDuration(Duration.ofSeconds(10), cfg.duration().dividedBy(4));
            Duration quickRampSearchBudget = quickRampSustainWindow.multipliedBy(2);
            RampController ramp = new RampController(env.lagProbe(),
                    ScenarioSupport.observationWindowFor(cfg), quickRampSustainWindow, 0.15, 0.3, cfg.batchSize());
            RampController.RampResult quick = ramp.findSustainableMax(generator, 100.0, quickRampSearchBudget);
            double normalLoadRate = Math.max(1.0, quick.sustainedRatePerSecond() * 0.5);
            generator.setRate(normalLoadRate);

            Thread.sleep(cfg.warmup().toMillis());
            latency.snapshot();   // discard the warm-up window (HLD-load-testing.md §3)
            Thread.sleep(cfg.duration().toMillis());
            LatencySnapshot snapshot = latency.snapshot();

            generator.stop();
            ScenarioSupport.waitForDrain(env.lagProbe(), id(), ScenarioSupport.maxDuration(Duration.ofMinutes(2), cfg.duration()));
            ScenarioSupport.CorrectnessReport report = ScenarioSupport.verify(generator, consumer);

            return new ScenarioResult(id(), report.passed(),
                    String.format("normal-load rate %.1f/s; p50=%s p95=%s p99=%s p999=%s; ordering violations=%d, missing=%d",
                            normalLoadRate, snapshot.p50(), snapshot.p95(), snapshot.p99(), snapshot.p999(),
                            report.orderingViolations(), report.missingKeys().size()),
                    Map.of(
                            "normalLoadRatePerSecond", normalLoadRate,
                            "p50Millis", snapshot.p50().toMillis(),
                            "p95Millis", snapshot.p95().toMillis(),
                            "p99Millis", snapshot.p99().toMillis(),
                            "p999Millis", snapshot.p999().toMillis(),
                            "orderingViolations", report.orderingViolations(),
                            "missingCount", report.missingKeys().size()));
        } finally {
            env.relayPool().stop();
        }
    }
}
