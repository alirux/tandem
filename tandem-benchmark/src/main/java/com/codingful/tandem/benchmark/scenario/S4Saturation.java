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
 * S4 — saturation / backpressure (HLD-load-testing.md §4): offers a rate far beyond anything the
 * environment can sustain (the in-flight semaphore and real DB/broker capacity self-limit actual
 * throughput, so a huge nominal target stands in for "beyond S1's measured max" without needing that
 * number first), confirms the backlog grows, then drops the rate back down and confirms it drains
 * with no loss and no reordering.
 */
public final class S4Saturation implements Scenario {

    private static final double OVERLOAD_RATE_PER_SECOND = 1_000_000.0;
    private static final double RECOVERY_RATE_PER_SECOND = 1.0;

    @Override
    public String id() {
        return "S4";
    }

    @Override
    public ScenarioResult run(ScenarioContext ctx) throws Exception {
        BenchmarkEnvironment env = ctx.environment();
        BenchmarkConfig cfg = ctx.config();
        env.relayPool().start();

        try (KafkaConsumer<String, byte[]> kafkaConsumer = env.newConsumer("bench-s4");
             CorrelationConsumer consumer = new CorrelationConsumer(
                     kafkaConsumer, new LatencyRecorder(Duration.ofSeconds(10)), null);
             LoadGenerator generator = new LoadGenerator(env.dataSource(), cfg.bucketCount(), cfg.maxConnections(),
                     AggregateSelector.uniform(id(), cfg.aggregateCardinality()), cfg.payloadBytes(), null)) {
            consumer.start();

            long pendingBefore = env.lagProbe().overall().pending();
            generator.start(OVERLOAD_RATE_PER_SECOND);
            Thread.sleep(cfg.duration().toMillis());
            long pendingUnderOverload = env.lagProbe().overall().pending();

            generator.setRate(RECOVERY_RATE_PER_SECOND);
            ScenarioSupport.waitForDrain(env.lagProbe(), id(), ScenarioSupport.maxDuration(Duration.ofMinutes(5), cfg.duration().multipliedBy(3)));
            generator.stop();

            ScenarioSupport.CorrectnessReport report = ScenarioSupport.verify(generator, consumer);
            boolean lagRoseUnderOverload = pendingUnderOverload > pendingBefore;

            return new ScenarioResult(id(), report.passed(),
                    String.format("pending rose from %d to %d under overload (rose=%s), then drained; ordering violations=%d, missing=%d",
                            pendingBefore, pendingUnderOverload, lagRoseUnderOverload, report.orderingViolations(), report.missingKeys().size()),
                    Map.of(
                            "pendingBefore", pendingBefore,
                            "pendingUnderOverload", pendingUnderOverload,
                            "lagRoseUnderOverload", lagRoseUnderOverload,
                            "orderingViolations", report.orderingViolations(),
                            "missingCount", report.missingKeys().size()));
        } finally {
            env.relayPool().stop();
        }
    }
}
