package com.codingful.tandem.benchmark;

import com.codingful.tandem.jdbc.Coordination;
import com.codingful.tandem.jdbc.RelayConfig;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Drives every meter the relay reports through a real Micrometer → Prometheus → Grafana pipeline, so
 * the signals can be judged on a dashboard instead of in an assertion (LLD-benchmark §6.3). Same
 * intent as {@link LagGaugeDemo} one level up: that one prints two numbers to a terminal, this one
 * shows all nine on the surface an operator would actually be watching.
 *
 * <p>It <b>measures nothing and gates nothing</b>. The scripted run walks the relay through the states
 * each meter exists to reveal — a backlog with no relay behind it, a drain, steady load, a failing
 * aggregate, a second instance joining, a crash with rows still in flight — and then holds the stack
 * open so the dashboard can be read while it is still live.
 *
 * <p>Report output, like the rest of this module: {@code System.out}, not a logger.
 *
 * <p>Usage: {@code ./gradlew :tandem-benchmark:metricsDashboardDemo}. Add
 * {@code --args="--hold=120"} to close by itself after the given number of seconds instead of waiting
 * on the keyboard. Needs Docker.
 */
public final class MetricsDashboardDemo {

    // Deliberately small. This demo shows the *shape* of each signal, so the backlog only has to be
    // clearly non-zero, not large — a few hundred rows read exactly like a few thousand on a graph,
    // and cost less to build and drain.
    private static final double STEADY_RATE = 10;
    private static final double BURST_RATE = 20;

    /** Short enough that a whole phase is many points on the dashboard rather than two or three. */
    private static final Duration METRICS_INTERVAL = Duration.ofSeconds(1);
    private static final Duration BUCKET_LEASE = Duration.ofSeconds(10);

    // reclaimInterval is deliberately left at RelayConfig's own 5s default rather than shortened the way
    // the S8 scenario shortens it. It is what paces bucket takeover after a crash, so a sub-second value
    // closes the coverage gap faster than any reading interval can sample it: bucket.uncovered would then
    // read a flat zero through a failover that genuinely happened. Watching the real default is the point.

    private static long runStart;

    private MetricsDashboardDemo() {
    }

    public static void main(String[] args) throws Exception {
        Duration hold = holdFrom(args);

        // Sized for legibility, not for throughput — this is not a benchmark (that is LoadTestRunner).
        // maxAttempts is lowered from the benchmark default so the failing-aggregate phase reaches a
        // terminal FAILED within the phase instead of spending minutes climbing the backoff ladder.
        // aggregateCardinality is low on purpose: the blocked chain grows at rate/cardinality, so with a
        // small offered rate a large universe would leave only a row or two piled up behind the poisoned
        // aggregate — too few to read on the blocked-vs-claimable panel.
        BenchmarkConfig cfg = BenchmarkConfig.builder()
                .bucketCount(256)   // must match the lease table seeded by the baseline DDL
                .workers(2)
                .batchSize(20)
                .deliveryTimeoutMs(8_000)
                .rowLease(Duration.ofSeconds(20))
                .maxAttempts(4)
                .maxConnections(8)
                .payloadBytes(1024)
                .aggregateCardinality(16)
                .build();

        List<MetricsExporter> exporters = List.of(new MetricsExporter("relay-1"), new MetricsExporter("relay-2"));
        try (BenchmarkEnvironment env = new BenchmarkEnvironment(cfg).start();
             ObservabilityStack observability = new ObservabilityStack(exporters).start()) {

            RelayInstance relayOne = env.newRelayInstance(leaseConfig(env, "demo-relay-1"), exporters.get(0).metrics());
            RelayInstance relayTwo = env.newRelayInstance(leaseConfig(env, "demo-relay-2"), exporters.get(1).metrics());

            AggregateSelector selector = AggregateSelector.uniform("metricsdemo", cfg.aggregateCardinality());
            String failingAggregate = selector.universe().get(0);
            String stallingAggregate = selector.universe().get(1);

            try (LoadGenerator generator = new LoadGenerator(env.dataSource(), cfg.bucketCount(),
                    cfg.maxConnections(), selector, cfg.payloadBytes(), null)) {

                runStart = System.nanoTime();
                intro(observability, exporters);

                phase(observability, "No relay is running yet", """
                        Load runs with no relay at all. The backlog climbs, and every gauge reads a
                        flat zero — not because there is nothing wrong, but because the gauges are
                        emitted BY the relay, and there is no relay. This is the blind spot HLD §7
                        describes: absence looks exactly like health.""");
                generator.start(BURST_RATE);
                await(Duration.ofSeconds(20), env);

                phase(observability, "The relay starts and drains the backlog", """
                        One instance under LEASE coordination. Watch backlog fall, publish rate rise,
                        workers.active jump to 2, and bucket.uncovered drop to 0 as it takes ownership.""");
                generator.stop();
                relayOne.pool().start();
                await(Duration.ofSeconds(25), env);

                phase(observability, "Steady load", """
                        A rate the relay absorbs comfortably. Backlog hovers near zero; the oldest
                        waiting event stays around one poll cycle.""");
                generator.start(STEADY_RATE);
                await(Duration.ofSeconds(20), env);

                phase(observability, "One aggregate starts failing", """
                        Every dispatch for aggregate %s fails retriably. retry.count climbs, and once
                        the attempts are spent the row turns terminal and failed.count rises.
                        Then watch 'waiting: claimable vs blocked' — and keep watching it for the rest
                        of the run. That one dead row blocks its chain permanently, so every later row
                        of that aggregate piles up behind it as PENDING. lag.count never returns to
                        zero again and lag.age_seconds climbs at one second per second for ever, with
                        the relay delivering every other aggregate perfectly. Without blocked.count to
                        say which of the two it is, an alert on either would latch on here and stay
                        on.""".formatted(failingAggregate));
                env.faultInjector().flakeAggregate(failingAggregate);
                await(Duration.ofSeconds(30), env);

                phase(observability, "A second relay instance joins", """
                        Both instances report the same GLOBAL backlog reading, on two separate series.
                        A dashboard must take max() or avg() over them, never sum() — the per-instance
                        signal here is workers.active, not lag.""");
                env.faultInjector().clear();
                relayTwo.pool().start();
                await(Duration.ofSeconds(20), env);

                phase(observability, "The second instance crashes with rows in flight", """
                        Dispatch for aggregate %s hangs, pinning its rows IN_FLIGHT under a row lease,
                        and the instance is then killed outright — no graceful release. Its scrape
                        endpoint goes down with it, so it stops being a target rather than reporting
                        zeroes: 'relay instances reporting' drops to 1 and workers.active loses a
                        series, which is what noticing a dead relay actually looks like.
                        lease_expired.count then climbs as the abandoned rows are reclaimed.
                        Do NOT expect bucket.uncovered to move here. Its buckets are uncovered only
                        between their lease expiring and the survivor's next heartbeat claiming them —
                        a gap bounded by reclaimInterval, routinely shorter than one reading.""".formatted(stallingAggregate));
                env.faultInjector().stallAggregate(stallingAggregate);
                await(Duration.ofSeconds(10), env);
                relayTwo.pool().kill();
                exporters.get(1).close();
                await(BUCKET_LEASE.plusSeconds(30), env);

                phase(observability, "Recovery", """
                        Faults cleared, load stopped. Everything the relay can still deliver drains
                        away — and what is left on the backlog panel is exactly the blocked chain,
                        which is the reading worth leaving on screen: 'claimable' at zero, 'blocked'
                        holding whatever piled up, failed.count at 1. The relay has nothing left to
                        do; a human does.""");
                env.faultInjector().clear();
                generator.stop();
                await(Duration.ofSeconds(25), env);

                outro(observability);
                waitForOperator(hold);
            } finally {
                relayOne.pool().stop();
                relayTwo.pool().stop();
            }
        } finally {
            exporters.forEach(MetricsExporter::close);
        }
    }

    private static RelayConfig leaseConfig(BenchmarkEnvironment env, String instanceId) {
        return env.relayConfigBuilder()
                .coordination(Coordination.LEASE)
                .instanceId(instanceId)
                .bucketLease(BUCKET_LEASE)
                .metricsInterval(METRICS_INTERVAL)
                .build();
    }

    private static void intro(ObservabilityStack observability, List<MetricsExporter> exporters) {
        System.out.println();
        System.out.println("=".repeat(88));
        System.out.println("  Tandem relay metrics — live dashboard");
        System.out.println("=".repeat(88));
        System.out.println();
        System.out.println("  Grafana     " + observability.dashboardUrl());
        System.out.println("  Prometheus  " + observability.prometheusUrl());
        System.out.println();
        System.out.println("  Open the Grafana link now — the run below takes about three minutes and the");
        System.out.println("  dashboard is worth watching live. Every phase is announced here as it starts.");
        System.out.println();
        System.out.println("  Eight of the nine meters are driven below. The ninth, tandem.relay.config.invalid,");
        System.out.println("  is not: it fires once and then aborts the process by design (LLD-micrometer §4),");
        System.out.println("  so there is nothing left running for a scraper to read it from.");
        System.out.println();
        System.out.println("  What the relay exposes at /metrics right now:");
        exporters.get(0).scrape().lines()
                .filter(line -> line.startsWith("tandem_"))
                .forEach(line -> System.out.println("    " + line));
        System.out.println();
    }

    private static void phase(ObservabilityStack observability, String title, String explanation) {
        observability.annotate(title);
        System.out.println();
        System.out.printf(Locale.ROOT, "[%6.1fs]  %s%n", elapsedSeconds(), title);
        explanation.lines().forEach(line -> System.out.println("          " + line.strip()));
    }

    /** Sleeps out a phase, then prints where the outbox actually stands — the harness's own SQL, not the relay's. */
    private static void await(Duration window, BenchmarkEnvironment env) throws InterruptedException {
        Thread.sleep(window.toMillis());
        LagProbe.Lag lag = env.lagProbe().overall();
        System.out.printf(Locale.ROOT, "          -> waiting:%d, oldest:%.1fs%n",
                lag.pending(), lag.age().toMillis() / 1000d);
    }

    private static void outro(ObservabilityStack observability) {
        System.out.println();
        System.out.println("-".repeat(88));
        System.out.println("  Run complete. One relay instance is still running and still reporting, so the");
        System.out.println("  dashboard stays live rather than freezing at the last value.");
        System.out.println();
        System.out.println("  Grafana  " + observability.dashboardUrl());
    }

    private static void waitForOperator(Duration hold) throws Exception {
        if (hold != null) {
            System.out.printf(Locale.ROOT, "  Holding for %d seconds, then shutting the stack down.%n%n",
                    hold.toSeconds());
            Thread.sleep(hold.toMillis());
            return;
        }
        System.out.println("  Press Enter to shut the containers down.");
        System.out.println();
        // A closed stdin (the task is run without a console) reads -1 immediately rather than blocking
        // forever, which is the behaviour --hold exists for; falling through is the right answer either way.
        System.in.read();
    }

    private static Duration holdFrom(String[] args) {
        for (String arg : args) {
            if (arg.startsWith("--hold=")) {
                return Duration.ofSeconds(Long.parseLong(arg.substring("--hold=".length())));
            }
        }
        return null;
    }

    private static double elapsedSeconds() {
        return (System.nanoTime() - runStart) / 1e9;
    }
}
