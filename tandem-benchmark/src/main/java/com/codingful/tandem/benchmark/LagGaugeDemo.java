package com.codingful.tandem.benchmark;

import com.codingful.tandem.jdbc.RelayConfig;
import java.time.Duration;
import java.util.Locale;

/**
 * Prints what the relay's lag gauges actually look like over a run, so their shape can be judged
 * rather than merely asserted: a backlog is built with the relay stopped, the relay is then started
 * and drains it, and finally a moderate steady load runs. Each sample shows the relay's own reading
 * ({@code TandemMetrics}) next to {@link LagProbe}'s independent SQL over the same table — two
 * computations of the same figure, which is the point of reading them side by side.
 *
 * <p>Report output, like the rest of this module: {@code System.out}, not a logger.
 *
 * <p>Usage: {@code ./gradlew :tandem-benchmark:lagGaugeDemo}. Needs Docker.
 */
public final class LagGaugeDemo {

    private static final Duration SAMPLE_INTERVAL = Duration.ofMillis(500);
    private static final Duration BACKLOG_BUILD = Duration.ofSeconds(8);
    private static final Duration DRAIN_WATCH = Duration.ofSeconds(20);
    private static final Duration STEADY_LOAD = Duration.ofSeconds(15);
    private static final Duration SETTLE = Duration.ofSeconds(3);
    private static final double BURST_RATE = 400;
    private static final double STEADY_RATE = 60;

    /** Start of the run, so the elapsed column is continuous instead of restarting at each phase. */
    private static long runStart;

    private LagGaugeDemo() {
    }

    public static void main(String[] args) throws Exception {
        BenchmarkConfig cfg = BenchmarkConfig.defaults().toDemo();
        try (BenchmarkEnvironment env = new BenchmarkEnvironment(cfg).start()) {
            // A 500ms reading interval instead of the 10s default: the point here is the shape of the
            // curve, which a coarse interval would flatten into a handful of points.
            RelayConfig relayCfg = env.relayConfigBuilder().metricsInterval(SAMPLE_INTERVAL).build();
            RelayInstance relay = env.newRelayInstance(relayCfg);
            BenchmarkMetrics metrics = env.metrics();
            LagProbe probe = env.lagProbe();

            try (LoadGenerator generator = new LoadGenerator(env.dataSource(), cfg.bucketCount(),
                    cfg.maxConnections(), AggregateSelector.uniform("laggauge", cfg.aggregateCardinality()),
                    cfg.payloadBytes(), null)) {

                runStart = System.nanoTime();
                header();
                // Phase 1 — the relay is down, so nothing reports: only the probe sees the backlog grow.
                // This is what an operator's dashboard shows when a relay dies: gauges go stale, not to zero.
                generator.start(BURST_RATE);
                sampleFor(BACKLOG_BUILD, "relay down", metrics, probe);

                // Phase 2 — the relay starts and drains what piled up.
                generator.stop();
                relay.pool().start();
                sampleFor(DRAIN_WATCH, "draining", metrics, probe);

                // Phase 3 — moderate load the relay absorbs comfortably.
                generator.start(STEADY_RATE);
                sampleFor(STEADY_LOAD, "steady load", metrics, probe);

                // Let the inserts still in flight land before the generator closes: closing interrupts
                // them mid-transaction, which litters the report with driver stack traces.
                generator.stop();
                sampleFor(SETTLE, "settling", metrics, probe);
            } finally {
                relay.pool().stop();
            }
        }
    }

    private static void header() {
        System.out.println();
        System.out.println("What the columns mean");
        System.out.println("  time      seconds since the run started");
        System.out.println("  phase     what is being done to the relay at that moment");
        System.out.println("  waiting   events still to be published (rows in status PENDING)");
        System.out.println("  oldest    how long the oldest of those events has been waiting, in seconds");
        System.out.println("  workers   relay worker threads alive");
        System.out.println();
        System.out.println("  The left block is what the RELAY ITSELF last reported through TandemMetrics —");
        System.out.println("  what a dashboard would show, up to one reading interval old, and '-' until the");
        System.out.println("  relay has reported at all. The right block is the same two figures recomputed by");
        System.out.println("  the harness with its own SQL, at the instant the line is printed.");
        System.out.println();
        System.out.printf(Locale.ROOT, "%7s  %-12s | %-27s | %s%n",
                "", "", "  reported by the relay", "  measured now");
        System.out.printf(Locale.ROOT, "%7s  %-12s | %9s %8s %7s | %9s %8s%n",
                "time", "phase", "waiting", "oldest", "workers", "waiting", "oldest");
        System.out.println("-".repeat(78));
    }

    private static void sampleFor(Duration window, String phase, BenchmarkMetrics metrics, LagProbe probe)
            throws InterruptedException {
        long deadline = System.nanoTime() + window.toNanos();
        while (System.nanoTime() < deadline) {
            LagProbe.Lag observed = probe.overall();
            boolean reported = metrics.lag() >= 0;
            System.out.printf(Locale.ROOT, "%6.1fs  %-12s | %9s %8s %7s | %9d %8.1f%n",
                    (System.nanoTime() - runStart) / 1e9, phase,
                    reported ? Long.toString(metrics.lag()) : "-",
                    reported ? String.format(Locale.ROOT, "%.1f", metrics.lagAgeSeconds()) : "-",
                    reported ? Integer.toString(metrics.activeWorkers()) : "-",
                    observed.pending(), observed.age().toMillis() / 1000d);
            Thread.sleep(SAMPLE_INTERVAL.toMillis());
        }
    }
}
