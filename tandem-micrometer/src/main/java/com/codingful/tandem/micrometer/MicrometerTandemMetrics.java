package com.codingful.tandem.micrometer;

import com.codingful.tandem.core.port.TandemMetrics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@link TandemMetrics} backed by a Micrometer {@link MeterRegistry} (LLD-micrometer §2/§3): binds
 * every port method to the meter named in HLD §7. A gauge is registered exactly once, at construction,
 * over a mutable state holder the port methods then write to — Micrometer samples that holder at
 * scrape time rather than accepting a pushed value, so the holder (not the value) is what a
 * {@link Gauge} keeps a (weak) reference to; this instance's own fields are the strong reference that
 * keeps them alive for the process's life.
 */
public final class MicrometerTandemMetrics implements TandemMetrics {

    /**
     * Overrides Micrometer's own {@code Timer} default (30s) for {@code publish.latency}'s histogram
     * ceiling. 30s is too low for this specific meter: a row delayed behind a failure or a relay
     * restart can carry a wait well past it, and {@code histogram_quantile()} silently caps a reported
     * percentile at the highest finite bucket once the true value falls in the {@code +Inf} bucket —
     * so an under-sized ceiling doesn't drop the sample, it quietly under-reports it. 5 minutes covers
     * realistic backlog/failover delays (observed up to ~158s on a live run) without moving so far out
     * that bucket resolution (exponential between min and max) gets too coarse to be useful.
     */
    public static final Duration DEFAULT_MAX_EXPECTED_PUBLISH_LATENCY = Duration.ofMinutes(5);

    private final MeterRegistry registry;
    private final AtomicLong lag = new AtomicLong();
    private final AtomicLong lagAgeMillis = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private final AtomicLong blocked = new AtomicLong();
    private final AtomicInteger activeWorkers = new AtomicInteger();
    private final AtomicLong workerCycleAgeMillis = new AtomicLong();
    private final AtomicInteger uncoveredBuckets = new AtomicInteger();
    private final Counter published;
    private final Timer publishLatency;
    private final Counter retries;
    private final Counter leaseExpired;
    private final Counter seqRegressions;
    private final Map<String, AtomicInteger> configInvalidByCheck = new ConcurrentHashMap<>();

    /** @param registry where every meter below is registered; kept to register a new one per distinct {@code check} name */
    public MicrometerTandemMetrics(MeterRegistry registry) {
        this(registry, DEFAULT_MAX_EXPECTED_PUBLISH_LATENCY);
    }

    /**
     * @param registry                    where every meter below is registered; kept to register a new
     *                                     one per distinct {@code check} name
     * @param maxExpectedPublishLatency    the histogram ceiling for {@code publish.latency} — see
     *                                     {@link #DEFAULT_MAX_EXPECTED_PUBLISH_LATENCY}. A latency above
     *                                     it is still recorded (in the {@code +Inf} bucket), just without
     *                                     the resolution to place it precisely between finite buckets.
     */
    public MicrometerTandemMetrics(MeterRegistry registry, Duration maxExpectedPublishLatency) {
        this.registry = Objects.requireNonNull(registry, "registry");
        Objects.requireNonNull(maxExpectedPublishLatency, "maxExpectedPublishLatency");
        Gauge.builder("tandem.outbox.lag.count", lag, AtomicLong::get).register(registry);
        Gauge.builder("tandem.outbox.lag.age_seconds", lagAgeMillis, holder -> holder.get() / 1000d).register(registry);
        Gauge.builder("tandem.outbox.failed.count", failed, AtomicLong::get).register(registry);
        Gauge.builder("tandem.outbox.blocked.count", blocked, AtomicLong::get).register(registry);
        Gauge.builder("tandem.outbox.workers.active", activeWorkers, AtomicInteger::get).register(registry);
        Gauge.builder("tandem.outbox.workers.cycle_age_seconds", workerCycleAgeMillis, holder -> holder.get() / 1000d)
                .register(registry);
        Gauge.builder("tandem.outbox.bucket.uncovered", uncoveredBuckets, AtomicInteger::get).register(registry);
        this.published = Counter.builder("tandem.outbox.published").register(registry);
        // publishPercentileHistogram, not a client-computed percentile: percentiles cannot be averaged
        // across instances, but Prometheus (or any TSDB) can derive a correct multi-instance percentile
        // from the published histogram buckets (LLD-micrometer §2).
        this.publishLatency = Timer.builder("tandem.outbox.publish.latency")
                .publishPercentileHistogram(true)
                .maximumExpectedValue(maxExpectedPublishLatency)
                .register(registry);
        this.retries = Counter.builder("tandem.outbox.retry.count").register(registry);
        this.leaseExpired = Counter.builder("tandem.outbox.lease_expired.count").register(registry);
        this.seqRegressions = Counter.builder("tandem.outbox.seq_regression.count").register(registry);
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public void recordLag(long pending) {
        lag.set(pending);
    }

    @Override
    public void recordLagAgeSeconds(double age) {
        lagAgeMillis.set(Math.round(age * 1000));
    }

    @Override
    public void recordFailed(long count) {
        failed.set(count);
    }

    @Override
    public void recordBlocked(long count) {
        blocked.set(count);
    }

    @Override
    public void recordActiveWorkers(int n) {
        activeWorkers.set(n);
    }

    @Override
    public void recordWorkerCycleAgeSeconds(double age) {
        workerCycleAgeMillis.set(Math.round(age * 1000));
    }

    @Override
    public void recordUncoveredBuckets(int n) {
        uncoveredBuckets.set(n);
    }

    @Override
    public void incrementPublished(long n) {
        published.increment(n);
    }

    @Override
    public void recordPublishLatency(Duration latency) {
        publishLatency.record(latency);
    }

    @Override
    public void incrementRetry() {
        retries.increment();
    }

    @Override
    public void incrementSeqRegression() {
        seqRegressions.increment();
    }

    @Override
    public void incrementLeaseExpired(long n) {
        leaseExpired.increment(n);
    }

    /**
     * Registers a new {@code tandem.relay.config.invalid} gauge the first time a given {@code check}
     * name is reported, then sets it — never the literal {@code 1}, which Micrometer's own guidance
     * calls incorrect for an immutable number (LLD-micrometer §3).
     */
    @Override
    public void recordConfigInvalid(String check) {
        Objects.requireNonNull(check, "check");
        configInvalidByCheck.computeIfAbsent(check, name -> {
            AtomicInteger holder = new AtomicInteger();
            Gauge.builder("tandem.relay.config.invalid", holder, AtomicInteger::get)
                    .tag("check", name)
                    .register(registry);
            return holder;
        }).set(1);
    }
}
