package com.codingful.tandem.jdbc;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Per-worker sleep timing for the relay poll loop (LLD-jdbc §3.1). Two distinct waits, deliberately
 * kept apart because they answer different questions:
 *
 * <ul>
 *   <li><b>Idle</b> — the claim returned no rows. Waits {@code pollInterval} ±20%: the mean is
 *       unchanged (so discovery latency is exactly what the operator configured), but workers that
 *       started together stop polling in lockstep.</li>
 *   <li><b>Failed cycle</b> — the iteration threw. Waits an exponentially growing delay from
 *       {@code pollInterval} up to a cap, reset by the first cycle that completes. A dead database
 *       otherwise has every worker re-querying and logging a stack trace ten times a second.</li>
 * </ul>
 *
 * The ±20% jitter is narrow on purpose: it is below the doubling factor, so successive failure
 * delays still grow strictly monotonically, and an idle wait can never collapse towards zero and
 * turn the loop into a spin. Not thread-safe — each worker thread owns its own instance, which is
 * what makes the failure counter per-worker rather than a shared, contended one.
 */
final class PollBackoff {

    /** Fraction by which a computed delay is randomly stretched or shrunk. */
    private static final double JITTER = 0.2;

    /** Failure count past which the exponential can only be the cap; the counter stops there. */
    private static final int SATURATED = 62;

    private final long idleMillis;
    private final long errorCapMillis;

    private int consecutiveFailures;

    /**
     * @param pollInterval the idle backoff, and the first delay after a failed cycle
     * @param errorCap     ceiling for the failure backoff; raised to {@code pollInterval} when smaller,
     *                     so the failure delay is never shorter than the idle one
     */
    PollBackoff(Duration pollInterval, Duration errorCap) {
        this.idleMillis = Math.max(1, pollInterval.toMillis());
        this.errorCapMillis = Math.max(this.idleMillis, errorCap.toMillis());
    }

    /** Marks a cycle that completed without throwing, whether or not it claimed anything. */
    void onCycleCompleted() {
        consecutiveFailures = 0;
    }

    /** How long to wait after a claim that returned nothing. */
    long idleSleepMillis() {
        return jitter(idleMillis);
    }

    /** How long to wait after a cycle that threw; grows on each successive call until the cap. */
    long failedCycleSleepMillis() {
        long delay = jitter(boundedExponential(consecutiveFailures));
        if (consecutiveFailures < SATURATED) {
            consecutiveFailures++;   // stops there: counting further would only overflow the shift
        }
        // Jitter is applied before the clamp so the cap is a hard ceiling: an operator reading
        // "capped at reclaimInterval" must not find waits 20% past it.
        return Math.min(delay, errorCapMillis);
    }

    /** {@code min(cap, idle * 2^failures)}, overflow-safe: any large count clamps to the cap. */
    private long boundedExponential(int failures) {
        if (failures >= SATURATED) {
            // The shift below silently truncates from here on — 100 << 62 is 0, not a huge number,
            // which would turn a saturated backoff back into a spin.
            return errorCapMillis;
        }
        long scaled = idleMillis << failures;
        if (scaled < 0 || scaled > errorCapMillis) {
            return errorCapMillis;
        }
        return scaled;
    }

    /** {@code random[millis * (1 - JITTER), millis * (1 + JITTER)]}, never below 1 ms. */
    private static long jitter(long millis) {
        double spread = millis * JITTER;   // > 0: millis is at least 1
        double jittered = millis + ThreadLocalRandom.current().nextDouble(-spread, spread);
        return Math.max(1, Math.round(jittered));
    }
}
