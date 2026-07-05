package com.codingful.tandem.jdbc;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Exponential backoff with <b>full jitter</b> (LLD-jdbc §3.6): {@code delay = random(0, min(cap,
 * base * 2^attempts))}. Full jitter spreads retries so a batch of rows that failed together does not
 * stampede the broker in lockstep.
 */
public final class FullJitterBackoff implements BackoffStrategy {

    private final long baseMillis;
    private final long capMillis;

    /**
     * @param base the starting delay, doubled on each successive attempt
     * @param cap  the maximum delay, regardless of attempt count
     * @throws IllegalArgumentException if {@code base} is not positive or {@code cap < base}
     */
    public FullJitterBackoff(Duration base, Duration cap) {
        if (base.isNegative() || base.isZero()) {
            throw new IllegalArgumentException("base must be positive");
        }
        if (cap.compareTo(base) < 0) {
            throw new IllegalArgumentException("cap must be >= base");
        }
        this.baseMillis = base.toMillis();
        this.capMillis = cap.toMillis();
    }

    @Override
    public Duration delayFor(int attempts) {
        long ceiling = boundedExponential(attempts);
        // random(0, ceiling] — at least 1 ms so a retry never lands in the same instant it failed.
        long delay = 1 + ThreadLocalRandom.current().nextLong(ceiling);
        return Duration.ofMillis(delay);
    }

    /** {@code min(cap, base * 2^attempts)}, overflow-safe: any large {@code attempts} clamps to {@code cap}. */
    private long boundedExponential(int attempts) {
        if (attempts < 0) {
            throw new IllegalArgumentException("attempts must be >= 0");
        }
        if (attempts >= 62) {
            return capMillis;
        }
        long scaled = baseMillis << attempts;             // base * 2^attempts
        if (scaled < 0 || scaled > capMillis) {           // overflowed or exceeded the cap
            return capMillis;
        }
        return scaled;
    }
}
