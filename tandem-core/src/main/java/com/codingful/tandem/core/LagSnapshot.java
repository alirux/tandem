package com.codingful.tandem.core;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * One reading of the outbox backlog, taken in a single query so both figures describe the same
 * instant (HLD §7: {@code tandem.outbox.lag.count} and {@code tandem.outbox.lag.age_seconds}).
 *
 * <p><b>Backlog means {@code PENDING} only.</b> An {@code IN_FLIGHT} row is work already in progress,
 * not work waiting; if its relay dies, the lease reclaim puts it back to {@code PENDING} and it
 * re-enters this reading on its own (LLD-jdbc §3.5).
 *
 * @param pending      rows currently waiting to be published
 * @param oldestSince  when the oldest waiting row was created, empty when nothing is waiting
 */
public record LagSnapshot(long pending, Optional<Instant> oldestSince) {

    /**
     * @throws IllegalArgumentException if {@code pending} is negative
     * @throws NullPointerException     if {@code oldestSince} is null
     */
    public LagSnapshot {
        if (pending < 0) {
            throw new IllegalArgumentException("pending must not be negative, got " + pending);
        }
        Objects.requireNonNull(oldestSince, "oldestSince");
    }

    /**
     * How long the oldest waiting row has been waiting, as of {@code now} — the alerting signal, since
     * a count alone cannot distinguish a healthy burst from a stalled relay (HLD §7).
     *
     * @param now the instant to measure against
     * @return the age in seconds, or {@code 0} when nothing is waiting
     */
    public double ageSecondsAt(Instant now) {
        Objects.requireNonNull(now, "now");
        return oldestSince.map(since -> Math.max(0, (now.toEpochMilli() - since.toEpochMilli()) / 1000d))
                .orElse(0d);
    }
}
