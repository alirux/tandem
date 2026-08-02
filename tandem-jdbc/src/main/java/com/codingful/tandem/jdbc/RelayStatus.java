package com.codingful.tandem.jdbc;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * One reading of a relay instance's <b>in-process</b> state, taken by {@link WorkerPool#status()}
 * (LLD-jdbc §3.8) — what the application needs to answer "is this relay alive and working?" without
 * being told what "healthy" means.
 *
 * <p><b>This reading never touches the database.</b> That is the contract, not an implementation
 * detail: it is what makes the accessor safe to call at readiness/liveness-probe frequency, on every
 * instance, forever. Everything database-derived is deliberately absent — backlog size and age, failed
 * and blocked counts are already reachable through the public {@code OutboxStore} port
 * ({@code lag()}, {@code failedCount()}, {@code blockedCount()}), where the caller controls the query
 * cadence and does its own caching. Bucket ownership and coverage are absent for the same reason: under
 * {@link Coordination#LEASE} both are lease-table queries.
 *
 * <p><b>Tandem draws no health conclusion from these figures</b> — no threshold, no DOWN/UP verdict, no
 * framework health contributor. Deciding what an acceptable worker deficit or cycle age is belongs to
 * the application, exactly like routing logs or exporting meters does.
 *
 * @param instanceId        this relay's identity, the same value used as {@code tandem_bucket_lease.owner}
 *                          and as the worker thread-name prefix, so a reading correlates directly with
 *                          the lease table and the logs
 * @param state             where the pool is in its lifecycle; {@link State#STOPPING} is distinct from
 *                          {@link State#STOPPED} on purpose (see {@link State})
 * @param coordination      the configured coordination mode, needed to interpret the rest (a
 *                          {@code LEASE} instance legitimately owns no buckets at times)
 * @param workersConfigured worker threads this instance is configured to run
 * @param workersAlive      worker threads currently alive; the <em>gap</em> against
 *                          {@code workersConfigured} is the signal, since a died worker is restarted
 *                          automatically (LLD-jdbc §3.1) and a transient deficit is normal — a
 *                          persistent one is a worker dying in a loop
 * @param oldestWorkerCycle when the least recently progressing live worker last completed a claim
 *                          cycle, or empty when no worker is alive. A live thread is not a working
 *                          thread — one blocked in a database call that never returns stays alive
 *                          forever — so this, not {@code workersAlive}, is what distinguishes a relay
 *                          that is running from one that is merely started. Starting a worker counts
 *                          as its first cycle, so a relay that hangs immediately shows an ageing
 *                          timestamp rather than an empty one.
 */
public record RelayStatus(
        String instanceId,
        State state,
        Coordination coordination,
        int workersConfigured,
        int workersAlive,
        Optional<Instant> oldestWorkerCycle) {

    /**
     * @throws NullPointerException     if any reference component is null
     * @throws IllegalArgumentException if either worker count is negative
     */
    public RelayStatus {
        Objects.requireNonNull(instanceId, "instanceId");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(coordination, "coordination");
        Objects.requireNonNull(oldestWorkerCycle, "oldestWorkerCycle");
        if (workersConfigured < 0) {
            throw new IllegalArgumentException("workersConfigured must not be negative, got " + workersConfigured);
        }
        if (workersAlive < 0) {
            throw new IllegalArgumentException("workersAlive must not be negative, got " + workersAlive);
        }
    }

    /** Lifecycle state of the worker pool. */
    public enum State {

        /** Never started, or fully stopped. */
        STOPPED,

        /** Workers and maintenance jobs are running. */
        RUNNING,

        /**
         * A shutdown has begun and is still joining workers. Kept distinct from {@link #STOPPED}
         * because it is precisely the window in which an instance should leave the load balancer
         * <em>without</em> being reported as a failure — collapsing the two would make draining
         * indistinguishable from crashing.
         */
        STOPPING
    }

    /**
     * How long the least recently progressing live worker has been without completing a cycle, the
     * form an alert threshold is actually written against.
     *
     * @param now the instant to measure against
     * @return the age in seconds, or {@code 0} when no worker is alive
     */
    public double oldestWorkerCycleAgeSecondsAt(Instant now) {
        Objects.requireNonNull(now, "now");
        return oldestWorkerCycle.map(at -> Math.max(0, (now.toEpochMilli() - at.toEpochMilli()) / 1000d))
                .orElse(0d);
    }
}
