package com.codingful.tandem.core.port;

import com.codingful.tandem.core.LagSnapshot;
import com.codingful.tandem.core.OutboxRecord;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

/**
 * Relay-side persistence port (LLD-core §2.2): poll/claim/update/cleanup over {@code tandem_outbox}.
 * Implemented by {@code tandem-jdbc} (and {@code InMemoryOutbox} for unit tests).
 */
public interface OutboxStore {

    /**
     * Poll the worker's owned virtual buckets for the <b>head of each aggregate's pending chain</b>
     * (the earliest not-yet-DONE row — which also subsumes the poison gate, E2) and mark the batch
     * {@code IN_FLIGHT} for this worker (Q8: virtual-bucket sharding, §4.3/§6).
     *
     * @param buckets   the virtual buckets this worker owns
     * @param workerId  the claiming worker's id (written to {@code locked_by})
     * @param lease     how long the claim is held before it can be reclaimed
     * @param batchSize the maximum number of heads to claim
     */
    List<OutboxRecord> claimBatch(Set<Integer> buckets, String workerId, Duration lease, int batchSize);

    /** Mark a delivered row {@code DONE}. */
    void markDone(long id);

    /**
     * Mark several acked rows {@code DONE} in one call (§3.4.1). The ids may span different aggregates
     * — mark-DONE is order-independent — so the relay batches them to cut round-trips. The default
     * loops {@link #markDone}; the JDBC adapter overrides it with a single batched UPDATE.
     */
    default void markDoneBatch(Collection<Long> ids) {
        for (Long id : ids) {
            markDone(id);
        }
    }

    /**
     * A retriable failure: back to {@code PENDING}, eligible again after {@code retryDelay}.
     *
     * <p>The delay is passed <b>relative</b>, exactly like {@code claimBatch}'s lease: the caller owns
     * the backoff policy, the adapter owns the anchor. The JDBC adapter anchors it on the <b>DB clock</b>
     * ({@code next_attempt_at = now() + retryDelay}), which is also the clock the claim compares against
     * — so a relay whose host clock differs from the DB's cannot make a row due early or late
     * (LLD-jdbc §3.2/§3.6).
     */
    void markForRetry(long id, String error, Duration retryDelay);

    /** A permanent failure: {@code FAILED} (blocks the aggregate until an operator intervenes). */
    void markFailed(long id, String error);

    /** Reclaim rows whose lease expired back to {@code PENDING}; returns the count reclaimed. */
    int reclaimExpiredLeases();

    /** Housekeeping: delete {@code DONE} rows older than {@code doneBefore}, up to {@code batchSize}; returns the count deleted. */
    int cleanup(Instant doneBefore, int batchSize);

    /**
     * Read the current backlog for the lag gauges (LLD-jdbc §4). The relay calls this only while a
     * metrics adapter is wired, so a store that cannot answer cheaply may keep the default.
     *
     * @return the reading, or empty if this store does not report lag — the relay then emits nothing
     */
    default Optional<LagSnapshot> lag() {
        return Optional.empty();
    }

    /**
     * Read the current count of {@code FAILED} rows for the {@code failed.count} gauge (HLD §7). Unlike
     * {@link #lag()} this is a live count, not an accumulated total: a row counted here can later leave
     * {@code FAILED} (moved to {@code DISCARDED} by an operator, LLD-core §1.2), and the next reading
     * must reflect that — a running total of failure events would never go back down.
     *
     * @return the count, or empty if this store does not report it — the relay then emits nothing
     */
    default OptionalLong failedCount() {
        return OptionalLong.empty();
    }

    /**
     * Read how many {@code PENDING} rows sit behind a {@code FAILED} row of the same aggregate, for the
     * {@code blocked.count} gauge (HLD §7). These rows are counted by {@link #lag()} like any other, and
     * they are genuinely waiting — but no relay will ever claim them, because a {@code FAILED} head
     * blocks its chain until an operator resolves it. Without this reading the two are indistinguishable:
     * a single terminal failure pins {@code lag.count} above zero and makes {@code lag.age_seconds} climb
     * forever, so an alert on either latches on permanently while the relay is in fact healthy.
     *
     * <p>Deliberately <b>not</b> subtracted from {@link #lag()}: those rows are undelivered events, and a
     * backlog gauge that hid them would report a healthy outbox while an aggregate is completely stalled.
     *
     * @return the count, or empty if this store does not report it — the relay then emits nothing
     */
    default OptionalLong blockedCount() {
        return OptionalLong.empty();
    }
}
