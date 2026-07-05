package com.codingful.tandem.core.port;

import com.codingful.tandem.core.OutboxRecord;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
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

    /** A retriable failure: back to {@code PENDING}, eligible again at {@code nextAttemptAt}. */
    void markForRetry(long id, String error, Instant nextAttemptAt);

    /** A permanent failure: {@code FAILED} (blocks the aggregate until an operator intervenes). */
    void markFailed(long id, String error);

    /** Reclaim rows whose lease expired back to {@code PENDING}; returns the count reclaimed. */
    int reclaimExpiredLeases();

    /** Housekeeping: delete {@code DONE} rows older than {@code doneBefore}, up to {@code batchSize}; returns the count deleted. */
    int cleanup(Instant doneBefore, int batchSize);
}
