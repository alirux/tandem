package com.codingful.tandem.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.codingful.tandem.core.BucketHash;
import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.OutboxStatus;
import com.codingful.tandem.core.exception.DuplicateSeqException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class InMemoryOutboxTest {

    private static final int BUCKETS = 256;
    private static final int MAX_ATTEMPTS = 3;
    private static final String WORKER = "worker-1";
    private static final Duration LEASE = Duration.ofSeconds(60);

    private ControllableClock clock;
    private InMemoryOutbox outbox;

    @BeforeEach
    void setUp() {
        clock = ControllableClock.atEpochDay();
        outbox = new InMemoryOutbox(BUCKETS, MAX_ATTEMPTS, clock);
    }

    private static OutboxMessage message(String aggregateId, long seq) {
        return OutboxMessage.builder()
                .aggregateId(aggregateId)
                .aggregateType("Order")
                .seq(seq)
                .payload(("payload-" + aggregateId + '-' + seq).getBytes())
                .build();
    }

    private List<OutboxRecord> claimAll(int batchSize) {
        return outbox.claimBatch(outbox.allBuckets(), WORKER, LEASE, batchSize);
    }

    @Test
    void GIVEN_a_message_WHEN_inserted_THEN_it_is_pending_with_an_id_created_at_and_core_bucket() {
        OutboxMessage message = message("order-1", 1);
        outbox.insert(message);

        OutboxRecord stored = outbox.all().get(0);
        assertThat(stored.id()).isPositive();
        assertThat(stored.status()).isEqualTo(OutboxStatus.PENDING);
        assertThat(stored.createdAt()).isEqualTo(clock.instant());
        // Bucket must match the SAME core hash the JDBC adapter uses.
        assertThat(outbox.bucketOf(stored.id())).isEqualTo(BucketHash.bucketFor("order-1", BUCKETS));
    }

    @Test
    void GIVEN_a_duplicate_aggregate_id_and_seq_WHEN_inserted_THEN_it_is_rejected() {
        outbox.insert(message("order-1", 1));

        assertThatThrownBy(() -> outbox.insert(message("order-1", 1)))
                .isInstanceOf(DuplicateSeqException.class);
        // The same seq for a DIFFERENT aggregate is fine.
        outbox.insert(message("order-2", 1));
        assertThat(outbox.size()).isEqualTo(2);
    }

    @Test
    void GIVEN_several_messages_WHEN_inserted_together_THEN_all_are_stored() {
        outbox.insertAll(List.of(message("order-1", 1), message("order-1", 2), message("order-2", 1)));

        assertThat(outbox.byStatus(OutboxStatus.PENDING)).hasSize(3);
    }

    @Test
    void GIVEN_a_content_type_WHEN_inserted_THEN_it_is_folded_into_the_headers() {
        outbox.insert(OutboxMessage.builder()
                .aggregateId("order-1").aggregateType("Order").seq(1)
                .payload(new byte[] {1}).contentType("application/json").build());

        assertThat(outbox.all().get(0).headers()).containsEntry("content-type", "application/json");
    }

    @Test
    void GIVEN_two_aggregates_with_chains_WHEN_claimed_THEN_only_the_head_of_each_chain_is_returned_and_locked() {
        outbox.insert(message("order-1", 1));   // id 1 — head of order-1
        outbox.insert(message("order-1", 2));   // id 2 — blocked behind id 1
        outbox.insert(message("order-2", 1));   // id 3 — head of order-2

        List<OutboxRecord> claimed = claimAll(10);

        assertThat(claimed).extracting(OutboxRecord::id).containsExactly(1L, 3L);
        assertThat(claimed).allSatisfy(r -> {
            assertThat(r.status()).isEqualTo(OutboxStatus.IN_FLIGHT);
            assertThat(r.lockedBy()).isEqualTo(WORKER);
            assertThat(r.lockedUntil()).isEqualTo(clock.instant().plus(LEASE));
        });
    }

    @Test
    void GIVEN_a_claimed_head_WHEN_marked_done_THEN_the_next_seq_becomes_the_claimable_head() {
        outbox.insert(message("order-1", 1));   // id 1
        outbox.insert(message("order-1", 2));   // id 2

        long head = claimAll(10).get(0).id();
        assertThat(head).isEqualTo(1L);
        // Before the head is done, seq 2 must NOT be claimable (would reorder the aggregate).
        assertThat(claimAll(10)).isEmpty();

        outbox.markDone(head);
        assertThat(claimAll(10)).extracting(OutboxRecord::id).containsExactly(2L);
    }

    @Test
    void GIVEN_a_failed_earlier_row_WHEN_claimed_THEN_the_aggregate_is_blocked_by_the_poison_gate() {
        outbox.insert(message("order-1", 1));   // id 1
        outbox.insert(message("order-1", 2));   // id 2

        long head = claimAll(10).get(0).id();
        outbox.markFailed(head, "boom");

        // The FAILED head blocks every later row of the aggregate (head-of-chain over status 0/1/3).
        assertThat(claimAll(10)).isEmpty();
    }

    @Test
    void GIVEN_a_batch_size_smaller_than_available_heads_WHEN_claimed_THEN_it_is_capped() {
        outbox.insert(message("order-1", 1));
        outbox.insert(message("order-2", 1));
        outbox.insert(message("order-3", 1));

        assertThat(claimAll(2)).hasSize(2);
    }

    @Test
    void GIVEN_a_worker_owning_only_some_buckets_WHEN_claimed_THEN_rows_in_other_buckets_are_left() {
        outbox.insert(message("order-1", 1));
        int ownedBucket = outbox.bucketOf(1);

        // Claim with a bucket set that excludes the row's bucket → nothing claimed.
        Set<Integer> others = outbox.allBuckets();
        others.remove(ownedBucket);
        assertThat(outbox.claimBatch(others, WORKER, LEASE, 10)).isEmpty();
        // Claim with only the owned bucket → claimed.
        assertThat(outbox.claimBatch(Set.of(ownedBucket), WORKER, LEASE, 10)).hasSize(1);
    }

    @Test
    void GIVEN_a_row_with_a_future_next_attempt_WHEN_claimed_THEN_it_is_skipped_until_due() {
        outbox.insert(message("order-1", 1));
        long id = claimAll(10).get(0).id();
        outbox.markForRetry(id, "boom", Duration.ofSeconds(30));

        // Not yet due.
        assertThat(claimAll(10)).isEmpty();
        // Advance past next_attempt_at → claimable again.
        clock.advance(Duration.ofSeconds(31));
        assertThat(claimAll(10)).extracting(OutboxRecord::id).containsExactly(id);
    }

    @Test
    void GIVEN_a_retriable_failure_WHEN_the_row_is_scheduled_for_retry_THEN_it_returns_to_pending_with_an_incremented_attempt() {
        outbox.insert(message("order-1", 1));
        long id = claimAll(10).get(0).id();
        Duration delay = Duration.ofSeconds(5);
        Instant next = clock.instant().plus(delay);

        outbox.markForRetry(id, "transient", delay);

        OutboxRecord r = outbox.byId(id);
        assertThat(r.status()).isEqualTo(OutboxStatus.PENDING);
        assertThat(r.attempts()).isEqualTo(1);
        assertThat(r.lastError()).isEqualTo("transient");
        assertThat(r.nextAttemptAt()).isEqualTo(next);
        assertThat(r.lockedBy()).isNull();
        assertThat(r.lockedUntil()).isNull();
    }

    @Test
    void GIVEN_a_permanent_failure_WHEN_the_row_is_marked_as_a_permanent_failure_THEN_it_is_failed_with_an_incremented_attempt() {
        outbox.insert(message("order-1", 1));
        long id = claimAll(10).get(0).id();

        outbox.markFailed(id, "poison");

        OutboxRecord r = outbox.byId(id);
        assertThat(r.status()).isEqualTo(OutboxStatus.FAILED);
        assertThat(r.attempts()).isEqualTo(1);
        assertThat(r.lastError()).isEqualTo("poison");
    }

    @Test
    void GIVEN_an_expired_lease_WHEN_reclaimed_THEN_the_row_returns_to_pending_and_counts_as_an_attempt() {
        outbox.insert(message("order-1", 1));
        long id = claimAll(10).get(0).id();

        clock.advance(LEASE.plusSeconds(1));   // lease expires
        int reclaimed = outbox.reclaimExpiredLeases();

        assertThat(reclaimed).isEqualTo(1);
        OutboxRecord r = outbox.byId(id);
        assertThat(r.status()).isEqualTo(OutboxStatus.PENDING);
        assertThat(r.attempts()).isEqualTo(1);
        assertThat(r.lastError()).isEqualTo(InMemoryOutbox.LEASE_EXPIRED_ERROR);
        assertThat(r.lockedBy()).isNull();
    }

    @Test
    void GIVEN_a_live_lease_WHEN_reclaim_runs_THEN_the_row_is_untouched() {
        outbox.insert(message("order-1", 1));
        claimAll(10);

        clock.advance(LEASE.minusSeconds(1));   // still within lease
        assertThat(outbox.reclaimExpiredLeases()).isZero();
        assertThat(outbox.byId(1).status()).isEqualTo(OutboxStatus.IN_FLIGHT);
    }

    @Test
    void GIVEN_a_crash_poison_row_at_the_attempt_ceiling_WHEN_reclaimed_THEN_it_is_quarantined_to_failed() {
        outbox.insert(message("order-1", 1));

        // Re-claim then let the lease expire, repeatedly: each reclaim bumps attempts.
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            assertThat(claimAll(10)).hasSize(1);   // claimable because previous reclaim reset it to PENDING
            clock.advance(LEASE.plusSeconds(1));
            outbox.reclaimExpiredLeases();
        }

        OutboxRecord r = outbox.byId(1);
        assertThat(r.attempts()).isEqualTo(MAX_ATTEMPTS);
        assertThat(r.status()).isEqualTo(OutboxStatus.FAILED);
        // Quarantined → no longer claimable.
        assertThat(claimAll(10)).isEmpty();
    }

    @Test
    void GIVEN_old_done_rows_WHEN_housekeeping_runs_THEN_only_terminal_rows_before_the_cutoff_are_deleted_up_to_the_batch() {
        outbox.insert(message("order-1", 1));   // id 1 → will be DONE
        outbox.insert(message("order-2", 1));   // id 2 → will be DONE
        outbox.insert(message("order-3", 1));   // id 3 → stays PENDING
        outbox.markDone(1);
        outbox.markDone(2);

        clock.advance(Duration.ofDays(1));
        Instant cutoff = clock.instant();

        int deleted = outbox.cleanup(cutoff, 1);   // batch caps at 1
        assertThat(deleted).isEqualTo(1);
        assertThat(outbox.size()).isEqualTo(2);

        // A second pass removes the other DONE row; the PENDING row is never touched.
        assertThat(outbox.cleanup(cutoff, 10)).isEqualTo(1);
        assertThat(outbox.byStatus(OutboxStatus.PENDING)).extracting(OutboxRecord::id).containsExactly(3L);
    }

    @Test
    void GIVEN_a_done_row_newer_than_the_cutoff_WHEN_housekeeping_runs_THEN_it_is_retained() {
        outbox.insert(message("order-1", 1));
        outbox.markDone(1);

        // Cutoff is before the row's creation → not eligible.
        assertThat(outbox.cleanup(clock.instant().minus(Duration.ofDays(1)), 10)).isZero();
        assertThat(outbox.size()).isEqualTo(1);
    }
}
