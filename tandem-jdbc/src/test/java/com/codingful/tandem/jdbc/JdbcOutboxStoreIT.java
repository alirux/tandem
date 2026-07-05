package com.codingful.tandem.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.OutboxStatus;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class JdbcOutboxStoreIT extends AbstractPostgresIT {

    private static final int BUCKETS = 256;
    private static final String WORKER = "worker-1";
    private static final Duration LEASE = Duration.ofSeconds(60);

    private final JdbcOutboxRepository repository = new JdbcOutboxRepository(DATA_SOURCE, BUCKETS);
    private final JdbcOutboxStore store = new JdbcOutboxStore(DATA_SOURCE, 10);

    private void insert(String aggregateId, long seq) {
        repository.insert(OutboxMessage.builder()
                .aggregateId(aggregateId).aggregateType("Order").seq(seq).payload("{}".getBytes()).build());
    }

    private static Set<Integer> allBuckets() {
        Set<Integer> buckets = new HashSet<>();
        for (int b = 0; b < BUCKETS; b++) {
            buckets.add(b);
        }
        return buckets;
    }

    private List<OutboxRecord> claim(int batchSize) {
        return store.claimBatch(allBuckets(), WORKER, LEASE, batchSize);
    }

    @Test
    void GIVEN_chains_across_aggregates_WHEN_claimed_THEN_only_the_locked_head_of_each_aggregate_is_returned() {
        insert("order-1", 1);   // id 1 — head
        insert("order-1", 2);   // id 2 — blocked
        insert("order-2", 1);   // id 3 — head

        List<OutboxRecord> claimed = claim(10);

        assertThat(claimed).extracting(OutboxRecord::id).containsExactlyInAnyOrder(1L, 3L);
        assertThat(claimed).allSatisfy(r -> {
            assertThat(r.status()).isEqualTo(OutboxStatus.IN_FLIGHT);
            assertThat(r.lockedBy()).isEqualTo(WORKER);
            assertThat(r.lockedUntil()).isNotNull();
        });
        assertThat(statusOf(2)).isEqualTo(OutboxStatus.PENDING.code());   // the gated successor stays PENDING
    }

    @Test
    void GIVEN_a_claimed_head_WHEN_marked_done_in_a_batch_THEN_the_successor_becomes_claimable() {
        insert("order-1", 1);
        insert("order-1", 2);

        long head = claim(10).get(0).id();
        assertThat(claim(10)).isEmpty();   // successor blocked until the head finishes
        store.markDoneBatch(List.of(head));

        assertThat(claim(10)).extracting(OutboxRecord::id).containsExactly(2L);
    }

    @Test
    void GIVEN_a_retried_row_WHEN_its_next_attempt_is_in_the_future_THEN_it_is_not_claimable_until_due() {
        insert("order-1", 1);
        long id = claim(10).get(0).id();

        store.markForRetry(id, "transient", Instant.now().plus(Duration.ofHours(1)));
        assertThat(statusOf(id)).isEqualTo(OutboxStatus.PENDING.code());
        assertThat(attemptsOf(id)).isEqualTo(1);
        assertThat(claim(10)).isEmpty();

        store.markForRetry(id, "transient", Instant.now().minusSeconds(1));   // now due
        assertThat(claim(10)).extracting(OutboxRecord::id).containsExactly(id);
    }

    @Test
    void GIVEN_a_failed_head_WHEN_present_THEN_it_blocks_its_aggregate_and_bumps_the_attempt() {
        insert("order-1", 1);
        insert("order-1", 2);
        long head = claim(10).get(0).id();

        store.markFailed(head, "poison");

        assertThat(statusOf(head)).isEqualTo(OutboxStatus.FAILED.code());
        assertThat(attemptsOf(head)).isEqualTo(1);
        assertThat(claim(10)).isEmpty();   // the FAILED head gates the chain
    }

    @Test
    void GIVEN_an_expired_lease_WHEN_reclaimed_THEN_attempts_climb_until_the_row_is_quarantined_to_failed() {
        JdbcOutboxStore quarantining = new JdbcOutboxStore(DATA_SOURCE, 2);   // maxAttempts = 2
        insert("order-1", 1);
        long id = repoFirstId();

        // First expiry → reclaimed to PENDING with attempts = 1.
        quarantining.claimBatch(allBuckets(), WORKER, Duration.ofMillis(1), 10);
        sleep(50);
        assertThat(quarantining.reclaimExpiredLeases()).isEqualTo(1);
        assertThat(statusOf(id)).isEqualTo(OutboxStatus.PENDING.code());
        assertThat(attemptsOf(id)).isEqualTo(1);

        // Second expiry reaches maxAttempts → quarantined to FAILED.
        quarantining.claimBatch(allBuckets(), WORKER, Duration.ofMillis(1), 10);
        sleep(50);
        assertThat(quarantining.reclaimExpiredLeases()).isEqualTo(1);
        assertThat(statusOf(id)).isEqualTo(OutboxStatus.FAILED.code());
        assertThat(attemptsOf(id)).isEqualTo(2);
    }

    @Test
    void GIVEN_terminal_rows_older_than_the_cutoff_WHEN_cleaned_up_THEN_they_are_deleted_and_others_kept() {
        insert("order-1", 1);   // id 1 → DONE
        insert("order-2", 1);   // id 2 → DONE
        insert("order-3", 1);   // id 3 → stays PENDING
        store.markDoneBatch(List.of(1L, 2L));

        // Cutoff after the rows' creation → both DONE rows are eligible.
        assertThat(store.cleanup(Instant.now().plus(Duration.ofHours(1)), 10)).isEqualTo(2);
        assertThat(statusOf(3)).isEqualTo(OutboxStatus.PENDING.code());
        assertThat(rowExists(1)).isFalse();
    }

    @Test
    void GIVEN_terminal_rows_newer_than_the_cutoff_WHEN_cleaned_up_THEN_they_are_retained() {
        insert("order-1", 1);
        store.markDoneBatch(List.of(1L));

        assertThat(store.cleanup(Instant.now().minus(Duration.ofHours(1)), 10)).isZero();
        assertThat(rowExists(1)).isTrue();
    }

    @Test
    void GIVEN_the_baseline_schema_WHEN_inspected_THEN_the_dispatch_and_aggregate_indexes_exist() {
        assertThat(indexNames()).contains("idx_tandem_outbox_dispatch", "idx_tandem_outbox_aggregate");
    }

    @Test
    void GIVEN_several_workers_claiming_the_SAME_buckets_concurrently_WHEN_they_race_THEN_no_row_is_claimed_twice()
            throws Exception {
        // Models the transient membership-change window (LLD-jdbc §3.2): during a bucket handover two
        // instances can momentarily both believe they own the same bucket, so both poll it. Row-level
        // exclusivity is carried by the outbox row's status + FOR UPDATE SKIP LOCKED (CLAIM_SQL), NOT by
        // bucket ownership — so even when every worker passes the identical full bucket set (the maximal
        // "everyone owns bucket X" overlap), each row must be claimed by exactly one worker.
        int aggregates = 400;
        for (int a = 0; a < aggregates; a++) {
            insert("order-" + a, 1);   // one claimable head per aggregate → 400 rows all in contention
        }

        int workers = 4;
        ExecutorService pool = Executors.newFixedThreadPool(workers);
        CountDownLatch releaseAllAtOnce = new CountDownLatch(1);
        List<Future<List<Long>>> futures = new ArrayList<>();
        for (int w = 0; w < workers; w++) {
            String workerId = "worker-" + w;
            futures.add(pool.submit(() -> {
                releaseAllAtOnce.await();
                List<Long> claimedIds = new ArrayList<>();
                List<OutboxRecord> batch;
                while (!(batch = store.claimBatch(allBuckets(), workerId, LEASE, 25)).isEmpty()) {
                    batch.forEach(r -> claimedIds.add(r.id()));
                }
                return claimedIds;
            }));
        }
        releaseAllAtOnce.countDown();   // start every worker together → maximal contention

        List<Long> allClaimed = new ArrayList<>();
        for (Future<List<Long>> f : futures) {
            allClaimed.addAll(f.get());
        }
        pool.shutdown();
        assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();

        // Every row claimed, and no id claimed by two workers (a duplicate would mean the same event
        // could be published twice by two instances — the exact double-processing the design forbids).
        assertThat(allClaimed).hasSize(aggregates);
        assertThat(new HashSet<>(allClaimed)).as("no row claimed by more than one worker").hasSize(aggregates);
    }

    @Test
    void GIVEN_head_rows_locked_by_another_open_transaction_WHEN_a_worker_claims_THEN_it_skips_exactly_those_and_loses_nothing()
            throws Exception {
        // Deterministic counterpart to the racing test above: rather than hoping two claims overlap, we
        // hold real row locks from a separate still-open transaction, then claim. This forces the exact
        // interleaving the transient handover window relies on (LLD-jdbc §3.2) — CLAIM_SQL's FOR UPDATE
        // SKIP LOCKED must hand the claimer only the un-locked rows, never a row another holds.
        for (int a = 0; a < 10; a++) {
            insert("order-" + a, 1);   // heads, ids 1..10, one per aggregate → all independently claimable
        }
        List<Long> lockedIds = List.of(1L, 3L, 5L, 7L, 9L);

        try (Connection holder = DATA_SOURCE.getConnection()) {
            holder.setAutoCommit(false);
            try (PreparedStatement ps = holder.prepareStatement(
                    "SELECT id FROM tandem_outbox WHERE id = ANY(?) FOR UPDATE")) {
                ps.setArray(1, holder.createArrayOf("bigint", lockedIds.toArray()));
                ps.executeQuery().close();   // locks held open until holder rolls back
            }

            // SKIP LOCKED never waits, so this returns immediately with only the un-locked heads.
            List<Long> claimed = store.claimBatch(allBuckets(), "worker-B", LEASE, 100)
                    .stream().map(OutboxRecord::id).toList();
            assertThat(claimed).containsExactlyInAnyOrder(2L, 4L, 6L, 8L, 10L);
            assertThat(claimed).doesNotContainAnyElementsOf(lockedIds);

            holder.rollback();   // release the locks
        }

        // Nothing was lost: the previously-locked heads were never claimed and are now claimable.
        assertThat(claim(100)).extracting(OutboxRecord::id).containsExactlyInAnyOrder(1L, 3L, 5L, 7L, 9L);
    }

    // --- helpers ---

    private long repoFirstId() {
        try (Connection conn = DATA_SOURCE.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT min(id) FROM tandem_outbox");
             ResultSet rs = ps.executeQuery()) {
            rs.next();
            return rs.getLong(1);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private static int statusOf(long id) {
        return intColumn("SELECT status FROM tandem_outbox WHERE id = ?", id);
    }

    private static int attemptsOf(long id) {
        return intColumn("SELECT attempts FROM tandem_outbox WHERE id = ?", id);
    }

    private static int intColumn(String sql, long id) {
        try (Connection conn = DATA_SOURCE.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private static boolean rowExists(long id) {
        try (Connection conn = DATA_SOURCE.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT 1 FROM tandem_outbox WHERE id = ?")) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private static Set<String> indexNames() {
        Set<String> names = new HashSet<>();
        try (Connection conn = DATA_SOURCE.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT indexname FROM pg_indexes WHERE tablename = 'tandem_outbox'");
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                names.add(rs.getString(1));
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
        return names;
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
