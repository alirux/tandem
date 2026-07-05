package com.codingful.tandem.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.codingful.tandem.core.exception.TandemConfigurationException;
import java.lang.System.Logger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class BucketLeaseManagerIT extends AbstractPostgresIT {

    private static final int BUCKETS = 256;
    private static final Duration LEASE = Duration.ofSeconds(60);
    private static final Logger LOG = System.getLogger(BucketLeaseManagerIT.class.getName());

    private BucketLeaseManager manager(String instanceId) {
        return new BucketLeaseManager(DATA_SOURCE, instanceId, BUCKETS, LEASE);
    }

    @Test
    void GIVEN_a_lease_table_seeded_to_the_bucket_count_WHEN_validated_on_start_THEN_it_passes() {
        RecordingMetrics metrics = new RecordingMetrics();

        assertThatCode(() -> manager("instance-1").validateOnStart(metrics, LOG)).doesNotThrowAnyException();
        assertThat(metrics.configInvalidChecks()).isEmpty();
    }

    @Test
    void GIVEN_a_bucket_count_not_matching_the_seeded_lease_rows_WHEN_validated_on_start_THEN_it_fails_fast() {
        // The baseline seeds 256 rows; a manager expecting a different B is a misconfiguration.
        BucketLeaseManager mismatched = new BucketLeaseManager(DATA_SOURCE, "instance-1", 128, LEASE);
        RecordingMetrics metrics = new RecordingMetrics();

        assertThatThrownBy(() -> mismatched.validateOnStart(metrics, LOG))
                .isInstanceOf(TandemConfigurationException.class)
                .hasMessageContaining("tandem_bucket_lease")
                .hasMessageContaining("128");
        assertThat(metrics.configInvalidChecks())
                .containsExactly(BucketLeaseManager.CHECK_BUCKET_LEASE_SEEDED);
    }

    @Test
    void GIVEN_a_single_instance_WHEN_it_heartbeats_against_free_buckets_THEN_it_claims_them_all() {
        BucketLeaseManager only = manager("instance-1");

        only.heartbeat();

        assertThat(only.ownedBuckets()).hasSize(BUCKETS);
    }

    @Test
    void GIVEN_an_instance_owning_buckets_WHEN_it_releases_THEN_the_buckets_become_free_again() {
        BucketLeaseManager only = manager("instance-1");
        only.heartbeat();

        only.release();

        assertThat(only.ownedBuckets()).isEmpty();
        // A fresh instance can now claim everything.
        BucketLeaseManager next = manager("instance-2");
        next.heartbeat();
        assertThat(next.ownedBuckets()).hasSize(BUCKETS);
    }

    @Test
    void GIVEN_another_instances_leases_have_expired_WHEN_a_survivor_heartbeats_THEN_it_reclaims_the_expired_buckets() {
        manager("instance-1").heartbeat();   // claims all + registers presence
        // Simulate instance-1 dying: force both its bucket leases and its membership into the past.
        execute("UPDATE tandem_bucket_lease SET lease_until = now() - interval '1 hour' WHERE owner = 'instance-1'",
                "UPDATE tandem_relay_member SET lease_until = now() - interval '1 hour' WHERE owner = 'instance-1'");

        BucketLeaseManager survivor = manager("instance-2");
        survivor.heartbeat();

        assertThat(survivor.ownedBuckets()).hasSize(BUCKETS);
    }

    @Test
    void GIVEN_an_incumbent_owning_everything_WHEN_a_new_instance_joins_and_both_heartbeat_THEN_the_split_converges_to_the_fair_share() {
        BucketLeaseManager incumbent = manager("instance-1");
        BucketLeaseManager joiner = manager("instance-2");
        incumbent.heartbeat();   // sole instance: claims all 256, registers presence
        assertThat(incumbent.ownedBuckets()).hasSize(BUCKETS);

        // A plain scale-up: the joiner starts with zero buckets. Membership presence (not ownership)
        // makes it visible, so the incumbent releases its excess and the fleet rebalances to 128/128 —
        // the starvation the S8 load test surfaced (LLD-benchmark §8.2) no longer holds.
        for (int round = 0; round < 5; round++) {
            joiner.heartbeat();
            incumbent.heartbeat();
        }

        assertThat(incumbent.ownedBuckets()).hasSize(BUCKETS / 2);
        assertThat(joiner.ownedBuckets()).hasSize(BUCKETS / 2);
    }

    @Test
    void GIVEN_multiple_instances_heartbeating_CONCURRENTLY_WHEN_they_contend_for_buckets_THEN_ownership_stays_disjoint_fully_covered_and_fair()
            throws Exception {
        int instances = 4;
        List<BucketLeaseManager> managers = new ArrayList<>();
        for (int i = 0; i < instances; i++) {
            managers.add(manager("instance-" + i));
        }

        ExecutorService pool = Executors.newFixedThreadPool(instances);
        try {
            // Hammer heartbeat() from all instances at the same instant, repeatedly. A barrier aligns
            // each round so their CLAIM_DEFICIT / RELEASE_EXCESS updates truly overlap in the DB — the
            // FOR UPDATE SKIP LOCKED contention path the sequential tests above never exercise.
            for (int round = 0; round < 8; round++) {
                CyclicBarrier startTogether = new CyclicBarrier(instances);
                List<Future<?>> futures = new ArrayList<>();
                for (BucketLeaseManager m : managers) {
                    futures.add(pool.submit(() -> {
                        startTogether.await();
                        m.heartbeat();
                        return null;
                    }));
                }
                for (Future<?> f : futures) {
                    f.get();   // propagate any deadlock / SQL error from a worker as a test failure
                }
                // Quiescent snapshot after each concurrent round: no bucket is ever owned by two.
                assertNoBucketOwnedTwice(managers);
            }
        } finally {
            pool.shutdown();
            assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        }

        // Sequential settling sweeps close any transient released-not-yet-reclaimed gap, then assert the
        // strong steady-state invariants.
        for (int sweep = 0; sweep < 3; sweep++) {
            managers.forEach(BucketLeaseManager::heartbeat);
        }
        assertNoBucketOwnedTwice(managers);
        assertThat(unionOfOwned(managers)).as("full coverage — no bucket permanently orphaned").hasSize(BUCKETS);
        for (BucketLeaseManager m : managers) {
            // Fair share is 256/4 = 64; none starved to 0 (the original bug), none hogging everything.
            assertThat(m.ownedBuckets()).hasSizeBetween(BUCKETS / instances - 16, BUCKETS / instances + 16);
        }
    }

    @Test
    void GIVEN_free_buckets_locked_by_another_open_transaction_WHEN_an_instance_heartbeats_THEN_claimDeficit_skips_exactly_those()
            throws Exception {
        // Deterministic proof (no timing) that claimDeficit's FOR UPDATE SKIP LOCKED never assigns a
        // bucket a peer is concurrently claiming: hold real locks on some free bucket rows from a
        // separate still-open transaction, heartbeat, and confirm the instance claims every free bucket
        // EXCEPT the locked ones (LLD-jdbc §3.2). Complements the concurrent stress test above.
        List<Integer> locked = List.of(0, 1, 2, 3, 4);
        BucketLeaseManager instance = manager("instance-1");

        try (Connection holder = DATA_SOURCE.getConnection()) {
            holder.setAutoCommit(false);
            try (PreparedStatement ps = holder.prepareStatement(
                    "SELECT bucket FROM tandem_bucket_lease WHERE bucket = ANY(?) FOR UPDATE")) {
                ps.setArray(1, holder.createArrayOf("integer", locked.toArray()));
                ps.executeQuery().close();   // locks held open until holder rolls back
            }

            instance.heartbeat();   // sole live member → target is all 256, but 5 buckets are locked away
            Set<Integer> owned = instance.ownedBuckets();
            assertThat(owned).doesNotContainAnyElementsOf(locked);
            assertThat(owned).hasSize(BUCKETS - locked.size());   // claimed every free bucket it could reach

            holder.rollback();   // release the locks
        }

        // Once released, a follow-up heartbeat picks up the previously-locked buckets — full coverage.
        instance.heartbeat();
        assertThat(instance.ownedBuckets()).hasSize(BUCKETS);
    }

    @Test
    void GIVEN_a_live_peer_owning_half_the_buckets_WHEN_an_instance_heartbeats_THEN_it_claims_only_its_fair_share_of_the_free_half() {
        // A live peer owns buckets 0..127; 128..255 are free.
        execute("UPDATE tandem_bucket_lease SET owner = 'peer', lease_until = now() + interval '1 hour' WHERE bucket < 128");

        BucketLeaseManager joining = manager("instance-2");
        joining.heartbeat();

        // Two live workers → fair share ceil(256/2) = 128, taken from the free half.
        Set<Integer> owned = joining.ownedBuckets();
        assertThat(owned).hasSize(128);
        assertThat(owned).allMatch(b -> b >= 128);
    }

    @Test
    void GIVEN_more_relays_than_buckets_WHEN_they_heartbeat_THEN_every_bucket_is_covered_and_the_surplus_relay_owns_nothing() {
        // 3 buckets, 4 relays. Fair share is ceil(3/4) = 1, but only 3 buckets exist — so three relays
        // own one bucket each and the fourth is a hot standby that owns nothing. This is a stable
        // equilibrium (no error, no churn): the surplus relay keeps finding zero free buckets to claim,
        // and would only take over if one of the three others died. Full coverage is never at risk.
        int buckets = 3;
        int relays = 4;
        execute("DELETE FROM tandem_bucket_lease",
                "INSERT INTO tandem_bucket_lease (bucket) SELECT generate_series(0, 2)::smallint");
        try {
            List<BucketLeaseManager> managers = new ArrayList<>();
            for (int i = 0; i < relays; i++) {
                managers.add(new BucketLeaseManager(DATA_SOURCE, "instance-" + i, buckets, LEASE));
            }
            // A few rounds let the greedy assignment settle (the incumbent releases its excess, others claim).
            for (int round = 0; round < 5; round++) {
                managers.forEach(BucketLeaseManager::heartbeat);
            }

            Set<Integer> union = new HashSet<>();
            int owningRelays = 0;
            int idleRelays = 0;
            int totalOwned = 0;
            for (BucketLeaseManager m : managers) {
                Set<Integer> owned = m.ownedBuckets();
                assertThat(owned).hasSizeLessThanOrEqualTo(1);   // fair share is 1 bucket
                if (owned.isEmpty()) {
                    idleRelays++;
                } else {
                    owningRelays++;
                }
                union.addAll(owned);
                totalOwned += owned.size();
            }
            assertThat(union).containsExactlyInAnyOrder(0, 1, 2);   // every bucket covered
            assertThat(totalOwned).isEqualTo(buckets);              // and each covered exactly once (disjoint)
            assertThat(owningRelays).isEqualTo(3);
            assertThat(idleRelays).as("the surplus relay owns nothing").isEqualTo(1);
        } finally {
            // Restore the shared lease table to the baseline 256 rows for the other tests.
            execute("DELETE FROM tandem_bucket_lease",
                    "INSERT INTO tandem_bucket_lease (bucket) SELECT generate_series(0, 255)::smallint");
        }
    }

    /** Asserts the instances' owned-bucket sets are pairwise disjoint — no bucket owned by two at once. */
    private static void assertNoBucketOwnedTwice(List<BucketLeaseManager> managers) {
        Set<Integer> seen = new HashSet<>();
        for (BucketLeaseManager m : managers) {
            for (int bucket : m.ownedBuckets()) {
                assertThat(seen.add(bucket)).as("bucket %s owned by more than one instance", bucket).isTrue();
            }
        }
    }

    private static Set<Integer> unionOfOwned(List<BucketLeaseManager> managers) {
        Set<Integer> union = new HashSet<>();
        managers.forEach(m -> union.addAll(m.ownedBuckets()));
        return union;
    }
}
