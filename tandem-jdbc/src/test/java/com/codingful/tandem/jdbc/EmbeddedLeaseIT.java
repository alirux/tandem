package com.codingful.tandem.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.port.OutboxStore;
import com.codingful.tandem.core.port.TandemMetrics;
import com.codingful.tandem.test.RecordingDispatcher;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

/**
 * Two relay instances under {@link Coordination#LEASE} sharing one outbox (the embedded-multi-replica
 * case, HLD §3.2). Proves that lease coordination partitions buckets across instances and preserves the
 * zero-loss + per-aggregate-order contract — the same guarantees a single {@code SINGLE} instance gives.
 */
class EmbeddedLeaseIT extends AbstractPostgresIT {

    private static final int BUCKETS = 256;
    private static final int AGGREGATES = 20;
    private static final int PER_AGGREGATE = 10;
    private static final int TOTAL = AGGREGATES * PER_AGGREGATE;

    @Test
    void GIVEN_two_lease_instances_on_one_outbox_WHEN_they_drain_it_THEN_buckets_are_partitioned_and_every_event_is_delivered_in_order() {
        JdbcOutboxRepository repository = new JdbcOutboxRepository(DATA_SOURCE, BUCKETS);
        for (int a = 0; a < AGGREGATES; a++) {
            for (int seq = 1; seq <= PER_AGGREGATE; seq++) {
                repository.insert(OutboxMessage.builder()
                        .aggregateId("order-" + a).aggregateType("Order").seq(seq)
                        .payload(("{\"seq\":" + seq + "}").getBytes(StandardCharsets.UTF_8)).build());
            }
        }
        OutboxStore store = new JdbcOutboxStore(DATA_SOURCE, 10);

        RelayConfig cfg1 = leaseConfig("relay-1");
        RelayConfig cfg2 = leaseConfig("relay-2");
        BucketSource buckets1 = BucketSource.forCoordination(cfg1, DATA_SOURCE);
        BucketSource buckets2 = BucketSource.forCoordination(cfg2, DATA_SOURCE);
        RecordingDispatcher disp1 = new RecordingDispatcher();
        RecordingDispatcher disp2 = new RecordingDispatcher();
        WorkerPool pool1 = pool(store, disp1, cfg1, buckets1);
        WorkerPool pool2 = pool(store, disp2, cfg2, buckets2);

        pool1.start();
        pool2.start();
        try {
            awaitUpTo(Duration.ofSeconds(30),
                    () -> disp1.dispatchCount() + disp2.dispatchCount() == TOTAL);

            // Ownership is lease-partitioned and self-registration rebalances a plain scale-up: await the
            // steady state — disjoint (no bucket owned by both) and full coverage (every bucket owned by
            // exactly one instance). A brief rebalance window can leave released-not-yet-reclaimed buckets
            // momentarily free, so await rather than sample once.
            awaitUpTo(Duration.ofSeconds(10), () -> disjoint(buckets1, buckets2)
                    && union(buckets1.ownedBuckets(), buckets2.ownedBuckets()).size() == BUCKETS);

            Set<Integer> owned1 = buckets1.ownedBuckets();
            Set<Integer> owned2 = buckets2.ownedBuckets();
            assertThat(intersection(owned1, owned2)).as("buckets owned by both instances").isEmpty();
            assertThat(union(owned1, owned2))
                    .containsExactlyInAnyOrderElementsOf(IntStream.range(0, BUCKETS).boxed().toList());
        } finally {
            pool1.stop();
            pool2.stop();
        }

        // Zero loss + no duplicates: every (aggregate, seq) delivered exactly once across both instances.
        List<OutboxRecord> all = concat(disp1.dispatched(), disp2.dispatched());
        assertThat(all).hasSize(TOTAL);
        assertThat(all.stream().map(r -> r.aggregateId().value() + '#' + r.seq()).distinct().count())
                .isEqualTo(TOTAL);

        // Per-aggregate order preserved within each instance (ownership is disjoint, so an aggregate's
        // whole chain is dispatched by its single owning instance, in seq order).
        assertOrderedPerAggregate(disp1.dispatched());
        assertOrderedPerAggregate(disp2.dispatched());
    }

    @Test
    void GIVEN_three_lease_instances_WHEN_one_is_killed_mid_drain_THEN_survivors_reclaim_its_share_and_delivery_completes() {
        JdbcOutboxRepository repository = new JdbcOutboxRepository(DATA_SOURCE, BUCKETS);
        for (int a = 0; a < AGGREGATES; a++) {
            for (int seq = 1; seq <= PER_AGGREGATE; seq++) {
                repository.insert(OutboxMessage.builder()
                        .aggregateId("order-" + a).aggregateType("Order").seq(seq)
                        .payload(("{\"seq\":" + seq + "}").getBytes(StandardCharsets.UTF_8)).build());
            }
        }
        OutboxStore store = new JdbcOutboxStore(DATA_SOURCE, 10);

        // A short bucketLease/presence lease so the crash's effect (staleness discovered only on
        // expiry, not immediately) is observable in test time, not the 30s RelayConfig default.
        Duration shortLease = Duration.ofSeconds(2);
        RelayConfig cfg1 = leaseConfig("relay-1", shortLease);
        RelayConfig cfg2 = leaseConfig("relay-2", shortLease);
        RelayConfig cfg3 = leaseConfig("relay-3", shortLease);
        BucketSource buckets1 = BucketSource.forCoordination(cfg1, DATA_SOURCE);
        BucketSource buckets2 = BucketSource.forCoordination(cfg2, DATA_SOURCE);
        BucketSource buckets3 = BucketSource.forCoordination(cfg3, DATA_SOURCE);
        RecordingDispatcher disp1 = new RecordingDispatcher();
        RecordingDispatcher disp2 = new RecordingDispatcher();
        RecordingDispatcher disp3 = new RecordingDispatcher();
        WorkerPool pool1 = pool(store, disp1, cfg1, buckets1);
        WorkerPool pool2 = pool(store, disp2, cfg2, buckets2);
        WorkerPool pool3 = pool(store, disp3, cfg3, buckets3);

        pool1.start();
        pool2.start();
        pool3.start();
        try {
            // Let the three-way partition stabilize before pulling the plug on one — and wait for a
            // genuinely FAIR split, not just disjoint+full-coverage. That weaker condition is already
            // true the instant the very first instance to heartbeat claims everything, before it has
            // even learned its peers exist (round 1 of the converge-over-several-heartbeats algorithm,
            // §3.2) — which would let the test "kill" a victim owning zero buckets, a vacuous exercise
            // of the reclaim path. Found by this test itself flaking on that exact snapshot.
            awaitUpTo(Duration.ofSeconds(10), () -> disjoint(buckets1, buckets2) && disjoint(buckets1, buckets3)
                    && disjoint(buckets2, buckets3)
                    && union(union(buckets1.ownedBuckets(), buckets2.ownedBuckets()), buckets3.ownedBuckets()).size() == BUCKETS
                    && hasAFairShare(buckets1, 3, BUCKETS) && hasAFairShare(buckets2, 3, BUCKETS)
                    && hasAFairShare(buckets3, 3, BUCKETS));
            assertThat(buckets3.ownedBuckets()).as("instance-3 must actually own something before the kill").isNotEmpty();

            // kill(), not stop(): simulates an abrupt crash — instance-3's buckets and presence stay
            // claimed until their lease expires, not released immediately (LLD-jdbc §3.2).
            pool3.kill();

            // The survivors only learn instance-3 is gone once its lease (2s) actually expires, not
            // immediately — this is the point of the test (contrast the graceful-stop scenario above).
            awaitUpTo(Duration.ofSeconds(20), () -> disjoint(buckets1, buckets2)
                    && union(buckets1.ownedBuckets(), buckets2.ownedBuckets()).size() == BUCKETS
                    && hasAFairShare(buckets1, 2, BUCKETS) && hasAFairShare(buckets2, 2, BUCKETS));

            // Every event still gets delivered — including whatever instance-3 had already claimed but
            // not yet dispatched, redelivered once the survivors reclaim its (former) buckets.
            awaitUpTo(Duration.ofSeconds(20),
                    () -> disp1.dispatchCount() + disp2.dispatchCount() + disp3.dispatchCount() >= TOTAL);
        } finally {
            pool1.stop();
            pool2.stop();
            pool3.stop();   // already killed; a harmless no-op (idempotent, running is already false)
        }

        // Zero loss: every (aggregate, seq) delivered at least once across all three (duplicates from
        // the kill are expected and tolerated — the row-carried exclusivity contract, never a reorder).
        List<OutboxRecord> all = concat(concat(disp1.dispatched(), disp2.dispatched()), disp3.dispatched());
        assertThat(all.stream().map(r -> r.aggregateId().value() + '#' + r.seq()).distinct().count())
                .isEqualTo(TOTAL);
        assertOrderedPerAggregate(disp1.dispatched());
        assertOrderedPerAggregate(disp2.dispatched());
        assertOrderedPerAggregate(disp3.dispatched());
    }

    @Test
    void GIVEN_three_lease_instances_WHEN_TWO_are_killed_mid_drain_THEN_the_sole_survivor_reclaims_the_whole_ring_and_delivery_completes() {
        JdbcOutboxRepository repository = new JdbcOutboxRepository(DATA_SOURCE, BUCKETS);
        for (int a = 0; a < AGGREGATES; a++) {
            for (int seq = 1; seq <= PER_AGGREGATE; seq++) {
                repository.insert(OutboxMessage.builder()
                        .aggregateId("order-" + a).aggregateType("Order").seq(seq)
                        .payload(("{\"seq\":" + seq + "}").getBytes(StandardCharsets.UTF_8)).build());
            }
        }
        OutboxStore store = new JdbcOutboxStore(DATA_SOURCE, 10);

        Duration shortLease = Duration.ofSeconds(2);   // observe the crash effect in test time, not 30s
        RelayConfig cfg1 = leaseConfig("relay-1", shortLease);
        RelayConfig cfg2 = leaseConfig("relay-2", shortLease);
        RelayConfig cfg3 = leaseConfig("relay-3", shortLease);
        BucketSource buckets1 = BucketSource.forCoordination(cfg1, DATA_SOURCE);
        BucketSource buckets2 = BucketSource.forCoordination(cfg2, DATA_SOURCE);
        BucketSource buckets3 = BucketSource.forCoordination(cfg3, DATA_SOURCE);
        RecordingDispatcher disp1 = new RecordingDispatcher();
        RecordingDispatcher disp2 = new RecordingDispatcher();
        RecordingDispatcher disp3 = new RecordingDispatcher();
        WorkerPool pool1 = pool(store, disp1, cfg1, buckets1);
        WorkerPool pool2 = pool(store, disp2, cfg2, buckets2);
        WorkerPool pool3 = pool(store, disp3, cfg3, buckets3);

        pool1.start();
        pool2.start();
        pool3.start();
        try {
            // Wait for a genuinely fair three-way split, so the two victims each actually own a share
            // before the kill (see the one-kill test for why disjoint+coverage alone is too weak).
            awaitUpTo(Duration.ofSeconds(10), () -> disjoint(buckets1, buckets2) && disjoint(buckets1, buckets3)
                    && disjoint(buckets2, buckets3)
                    && union(union(buckets1.ownedBuckets(), buckets2.ownedBuckets()), buckets3.ownedBuckets()).size() == BUCKETS
                    && hasAFairShare(buckets1, 3, BUCKETS) && hasAFairShare(buckets2, 3, BUCKETS)
                    && hasAFairShare(buckets3, 3, BUCKETS));
            assertThat(buckets2.ownedBuckets()).as("victim relay-2 must own something before the kill").isNotEmpty();
            assertThat(buckets3.ownedBuckets()).as("victim relay-3 must own something before the kill").isNotEmpty();

            // A double crash: kill TWO instances abruptly. Neither releases eagerly (kill(), not stop()),
            // so both dead instances' buckets and presence rows linger until their leases expire.
            pool2.kill();
            pool3.kill();

            // Once both dead leases expire, the sole survivor prunes both stale members, recomputes its
            // fair share as ceil(B / 1) = B, and reclaims the ENTIRE ring — two dead shares at once.
            awaitUpTo(Duration.ofSeconds(20), () -> buckets1.ownedBuckets().size() == BUCKETS);

            // All events still get delivered — whatever the dead instances left behind is drained by the
            // survivor once it owns their former buckets.
            awaitUpTo(Duration.ofSeconds(20),
                    () -> disp1.dispatchCount() + disp2.dispatchCount() + disp3.dispatchCount() >= TOTAL);
        } finally {
            pool1.stop();
            pool2.stop();   // already killed — idempotent no-op
            pool3.stop();
        }

        // Zero loss: every (aggregate, seq) delivered at least once across all three (kill-time duplicates
        // are tolerated — row-carried exclusivity, never a reorder), each instance's stream still ordered.
        List<OutboxRecord> all = concat(concat(disp1.dispatched(), disp2.dispatched()), disp3.dispatched());
        assertThat(all.stream().map(r -> r.aggregateId().value() + '#' + r.seq()).distinct().count())
                .isEqualTo(TOTAL);
        assertOrderedPerAggregate(disp1.dispatched());
        assertOrderedPerAggregate(disp2.dispatched());
        assertOrderedPerAggregate(disp3.dispatched());
    }

    @Test
    void GIVEN_the_whole_fleet_crashes_at_once_WHEN_the_instances_restart_at_random_times_THEN_coverage_recovers_and_delivery_completes()
            throws Exception {
        JdbcOutboxRepository repository = new JdbcOutboxRepository(DATA_SOURCE, BUCKETS);
        for (int a = 0; a < AGGREGATES; a++) {
            for (int seq = 1; seq <= PER_AGGREGATE; seq++) {
                repository.insert(OutboxMessage.builder()
                        .aggregateId("order-" + a).aggregateType("Order").seq(seq)
                        .payload(("{\"seq\":" + seq + "}").getBytes(StandardCharsets.UTF_8)).build());
            }
        }
        OutboxStore store = new JdbcOutboxStore(DATA_SOURCE, 10);

        Duration shortLease = Duration.ofSeconds(2);
        RelayConfig cfg1 = leaseConfig("relay-1", shortLease);
        RelayConfig cfg2 = leaseConfig("relay-2", shortLease);
        RelayConfig cfg3 = leaseConfig("relay-3", shortLease);
        BucketSource buckets1 = BucketSource.forCoordination(cfg1, DATA_SOURCE);
        BucketSource buckets2 = BucketSource.forCoordination(cfg2, DATA_SOURCE);
        BucketSource buckets3 = BucketSource.forCoordination(cfg3, DATA_SOURCE);
        RecordingDispatcher disp1 = new RecordingDispatcher();
        RecordingDispatcher disp2 = new RecordingDispatcher();
        RecordingDispatcher disp3 = new RecordingDispatcher();
        WorkerPool pool1 = pool(store, disp1, cfg1, buckets1);
        WorkerPool pool2 = pool(store, disp2, cfg2, buckets2);
        WorkerPool pool3 = pool(store, disp3, cfg3, buckets3);

        pool1.start();
        pool2.start();
        pool3.start();
        try {
            awaitUpTo(Duration.ofSeconds(10), () -> disjoint(buckets1, buckets2) && disjoint(buckets1, buckets3)
                    && disjoint(buckets2, buckets3)
                    && union(union(buckets1.ownedBuckets(), buckets2.ownedBuckets()), buckets3.ownedBuckets()).size() == BUCKETS);

            // Simulate an emergency: the whole fleet crashes at once (no graceful release — every
            // instance's buckets and presence linger in the DB until their leases expire).
            pool1.kill();
            pool2.kill();
            pool3.kill();

            // ...then the pods come back in a random order, staggered — WorkerPool.start() re-arms a
            // killed pool. Each restarted instance keeps its instanceId, so it renews its own still-tagged
            // buckets and/or reclaims expired ones; the fleet must re-converge whatever the restart order.
            List<WorkerPool> restartOrder = new ArrayList<>(List.of(pool1, pool2, pool3));
            Collections.shuffle(restartOrder);
            for (WorkerPool p : restartOrder) {
                Thread.sleep(ThreadLocalRandom.current().nextInt(300));
                p.start();
            }

            awaitUpTo(Duration.ofSeconds(20), () -> disjoint(buckets1, buckets2) && disjoint(buckets1, buckets3)
                    && disjoint(buckets2, buckets3)
                    && union(union(buckets1.ownedBuckets(), buckets2.ownedBuckets()), buckets3.ownedBuckets()).size() == BUCKETS
                    && hasAFairShare(buckets1, 3, BUCKETS) && hasAFairShare(buckets2, 3, BUCKETS)
                    && hasAFairShare(buckets3, 3, BUCKETS));
            awaitUpTo(Duration.ofSeconds(20),
                    () -> disp1.dispatchCount() + disp2.dispatchCount() + disp3.dispatchCount() >= TOTAL);
        } finally {
            pool1.stop();
            pool2.stop();
            pool3.stop();
        }

        // The crash + chaotic restart lost nothing and reordered nothing.
        List<OutboxRecord> all = concat(concat(disp1.dispatched(), disp2.dispatched()), disp3.dispatched());
        assertThat(all.stream().map(r -> r.aggregateId().value() + '#' + r.seq()).distinct().count())
                .isEqualTo(TOTAL);
        assertOrderedPerAggregate(disp1.dispatched());
        assertOrderedPerAggregate(disp2.dispatched());
        assertOrderedPerAggregate(disp3.dispatched());
    }

    private RelayConfig leaseConfig(String instanceId) {
        return leaseConfig(instanceId, Duration.ofSeconds(30));   // RelayConfig's own bucketLease default
    }

    private RelayConfig leaseConfig(String instanceId, Duration bucketLease) {
        return RelayConfig.builder()
                .bucketCount(BUCKETS)
                .coordination(Coordination.LEASE)
                .instanceId(instanceId)
                .bucketLease(bucketLease)
                .workersPerInstance(2)
                .pollInterval(Duration.ofMillis(10))
                .reclaimInterval(Duration.ofMillis(200))   // first heartbeat populates ownership quickly
                .build();
    }

    private static WorkerPool pool(OutboxStore store, RecordingDispatcher dispatcher, RelayConfig cfg,
                                   BucketSource buckets) {
        return new WorkerPool(store, dispatcher, cfg, TandemMetrics.NOOP, Clock.systemUTC(),
                BackoffStrategy.fullJitter(), buckets);
    }

    private static void assertOrderedPerAggregate(List<OutboxRecord> dispatched) {
        dispatched.stream()
                .collect(Collectors.groupingBy(r -> r.aggregateId().value(),
                        Collectors.mapping(OutboxRecord::seq, Collectors.toList())))
                .forEach((aggregate, seqs) ->
                        assertThat(seqs).as("order within %s", aggregate).isSorted());
    }

    private static boolean disjoint(BucketSource a, BucketSource b) {
        return intersection(a.ownedBuckets(), b.ownedBuckets()).isEmpty();
    }

    /** True once {@code source} owns at least half of an even {@code bucketCount / liveInstances} split. */
    private static boolean hasAFairShare(BucketSource source, int liveInstances, int bucketCount) {
        return source.ownedBuckets().size() >= bucketCount / (liveInstances * 2);
    }

    private static Set<Integer> intersection(Set<Integer> a, Set<Integer> b) {
        Set<Integer> i = new HashSet<>(a);
        i.retainAll(b);
        return i;
    }

    private static Set<Integer> union(Set<Integer> a, Set<Integer> b) {
        return java.util.stream.Stream.concat(a.stream(), b.stream()).collect(Collectors.toSet());
    }

    private static List<OutboxRecord> concat(List<OutboxRecord> a, List<OutboxRecord> b) {
        return java.util.stream.Stream.concat(a.stream(), b.stream()).toList();
    }

    private static void awaitUpTo(Duration timeout, BooleanSupplier condition) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while awaiting condition", e);
            }
        }
        throw new AssertionError("condition not met within " + timeout);
    }
}
