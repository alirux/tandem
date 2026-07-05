package com.codingful.tandem.benchmark.scenario;

import com.codingful.tandem.benchmark.AggregateSelector;
import com.codingful.tandem.benchmark.BenchmarkConfig;
import com.codingful.tandem.benchmark.BenchmarkEnvironment;
import com.codingful.tandem.benchmark.CorrelationConsumer;
import com.codingful.tandem.benchmark.LatencyRecorder;
import com.codingful.tandem.benchmark.LoadGenerator;
import com.codingful.tandem.benchmark.RelayInstance;
import com.codingful.tandem.jdbc.Coordination;
import com.codingful.tandem.jdbc.RelayConfig;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * S8 — multi-instance {@code LEASE} coordination, including a simulated crash (HLD §3.2 axis 2;
 * LLD-jdbc §3.2): the "N embedded replicas" case a naive {@code SINGLE} deployment gets wrong — every
 * instance would poll every bucket, multiplying DB load for zero throughput gain (HLD §3.2: "running
 * more than one relay instance without LEASE is a misconfiguration, not a corruption").
 *
 * <p>Runs <b>three</b> relay instances under {@code Coordination.LEASE} against one shared outbox,
 * confirms the three-way partition stabilizes (disjoint, full coverage), then <b>kills one</b>
 * (`WorkerPool.kill()` — an abrupt crash, not a graceful {@code stop()}: buckets and presence stay
 * claimed until their lease actually expires, exactly like a real process dying) and confirms the two
 * survivors reclaim its share and delivery still completes with per-aggregate order intact. The same
 * mechanism {@code EmbeddedLeaseIT}'s
 * {@code GIVEN_three_lease_instances_WHEN_one_is_killed_mid_drain} proves at unit-test scale, exercised
 * here through the real Kafka + correlation-consumer machinery the other scenarios use.
 *
 * <p>Not a throughput-scaling demonstration: the shared Postgres/Kafka remain the bottleneck regardless
 * of instance count. What this proves is the *coordination* claim — disjoint ownership, no wasted
 * overlapping polling, self-healing failover, identical correctness — not that three instances move
 * more events than one.
 *
 * <p><b>History (LLD-benchmark §8.2, discoveries #10):</b> this scenario originally exposed a
 * maximally-lopsided two-instance split (one owning everything, the other 0) as a stable equilibrium —
 * {@code BucketLeaseManager} derived liveness purely from bucket ownership, so a zero-owned joiner was
 * invisible to an incumbent holding everything. Fixed in {@code tandem-jdbc} by decoupling presence
 * from ownership ({@code tandem_relay_member}); a scale-up now rebalances on its own. The kill-recovery
 * step added here needed its own small `tandem-jdbc` addition, {@code WorkerPool.kill()} — the existing
 * {@code stop()} always releases the bucket source, which for `LEASE` cleans up immediately and so
 * cannot exercise the lease-expiry self-heal path at all.
 */
public final class S8MultiInstanceLease implements Scenario {

    private static final int INSTANCE_COUNT = 3;
    private static final double OFFERED_RATE_PER_SECOND = 50.0;
    private static final Duration BUCKET_LEASE = Duration.ofSeconds(10);
    private static final Duration RECLAIM_INTERVAL = Duration.ofMillis(200);   // converge ownership quickly
    private static final Duration PARTITION_TIMEOUT = Duration.ofSeconds(15);
    private static final Duration RECOVERY_TIMEOUT = BUCKET_LEASE.plusSeconds(15);   // must exceed BUCKET_LEASE

    @Override
    public String id() {
        return "S8";
    }

    @Override
    public ScenarioResult run(ScenarioContext ctx) throws Exception {
        BenchmarkEnvironment env = ctx.environment();
        BenchmarkConfig cfg = ctx.config();

        List<RelayInstance> instances = new ArrayList<>();
        for (int i = 1; i <= INSTANCE_COUNT; i++) {
            RelayInstance instance = env.newRelayInstance(leaseConfig(env, "s8-instance-" + i));
            instances.add(instance);
            instance.pool().start();
        }

        try (KafkaConsumer<String, byte[]> kafkaConsumer = env.newConsumer("bench-s8");
             CorrelationConsumer consumer = new CorrelationConsumer(
                     kafkaConsumer, new LatencyRecorder(Duration.ofSeconds(10)), null);
             LoadGenerator generator = new LoadGenerator(env.dataSource(), cfg.bucketCount(), cfg.maxConnections(),
                     AggregateSelector.uniform(id(), cfg.aggregateCardinality()), cfg.payloadBytes(), null)) {
            consumer.start();
            generator.start(OFFERED_RATE_PER_SECOND);

            // Let the three-way partition stabilize before pulling the plug on one — and wait for a
            // genuinely FAIR split, not just disjoint+full-coverage: that weaker condition is already
            // met the instant the very first instance claims everything (round 1, before it has even
            // learned its peers exist), which would let us "kill" a victim owning zero buckets — a
            // vacuous test of the reclaim path. Discovered by actually running this scenario, not by
            // inspection: an early version used the weaker check and did exactly that.
            ScenarioSupport.awaitUpTo(PARTITION_TIMEOUT, () -> fairlyPartitioned(instances, cfg.bucketCount()));
            int killedOwnedBeforeKill = instances.get(INSTANCE_COUNT - 1).bucketSource().ownedBuckets().size();

            // kill(), not stop(): an abrupt crash — the victim's buckets/presence stay claimed until
            // their lease actually expires, not released immediately as a graceful stop() would.
            RelayInstance killed = instances.remove(INSTANCE_COUNT - 1);
            killed.pool().kill();

            // The two survivors only learn the third is gone once its lease expires — not immediately.
            ScenarioSupport.awaitUpTo(RECOVERY_TIMEOUT, () -> fairlyPartitioned(instances, cfg.bucketCount()));

            Thread.sleep(cfg.duration().toMillis());
            generator.stop();
            ScenarioSupport.waitForDrain(env.lagProbe(), id(), ScenarioSupport.maxDuration(Duration.ofMinutes(2), cfg.duration()));

            Set<Integer> owned1 = instances.get(0).bucketSource().ownedBuckets();
            Set<Integer> owned2 = instances.get(1).bucketSource().ownedBuckets();
            boolean survivorsDisjoint = disjoint(owned1, owned2);
            int survivorsCovered = union(owned1, owned2).size();

            ScenarioSupport.CorrectnessReport report = ScenarioSupport.verify(generator, consumer);
            boolean passed = report.passed() && survivorsDisjoint && survivorsCovered == cfg.bucketCount();

            return new ScenarioResult(id(), passed,
                    String.format(
                            "3 instances, killed 1 (owned %d buckets pre-kill); survivors own %d + %d "
                                    + "(disjoint=%s, covered=%d/%d); ordering violations=%d, missing=%d, duplicates=%d",
                            killedOwnedBeforeKill, owned1.size(), owned2.size(), survivorsDisjoint,
                            survivorsCovered, cfg.bucketCount(), report.orderingViolations(),
                            report.missingKeys().size(), report.duplicateCount()),
                    Map.of(
                            "killedOwnedBeforeKill", killedOwnedBeforeKill,
                            "survivor1Buckets", owned1.size(),
                            "survivor2Buckets", owned2.size(),
                            "survivorsDisjoint", survivorsDisjoint,
                            "survivorsCovered", survivorsCovered,
                            "orderingViolations", report.orderingViolations(),
                            "missingCount", report.missingKeys().size(),
                            "duplicateCount", report.duplicateCount()));
        } finally {
            for (RelayInstance instance : instances) {
                instance.pool().stop();
            }
        }
    }

    private static RelayConfig leaseConfig(BenchmarkEnvironment env, String instanceId) {
        return env.relayConfigBuilder()
                .coordination(Coordination.LEASE)
                .instanceId(instanceId)
                .bucketLease(BUCKET_LEASE)
                .reclaimInterval(RECLAIM_INTERVAL)
                .build();
    }

    /** True once the given instances' owned-bucket sets are pairwise disjoint and together cover every bucket. */
    private static boolean fullyPartitioned(List<RelayInstance> instances, int bucketCount) {
        Set<Integer> covered = new HashSet<>();
        for (RelayInstance instance : instances) {
            Set<Integer> owned = instance.bucketSource().ownedBuckets();
            for (Integer bucket : owned) {
                if (!covered.add(bucket)) {
                    return false;   // already claimed by another instance -> not yet disjoint
                }
            }
        }
        return covered.size() == bucketCount;
    }

    /**
     * {@link #fullyPartitioned} plus a fairness floor: every instance must own at least half of an
     * even split. Disjoint + full coverage alone is too weak a "stable" signal — it is already true
     * the instant the first instance to heartbeat claims everything, before it has learned any peers
     * exist (round 1 of the converge-over-several-heartbeats algorithm, LLD-jdbc §3.2).
     */
    private static boolean fairlyPartitioned(List<RelayInstance> instances, int bucketCount) {
        if (!fullyPartitioned(instances, bucketCount)) {
            return false;
        }
        int minimumFairShare = bucketCount / (instances.size() * 2);
        for (RelayInstance instance : instances) {
            if (instance.bucketSource().ownedBuckets().size() < minimumFairShare) {
                return false;
            }
        }
        return true;
    }

    private static boolean disjoint(Set<Integer> a, Set<Integer> b) {
        Set<Integer> intersection = new HashSet<>(a);
        intersection.retainAll(b);
        return intersection.isEmpty();
    }

    private static Set<Integer> union(Set<Integer> a, Set<Integer> b) {
        Set<Integer> u = new HashSet<>(a);
        u.addAll(b);
        return u;
    }
}
