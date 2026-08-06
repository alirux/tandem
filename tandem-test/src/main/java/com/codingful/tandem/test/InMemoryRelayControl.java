package com.codingful.tandem.test;

import com.codingful.tandem.core.BucketStatusView;
import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.OutboxStatus;
import com.codingful.tandem.core.RelayCoordinationMode;
import com.codingful.tandem.core.RelayStatusView;
import com.codingful.tandem.core.WorkerView;
import com.codingful.tandem.core.port.RelayControl;
import com.codingful.tandem.core.port.RelayQuery;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;

/**
 * A faithful in-memory {@link RelayQuery}/{@link RelayControl} — a real collaborator for the Admin API's
 * relay use-case tests, not a mock (LLD-test §1). Mirrors {@code JdbcRelayQuery}/{@code JdbcRelayControl}'s
 * semantics: {@link RelayCoordinationMode#SINGLE} reports {@code 0} for {@code uncoveredBuckets}/
 * {@code workers} and refuses every {@code LEASE}-only bucket/worker read the same way the real adapter's
 * caller (the admin use case) is expected to short-circuit before ever calling them.
 *
 * <p>Bucket/worker state lives here; pending-row counts per bucket are read from the {@link InMemoryOutbox}
 * given at construction, exactly as the real adapter joins {@code tandem_bucket_lease} with
 * {@code tandem_outbox} — one shared source of row state, never a second copy of it.
 */
public final class InMemoryRelayControl implements RelayQuery, RelayControl {

    /** One {@code tandem_bucket_lease} row. */
    private static final class BucketLease {
        String owner;
        Instant leaseUntil;
        boolean paused;
    }

    private final InMemoryOutbox outbox;
    private final int bucketCount;
    private final Map<Integer, BucketLease> buckets = new TreeMap<>();
    private final Map<String, Instant> members = new TreeMap<>();

    private volatile RelayCoordinationMode coordinationMode = RelayCoordinationMode.SINGLE;
    private volatile boolean wholeRelayPaused;
    private volatile boolean alive = true;

    /**
     * @param outbox      the real outbox collaborator this relay's buckets belong to — {@link #buckets}
     *                    reads its rows for {@code pendingCount}/{@code lagAgeSeconds}, never a copy
     * @param bucketCount the fixed total virtual bucket count; every bucket starts unowned, unpaused
     */
    public InMemoryRelayControl(InMemoryOutbox outbox, int bucketCount) {
        this.outbox = Objects.requireNonNull(outbox, "outbox");
        if (bucketCount <= 0) {
            throw new IllegalArgumentException("bucketCount must be positive");
        }
        this.bucketCount = bucketCount;
        for (int b = 0; b < bucketCount; b++) {
            buckets.put(b, new BucketLease());
        }
    }

    // --- test affordances ---

    public void setCoordinationMode(RelayCoordinationMode mode) {
        this.coordinationMode = Objects.requireNonNull(mode, "mode");
    }

    /** @param leaseUntil the lease's expiry; a past instant makes the bucket uncovered though still owned */
    public void own(int bucket, String owner, Instant leaseUntil) {
        BucketLease lease = requireBucket(bucket);
        lease.owner = Objects.requireNonNull(owner, "owner");
        lease.leaseUntil = Objects.requireNonNull(leaseUntil, "leaseUntil");
    }

    public void addMember(String instanceId, Instant lastHeartbeat) {
        members.put(instanceId, lastHeartbeat);
    }

    /** Simulates a dead relay for {@code RelayStatus.state == DOWN} tests; true (alive) by default. */
    public void setAlive(boolean alive) {
        this.alive = alive;
    }

    // --- RelayQuery ---

    @Override
    public RelayCoordinationMode coordinationMode() {
        return coordinationMode;
    }

    @Override
    public RelayStatusView status() {
        if (coordinationMode == RelayCoordinationMode.SINGLE) {
            return new RelayStatusView(alive, wholeRelayPaused, bucketCount, 0, 0);
        }
        int uncovered = 0;
        for (Map.Entry<Integer, BucketLease> entry : buckets.entrySet()) {
            if (!covered(entry.getValue()) && pendingCount(entry.getKey()) > 0) {
                uncovered++;
            }
        }
        return new RelayStatusView(alive, wholeRelayPaused, bucketCount, uncovered, members.size());
    }

    @Override
    public List<BucketStatusView> buckets(boolean uncoveredOnly) {
        List<BucketStatusView> views = new ArrayList<>();
        for (Map.Entry<Integer, BucketLease> entry : buckets.entrySet()) {
            int bucket = entry.getKey();
            long pending = pendingCount(bucket);
            if (uncoveredOnly && (covered(entry.getValue()) || pending == 0)) {
                continue;
            }
            views.add(toView(bucket, entry.getValue(), pending));
        }
        return views;
    }

    @Override
    public Optional<BucketStatusView> bucket(int bucket) {
        BucketLease lease = buckets.get(bucket);
        return lease == null ? Optional.empty() : Optional.of(toView(bucket, lease, pendingCount(bucket)));
    }

    @Override
    public List<WorkerView> workers() {
        List<WorkerView> views = new ArrayList<>();
        for (Map.Entry<String, Instant> entry : members.entrySet()) {
            int owned = 0;
            for (BucketLease lease : buckets.values()) {
                if (entry.getKey().equals(lease.owner) && covered(lease)) {
                    owned++;
                }
            }
            views.add(new WorkerView(entry.getKey(), entry.getValue(), owned));
        }
        return views;
    }

    // --- RelayControl ---

    @Override
    public void pauseAll() {
        wholeRelayPaused = true;
    }

    @Override
    public void resumeAll() {
        wholeRelayPaused = false;
    }

    @Override
    public boolean pauseBucket(int bucket) {
        return setBucketPaused(bucket, true);
    }

    @Override
    public boolean resumeBucket(int bucket) {
        return setBucketPaused(bucket, false);
    }

    @Override
    public boolean releaseBucket(int bucket) {
        BucketLease lease = buckets.get(bucket);
        if (lease == null) {
            return false;
        }
        lease.owner = null;
        lease.leaseUntil = null;
        return true;
    }

    private boolean setBucketPaused(int bucket, boolean paused) {
        BucketLease lease = buckets.get(bucket);
        if (lease == null) {
            return false;
        }
        lease.paused = paused;
        return true;
    }

    private BucketLease requireBucket(int bucket) {
        BucketLease lease = buckets.get(bucket);
        if (lease == null) {
            throw new IllegalArgumentException("no such bucket: " + bucket);
        }
        return lease;
    }

    private static boolean covered(BucketLease lease) {
        return lease.owner != null && lease.leaseUntil != null && lease.leaseUntil.isAfter(Instant.now());
    }

    private long pendingCount(int bucket) {
        long count = 0;
        for (OutboxRecord r : outbox.all()) {
            if (r.status() == OutboxStatus.PENDING && outbox.bucketOf(r.id()) == bucket) {
                count++;
            }
        }
        return count;
    }

    private Double oldestPendingAgeSeconds(int bucket) {
        Instant oldest = null;
        for (OutboxRecord r : outbox.all()) {
            if (r.status() == OutboxStatus.PENDING && outbox.bucketOf(r.id()) == bucket) {
                if (oldest == null || r.createdAt().isBefore(oldest)) {
                    oldest = r.createdAt();
                }
            }
        }
        return oldest == null ? null : Math.max(0, (Instant.now().toEpochMilli() - oldest.toEpochMilli()) / 1000d);
    }

    private BucketStatusView toView(int bucket, BucketLease lease, long pending) {
        return new BucketStatusView(bucket, lease.owner, lease.leaseUntil, covered(lease), lease.paused,
                pending, oldestPendingAgeSeconds(bucket));
    }
}
