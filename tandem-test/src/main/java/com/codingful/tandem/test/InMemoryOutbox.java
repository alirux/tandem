package com.codingful.tandem.test;

import com.codingful.tandem.core.BucketHash;
import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.OutboxStatus;
import com.codingful.tandem.core.exception.DuplicateSeqException;
import com.codingful.tandem.core.port.OutboxRepository;
import com.codingful.tandem.core.port.OutboxStore;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A faithful in-memory implementation of <b>both</b> {@link OutboxRepository} (write-side) and
 * {@link OutboxStore} (relay-side) — a real collaborator for unit tests, not a mock (LLD-test §1).
 *
 * <p>It mirrors the JDBC adapter's semantics (LLD-jdbc §2/§3.3/§3.5/§3.7): the {@code bucket} is
 * computed with the <b>same core {@link BucketHash}</b> the DB path uses, {@code claimBatch} returns
 * the <b>head of each aggregate's pending chain</b> (the head-of-chain predicate that subsumes the
 * poison gate), the lease reclaim <b>counts as an attempt</b> and quarantines to {@code FAILED} at
 * {@code maxAttempts}, and {@code content-type} is folded into {@code headers} at insert.
 *
 * <p>The {@link Clock} is injectable so backoff/lease/retention are deterministic.
 */
public final class InMemoryOutbox implements OutboxRepository, OutboxStore {

    /** Default virtual-bucket count — matches the baseline {@code B = 256} (schema). */
    public static final int DEFAULT_BUCKET_COUNT = 256;

    /** Default max attempts before a row is quarantined to {@code FAILED} (LLD-jdbc §3.6). */
    public static final int DEFAULT_MAX_ATTEMPTS = 10;

    static final String LEASE_EXPIRED_ERROR = "lease expired (worker crash or stall) before ack";

    /** One stored row: the immutable {@link OutboxRecord} plus the bucket it hashed into. */
    private static final class Entry {
        OutboxRecord record;
        final int bucket;

        Entry(OutboxRecord record, int bucket) {
            this.record = record;
            this.bucket = bucket;
        }
    }

    private final int bucketCount;
    private final int maxAttempts;
    private final Clock clock;

    private final Object lock = new Object();
    private final AtomicLong idSeq = new AtomicLong();
    // id -> entry, ordered by id so head-of-chain scans are deterministic.
    private final TreeMap<Long, Entry> rows = new TreeMap<>();
    // Enforces UNIQUE(aggregate_id, seq).
    private final Set<String> uniqueKeys = new HashSet<>();

    public InMemoryOutbox() {
        this(DEFAULT_BUCKET_COUNT, DEFAULT_MAX_ATTEMPTS, Clock.systemUTC());
    }

    /**
     * @param bucketCount must match {@code BucketHash}'s bucket space used by any peer JDBC instance under test
     * @param maxAttempts retriable failures (including lease-reclaims) allowed before a row is quarantined to {@code FAILED}
     * @param clock       drives {@code createdAt} and lease/retention comparisons; inject {@link ControllableClock} for determinism
     * @throws IllegalArgumentException if {@code bucketCount <= 0} or {@code maxAttempts <= 0}
     */
    public InMemoryOutbox(int bucketCount, int maxAttempts, Clock clock) {
        if (bucketCount <= 0) {
            throw new IllegalArgumentException("bucketCount must be positive");
        }
        if (maxAttempts <= 0) {
            throw new IllegalArgumentException("maxAttempts must be positive");
        }
        this.bucketCount = bucketCount;
        this.maxAttempts = maxAttempts;
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    // --- OutboxRepository (write-side) ---

    @Override
    public void insert(OutboxMessage message) {
        synchronized (lock) {
            doInsert(message);
        }
    }

    @Override
    public void insertAll(Collection<OutboxMessage> messages) {
        synchronized (lock) {
            for (OutboxMessage message : messages) {
                doInsert(message);
            }
        }
    }

    private void doInsert(OutboxMessage message) {
        String uniqueKey = message.aggregateId().value() + '\0' + message.seq();
        if (!uniqueKeys.add(uniqueKey)) {
            throw new DuplicateSeqException(
                    "duplicate (aggregate_id, seq) = (" + message.aggregateId() + ", " + message.seq() + ')');
        }
        long id = idSeq.incrementAndGet();
        int bucket = BucketHash.bucketFor(message.aggregateId().value(), bucketCount);
        OutboxRecord record = OutboxRecord.builder()
                .id(id)
                .message(foldContentTypeIntoHeaders(message))
                .status(OutboxStatus.PENDING)
                .createdAt(clock.instant())
                .build();
        rows.put(id, new Entry(record, bucket));
    }

    /** The write-side serializes {@code contentType} into {@code headers["content-type"]} at insert (LLD-jdbc §2). */
    private static OutboxMessage foldContentTypeIntoHeaders(OutboxMessage message) {
        if (message.contentType() == null || message.headers().containsKey("content-type")) {
            return message;
        }
        OutboxMessage.Builder b = OutboxMessage.builder()
                .aggregateId(message.aggregateId())
                .aggregateType(message.aggregateType())
                .type(message.type())
                .seq(message.seq())
                .payload(message.payload())
                .contentType(message.contentType())
                .headers(message.headers());
        b.header("content-type", message.contentType());
        return b.build();
    }

    // --- OutboxStore (relay-side) ---

    @Override
    public List<OutboxRecord> claimBatch(Set<Integer> buckets, String workerId, Duration lease, int batchSize) {
        Objects.requireNonNull(buckets, "buckets");
        Objects.requireNonNull(workerId, "workerId");
        Instant now = clock.instant();
        List<OutboxRecord> claimed = new ArrayList<>();
        synchronized (lock) {
            // Aggregates already excluded by an earlier unfinished row (PENDING/IN_FLIGHT/FAILED).
            Set<String> blockedAggregates = new HashSet<>();
            for (Entry entry : rows.values()) {       // ascending id order (TreeMap)
                OutboxRecord r = entry.record;
                String aggregate = r.aggregateId().value();
                boolean unfinished = r.status() == OutboxStatus.PENDING
                        || r.status() == OutboxStatus.IN_FLIGHT
                        || r.status() == OutboxStatus.FAILED;
                if (!unfinished) {
                    continue;
                }
                // First unfinished row seen for this aggregate is its head; any later one is blocked.
                if (!blockedAggregates.add(aggregate)) {
                    continue;   // already saw an earlier unfinished row → not the head
                }
                // This row is the head of its aggregate's chain. Is it claimable now?
                if (claimed.size() >= batchSize) {
                    continue;
                }
                boolean inBucket = buckets.contains(entry.bucket);
                boolean pending = r.status() == OutboxStatus.PENDING;
                boolean dueNow = r.nextAttemptAt() == null || !r.nextAttemptAt().isAfter(now);
                if (inBucket && pending && dueNow) {
                    OutboxRecord locked = r.toBuilder()
                            .status(OutboxStatus.IN_FLIGHT)
                            .lockedBy(workerId)
                            .lockedUntil(now.plus(lease))
                            .build();
                    entry.record = locked;
                    claimed.add(locked);
                }
            }
        }
        return claimed;
    }

    @Override
    public void markDone(long id) {
        mutate(id, r -> r.toBuilder()
                .status(OutboxStatus.DONE)
                .lockedBy(null)
                .lockedUntil(null)
                .build());
    }

    @Override
    public void markForRetry(long id, String error, Duration retryDelay) {
        // The JDBC adapter anchors the delay on the DB clock; single-process, this fake's own clock is
        // the same clock the claim compares against, so anchoring here is the faithful equivalent.
        Instant nextAttemptAt = retryDelay == null ? null : clock.instant().plus(retryDelay);
        mutate(id, r -> r.toBuilder()
                .status(OutboxStatus.PENDING)
                .attempts(r.attempts() + 1)
                .lastError(error)
                .nextAttemptAt(nextAttemptAt)
                .lockedBy(null)
                .lockedUntil(null)
                .build());
    }

    @Override
    public void markFailed(long id, String error) {
        mutate(id, r -> r.toBuilder()
                .status(OutboxStatus.FAILED)
                .attempts(r.attempts() + 1)
                .lastError(error)
                .lockedBy(null)
                .lockedUntil(null)
                .build());
    }

    @Override
    public int reclaimExpiredLeases() {
        Instant now = clock.instant();
        int reclaimed = 0;
        synchronized (lock) {
            for (Entry entry : rows.values()) {
                OutboxRecord r = entry.record;
                boolean expired = r.status() == OutboxStatus.IN_FLIGHT
                        && r.lockedUntil() != null
                        && r.lockedUntil().isBefore(now);
                if (!expired) {
                    continue;
                }
                int nextAttempts = r.attempts() + 1;
                OutboxStatus nextStatus =
                        nextAttempts >= maxAttempts ? OutboxStatus.FAILED : OutboxStatus.PENDING;
                entry.record = r.toBuilder()
                        .status(nextStatus)
                        .attempts(nextAttempts)
                        .lastError(LEASE_EXPIRED_ERROR)
                        .nextAttemptAt(null)
                        .lockedBy(null)
                        .lockedUntil(null)
                        .build();
                reclaimed++;
            }
        }
        return reclaimed;
    }

    @Override
    public int cleanup(Instant doneBefore, int batchSize) {
        int deleted = 0;
        synchronized (lock) {
            List<Long> toDelete = new ArrayList<>();
            for (Entry entry : rows.values()) {
                if (deleted >= batchSize) {
                    break;
                }
                OutboxRecord r = entry.record;
                boolean terminal = r.status() == OutboxStatus.DONE || r.status() == OutboxStatus.DISCARDED;
                if (terminal && r.createdAt().isBefore(doneBefore)) {
                    toDelete.add(r.id());
                    deleted++;
                }
            }
            for (Long id : toDelete) {
                Entry removed = rows.remove(id);
                if (removed != null) {
                    String key = removed.record.aggregateId().value() + '\0' + removed.record.seq();
                    uniqueKeys.remove(key);
                }
            }
        }
        return deleted;
    }

    private void mutate(long id, java.util.function.UnaryOperator<OutboxRecord> change) {
        synchronized (lock) {
            Entry entry = rows.get(id);
            if (entry == null) {
                throw new IllegalArgumentException("no outbox row with id " + id);
            }
            entry.record = change.apply(entry.record);
        }
    }

    // --- test affordances ---

    /** All stored rows, ordered by id. */
    public List<OutboxRecord> all() {
        synchronized (lock) {
            List<OutboxRecord> out = new ArrayList<>(rows.size());
            for (Entry e : rows.values()) {
                out.add(e.record);
            }
            return out;
        }
    }

    /** Rows in the given status, ordered by id. */
    public List<OutboxRecord> byStatus(OutboxStatus status) {
        return all().stream().filter(r -> r.status() == status).toList();
    }

    public OutboxRecord byId(long id) {
        synchronized (lock) {
            Entry e = rows.get(id);
            return e == null ? null : e.record;
        }
    }

    /** The bucket a row hashed into (the value the relay would poll by). */
    public int bucketOf(long id) {
        synchronized (lock) {
            Entry e = rows.get(id);
            if (e == null) {
                throw new IllegalArgumentException("no outbox row with id " + id);
            }
            return e.bucket;
        }
    }

    /** Every bucket that currently holds at least one row — handy to claim "all" in a test. */
    public Set<Integer> occupiedBuckets() {
        synchronized (lock) {
            Set<Integer> out = new HashSet<>();
            for (Entry e : rows.values()) {
                out.add(e.bucket);
            }
            return out;
        }
    }

    /** A claim set covering every bucket {@code [0, bucketCount)} — i.e. a single worker owning all buckets. */
    public Set<Integer> allBuckets() {
        Set<Integer> out = new HashSet<>(bucketCount);
        for (int b = 0; b < bucketCount; b++) {
            out.add(b);
        }
        return out;
    }

    public int size() {
        synchronized (lock) {
            return rows.size();
        }
    }

    /** Count of rows per status — convenience for assertions. */
    public Map<OutboxStatus, Long> statusCounts() {
        Map<OutboxStatus, Long> counts = new TreeMap<>(Comparator.comparingInt(OutboxStatus::code));
        for (OutboxRecord r : all()) {
            counts.merge(r.status(), 1L, Long::sum);
        }
        return counts;
    }
}
