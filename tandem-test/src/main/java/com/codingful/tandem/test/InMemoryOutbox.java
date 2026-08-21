package com.codingful.tandem.test;

import com.codingful.tandem.core.BucketHash;
import com.codingful.tandem.core.LagSnapshot;
import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.OutboxRowDetail;
import com.codingful.tandem.core.OutboxRowView;
import com.codingful.tandem.core.OutboxSearchCriteria;
import com.codingful.tandem.core.OutboxStatus;
import com.codingful.tandem.core.ReplayCriteria;
import com.codingful.tandem.core.ReplayResult;
import com.codingful.tandem.core.TandemHeaders;
import com.codingful.tandem.core.exception.DuplicateSeqException;
import com.codingful.tandem.core.port.DiscardService;
import com.codingful.tandem.core.port.OutboxQuery;
import com.codingful.tandem.core.port.OutboxRepository;
import com.codingful.tandem.core.port.OutboxStore;
import com.codingful.tandem.core.port.ReplayService;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
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
 * {@code maxAttempts}, {@code content-type} is folded into {@code headers} at insert, and a message
 * that leaves {@code seq} to Tandem ({@code managedSeq()}) is numbered from an outbox-wide counter,
 * as the {@code tandem_seq} sequence numbers it in the database (HLD-managed-seq §4.1).
 *
 * <p>The {@link Clock} is injectable so backoff/lease/retention are deterministic.
 *
 * <p>Also implements {@link OutboxQuery} — the Admin API's read side — projecting its rows onto the
 * read model ({@link OutboxRowView}/{@link OutboxRowDetail}), never the write/relay model, matching
 * {@code JdbcOutboxQuery}'s semantics (HLD-admin-api §4). And {@link ReplayService}/
 * {@link DiscardService} — the Admin API's mutating actions (slice 2) — mirroring
 * {@code JdbcReplayService}'s/{@code JdbcDiscardService}'s reset/transition semantics exactly, so the
 * admin use cases stay unit-testable without a database.
 */
public final class InMemoryOutbox implements OutboxRepository, OutboxStore, OutboxQuery, ReplayService, DiscardService {

    /** Replayable statuses — mirrors {@code JdbcReplayService}'s own restriction. */
    private static final Set<OutboxStatus> REPLAYABLE = Set.of(OutboxStatus.DONE, OutboxStatus.FAILED);

    /** Default virtual-bucket count — matches the baseline {@code B = 256} (schema). */
    public static final int DEFAULT_BUCKET_COUNT = 256;

    /** Default max attempts before a row is quarantined to {@code FAILED} (LLD-jdbc §3.6). */
    public static final int DEFAULT_MAX_ATTEMPTS = 10;

    static final String LEASE_EXPIRED_ERROR = "lease expired (worker crash or stall) before ack";

    /**
     * One stored row: the immutable {@link OutboxRecord} plus the bucket it hashed into and the
     * lifetime replay count. {@code replays} is held here rather than on {@link OutboxRecord} for the
     * same reason the JDBC adapter keeps it out of the claim query: the relay never reads it, and a
     * field on the record would hand a claimed row a {@code replays} of 0 that looks authoritative
     * and is not.
     */
    private static final class Entry {
        OutboxRecord record;
        final int bucket;
        int replays;

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
    // Stands in for the tandem_seq sequence the JDBC adapter's managed-seq insert reads (HLD-managed-seq
    // §4.1): one counter for the whole outbox, not one per aggregate, so the numbers it hands out are
    // sparse per aggregate exactly as the database's are.
    private final AtomicLong managedSeq = new AtomicLong();
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
        OutboxMessage stored = asPersisted(message);
        String uniqueKey = stored.aggregateId().value() + '\0' + stored.seq();
        if (!uniqueKeys.add(uniqueKey)) {
            throw new DuplicateSeqException(
                    "duplicate (aggregate_id, seq) = (" + stored.aggregateId() + ", " + stored.seq() + ')');
        }
        long id = idSeq.incrementAndGet();
        int bucket = BucketHash.bucketFor(stored.aggregateId().value(), bucketCount);
        OutboxRecord record = OutboxRecord.builder()
                .id(id)
                .message(stored)
                .status(OutboxStatus.PENDING)
                .createdAt(clock.instant())
                .build();
        rows.put(id, new Entry(record, bucket));
    }

    /**
     * The message as the row holds it: {@code contentType} serialized into
     * {@code headers["content-type"]}, and a number in place of a request to assign one — the two
     * things the write side does on the way to storage (LLD-jdbc §2, HLD-managed-seq §4.1).
     * {@code lockedWrite} carries no storage effect of its own (there is no column for it — a real
     * insert only consumes it to decide whether to take the advisory lock, HLD-managed-seq §4.2), but
     * this rebuild still copies it across so the stored message keeps every flag the caller set.
     *
     * <p>The typed field is the single source of truth for the media type, exactly as in the JDBC
     * adapter: when set it overrides a {@code content-type} the caller also put in {@code headers}
     * (LLD-jdbc §2).
     */
    private OutboxMessage asPersisted(OutboxMessage message) {
        if (message.contentType() == null && !message.managedSeq()) {
            return message;
        }
        OutboxMessage.Builder b = OutboxMessage.builder()
                .aggregateId(message.aggregateId())
                .aggregateType(message.aggregateType())
                .type(message.type())
                .seq(message.managedSeq() ? managedSeq.incrementAndGet() : message.seq())
                .payload(message.payload())
                .contentType(message.contentType())
                .headers(message.headers());
        if (message.contentType() != null) {
            b.header(TandemHeaders.CONTENT_TYPE, message.contentType());
        }
        if (message.lockedWrite()) {
            b.lockedWrite();
        }
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

    /** Same reading as the JDBC store: {@code PENDING} rows only, and the oldest one's creation time. */
    @Override
    public Optional<LagSnapshot> lag() {
        synchronized (lock) {
            long pending = 0;
            Instant oldest = null;
            for (Entry e : rows.values()) {
                if (e.record.status() != OutboxStatus.PENDING) {
                    continue;
                }
                pending++;
                if (oldest == null || e.record.createdAt().isBefore(oldest)) {
                    oldest = e.record.createdAt();
                }
            }
            return Optional.of(new LagSnapshot(pending, Optional.ofNullable(oldest)));
        }
    }

    /** Same reading as the JDBC store: a live count, not an accumulated total (§4). */
    @Override
    public OptionalLong failedCount() {
        synchronized (lock) {
            long failed = 0;
            for (Entry e : rows.values()) {
                if (e.record.status() == OutboxStatus.FAILED) {
                    failed++;
                }
            }
            return OptionalLong.of(failed);
        }
    }

    /** Same reading as the JDBC store: {@code PENDING} rows an earlier {@code FAILED} row blocks (§4). */
    @Override
    public OptionalLong blockedCount() {
        synchronized (lock) {
            Set<String> failedAggregates = new HashSet<>();
            long blocked = 0;
            for (Entry e : rows.values()) {       // ascending id order (TreeMap)
                OutboxRecord r = e.record;
                String aggregate = r.aggregateId().value();
                if (r.status() == OutboxStatus.FAILED) {
                    failedAggregates.add(aggregate);
                } else if (r.status() == OutboxStatus.PENDING && failedAggregates.contains(aggregate)) {
                    // Reached after its aggregate's FAILED row in id order — the in-memory equivalent of
                    // the JDBC store's `o.id > f.first_failed_id`.
                    blocked++;
                }
            }
            return OptionalLong.of(blocked);
        }
    }

    @Override
    public OptionalInt replaysOf(long id) {
        synchronized (lock) {
            Entry e = rows.get(id);
            // Empty for an unknown row, mirroring the JDBC adapter: cleanup can delete a DONE row
            // between the ack and this lookup, and an absent row cannot be shown not to have been replayed.
            return e == null ? OptionalInt.empty() : OptionalInt.of(e.replays);
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

    // --- OutboxQuery (Admin API read side) ---

    /**
     * {@inheritDoc}
     *
     * <p>Also usable directly as a test assertion convenience.
     */
    @Override
    public Map<OutboxStatus, Long> statusCounts() {
        Map<OutboxStatus, Long> counts = new EnumMap<>(OutboxStatus.class);
        for (OutboxStatus status : OutboxStatus.values()) {
            counts.put(status, 0L);
        }
        for (OutboxRecord r : all()) {
            counts.merge(r.status(), 1L, Long::sum);
        }
        return counts;
    }

    @Override
    public List<OutboxRowView> search(OutboxSearchCriteria criteria) {
        Objects.requireNonNull(criteria, "criteria");
        synchronized (lock) {
            List<OutboxRowView> out = new ArrayList<>();
            for (Entry entry : rows.values()) {   // ascending id order (TreeMap)
                OutboxRecord r = entry.record;
                if (out.size() >= criteria.limit()) {
                    break;
                }
                if (!matches(r, criteria)) {
                    continue;
                }
                out.add(toRowView(entry));
            }
            return out;
        }
    }

    @Override
    public Optional<OutboxRowDetail> findById(long id) {
        synchronized (lock) {
            Entry e = rows.get(id);
            return e == null ? Optional.empty() : Optional.of(toRowDetail(e));
        }
    }

    private static boolean matches(OutboxRecord r, OutboxSearchCriteria criteria) {
        if (criteria.afterId() != null && r.id() <= criteria.afterId()) {
            return false;
        }
        if (criteria.status() != null && r.status() != criteria.status()) {
            return false;
        }
        if (criteria.aggregateId() != null && !r.aggregateId().equals(criteria.aggregateId())) {
            return false;
        }
        if (criteria.aggregateType() != null && !r.aggregateType().equals(criteria.aggregateType())) {
            return false;
        }
        if (criteria.type() != null && !criteria.type().equals(r.type())) {
            return false;
        }
        if (criteria.createdFrom() != null && r.createdAt().isBefore(criteria.createdFrom())) {
            return false;
        }
        if (criteria.correlationId() != null && !criteria.correlationId().equals(correlationIdOf(r))) {
            return false;
        }
        return criteria.createdTo() == null || !r.createdAt().isAfter(criteria.createdTo());
    }

    private static OutboxRowView toRowView(Entry e) {
        OutboxRecord r = e.record;
        return new OutboxRowView(
                r.id(), r.aggregateId(), r.aggregateType(), r.type(), r.seq(), r.status(), r.attempts(),
                e.replays, r.lastError(), r.discardReason(), r.nextAttemptAt(), r.lockedBy(), r.lockedUntil(),
                r.createdAt(), correlationIdOf(r));
    }

    /**
     * Stands in for the JDBC adapter's {@code correlation_id} column, which it fills from
     * {@code headers["correlation-id"]} at insert (LLD-jdbc §2) — here the headers are read directly,
     * since this outbox has no columns.
     */
    private static String correlationIdOf(OutboxRecord r) {
        return r.headers().get(TandemHeaders.CORRELATION_ID);
    }

    private static OutboxRowDetail toRowDetail(Entry e) {
        return new OutboxRowDetail(toRowView(e), e.record.payload(), e.record.headers());
    }

    // --- ReplayService (Admin API, slice 2) ---

    @Override
    public ReplayResult replay(ReplayCriteria criteria) {
        Objects.requireNonNull(criteria, "criteria");
        Set<OutboxStatus> statuses = effectiveReplayStatuses(criteria);
        if (statuses.isEmpty()) {
            return new ReplayResult(0, 0, criteria.dryRun());
        }
        synchronized (lock) {
            List<Entry> matched = new ArrayList<>();
            for (Entry entry : rows.values()) {
                if (matchesReplay(entry.record, criteria, statuses)) {
                    matched.add(entry);
                }
            }
            if (criteria.dryRun()) {
                return new ReplayResult(matched.size(), 0, true);
            }
            for (Entry entry : matched) {
                entry.record = entry.record.toBuilder()
                        .status(OutboxStatus.PENDING)
                        .attempts(0)
                        .nextAttemptAt(null)   // lastError kept, mirroring the JDBC adapter (HLD §8)
                        .lockedBy(null)
                        .lockedUntil(null)
                        .build();
                entry.replays++;   // attempts resets, the lifetime replay count does not (HLD §8)
            }
            return new ReplayResult(matched.size(), matched.size(), false);
        }
    }

    /** Requested statuses intersected with the replayable set; defaults to all replayable when unset. */
    private static Set<OutboxStatus> effectiveReplayStatuses(ReplayCriteria criteria) {
        if (criteria.statuses().isEmpty()) {
            return REPLAYABLE;
        }
        Set<OutboxStatus> effective = new LinkedHashSet<>(criteria.statuses());
        effective.retainAll(REPLAYABLE);
        return effective;
    }

    private static boolean matchesReplay(OutboxRecord r, ReplayCriteria criteria, Set<OutboxStatus> statuses) {
        if (!statuses.contains(r.status())) {
            return false;
        }
        if (criteria.aggregateId() != null && !r.aggregateId().equals(criteria.aggregateId())) {
            return false;
        }
        if (criteria.aggregateType() != null && !r.aggregateType().equals(criteria.aggregateType())) {
            return false;
        }
        if (criteria.fromId() != null && r.id() < criteria.fromId()) {
            return false;
        }
        return criteria.toId() == null || r.id() <= criteria.toId();
    }

    // --- DiscardService (Admin API, slice 2) ---

    @Override
    public boolean discard(long id, String reason) {
        synchronized (lock) {
            Entry entry = rows.get(id);
            if (entry == null || entry.record.status() != OutboxStatus.FAILED) {
                return false;
            }
            entry.record = entry.record.toBuilder()
                    .status(OutboxStatus.DISCARDED)
                    .discardReason(reason)
                    .build();
            return true;
        }
    }
}
