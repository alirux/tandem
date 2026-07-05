package com.codingful.tandem.benchmark;

import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.jdbc.JdbcOutboxRepository;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import javax.sql.DataSource;

/**
 * Drives the domain write path at a controlled offered rate through the real
 * {@link JdbcOutboxRepository} write-side API — never a raw {@code INSERT} (LLD-benchmark §4;
 * HLD-load-testing.md §3, §3.1: the system under test is the library, not PostgreSQL).
 *
 * <p>Each generated event runs, in one transaction: {@code SELECT ... FOR UPDATE} + {@code version++}
 * on a synthetic {@code bench_aggregate} row, then {@code repository.insert}, then {@code COMMIT}
 * (§4.1). One virtual thread per insert (§4.2) — real concurrency is bounded by
 * {@link #inFlightPermits}, sized to the DataSource connection pool, not by the thread count.
 */
public final class LoadGenerator implements AutoCloseable {

    static final String AGGREGATE_TYPE = "BenchAggregate";

    private static final String SELECT_FOR_UPDATE_SQL =
            "SELECT version FROM bench_aggregate WHERE aggregate_id = ? FOR UPDATE";
    private static final String BUMP_VERSION_SQL =
            "UPDATE bench_aggregate SET version = ? WHERE aggregate_id = ?";
    private static final String SEED_SQL =
            "INSERT INTO bench_aggregate (aggregate_id, version) VALUES (?, 0) ON CONFLICT (aggregate_id) DO NOTHING";

    private final TransactionalUnitOfWork unitOfWork;
    private final JdbcOutboxRepository repository;
    private final AggregateSelector selector;
    private final byte[] payload;
    private final CommitTimestamps commitTimestamps;   // nullable — only set in ACCURATE latency mode
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final Semaphore inFlightPermits;

    private final AtomicLong attempted = new AtomicLong();
    private final AtomicLong succeeded = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private final Set<String> insertedKeys = ConcurrentHashMap.newKeySet();

    private final AtomicReference<Double> targetRatePerSecond = new AtomicReference<>(0.0);
    private volatile boolean running;
    private Thread pacerThread;

    public LoadGenerator(DataSource rawDataSource, int bucketCount, int maxInFlight, AggregateSelector selector,
                          int payloadBytes, CommitTimestamps commitTimestamps) {
        this.unitOfWork = new TransactionalUnitOfWork(rawDataSource);
        this.repository = new JdbcOutboxRepository(unitOfWork.transactionAware(), bucketCount);
        this.selector = selector;
        this.payload = referencePayload(payloadBytes);
        this.commitTimestamps = commitTimestamps;
        this.inFlightPermits = new Semaphore(maxInFlight);
        seedAggregates(rawDataSource, selector.universe());
    }

    /** Starts the pacer thread offering inserts at {@code ratePerSecond}. */
    public void start(double ratePerSecond) {
        targetRatePerSecond.set(ratePerSecond);
        running = true;
        pacerThread = new Thread(this::pacerLoop, "bench-pacer");
        pacerThread.setDaemon(true);
        pacerThread.start();
    }

    /** Adjusts the offered rate while running (used by {@link RampController}). */
    public void setRate(double ratePerSecond) {
        targetRatePerSecond.set(ratePerSecond);
    }

    /** Stops offering new inserts and waits for the pacer thread to exit. In-flight inserts are not aborted. */
    public void stop() {
        running = false;
        if (pacerThread != null) {
            try {
                pacerThread.join(TimeUnit.SECONDS.toMillis(10));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void close() {
        stop();
        executor.shutdownNow();
    }

    public long attempted() {
        return attempted.get();
    }

    public long succeeded() {
        return succeeded.get();
    }

    public long failedCount() {
        return failed.get();
    }

    /** Every {@code (aggregateId, seq)} successfully committed, as {@code aggregateId + '#' + seq} — the zero-loss reconciliation set. */
    public Set<String> insertedKeys() {
        return insertedKeys;
    }

    private void pacerLoop() {
        long nextSubmitAt = System.nanoTime();
        while (running) {
            double rate = targetRatePerSecond.get();
            if (rate <= 0) {
                parkFor(Duration.ofMillis(10));
                nextSubmitAt = System.nanoTime();
                continue;
            }
            long intervalNanos = Math.max(1, (long) (1_000_000_000.0 / rate));
            long now = System.nanoTime();
            if (now < nextSubmitAt) {
                LockSupport.parkNanos(nextSubmitAt - now);
            }
            if (inFlightPermits.tryAcquire()) {
                executor.submit(() -> {
                    try {
                        insertOne();
                    } finally {
                        inFlightPermits.release();
                    }
                });
            }
            nextSubmitAt += intervalNanos;
            // Fell far behind (e.g. after an idle pause) — resync instead of bursting to catch up.
            if (nextSubmitAt < System.nanoTime() - intervalNanos) {
                nextSubmitAt = System.nanoTime();
            }
        }
    }

    /** One unit of work: {@code SELECT ... FOR UPDATE} + {@code version++} + real insert, then {@code COMMIT} (§4.1). */
    private void insertOne() {
        String aggregateId = selector.nextAggregateId();
        attempted.incrementAndGet();
        try {
            long seq = unitOfWork.runInTransaction(conn -> {
                long version = lockAndBumpVersion(conn, aggregateId);
                OutboxMessage message = OutboxMessage.builder()
                        .aggregateId(aggregateId)
                        .aggregateType(AGGREGATE_TYPE)
                        .type("bench.event")
                        .seq(version)
                        .payload(payload)
                        .contentType("application/json")
                        .header(BenchmarkHeaders.T0_NANOS, Long.toString(System.nanoTime()))
                        .build();
                repository.insert(message);
                return version;
            });
            if (commitTimestamps != null) {
                commitTimestamps.recordCommit(aggregateId, seq, System.nanoTime());
            }
            insertedKeys.add(aggregateId + '#' + seq);
            succeeded.incrementAndGet();
        } catch (Exception e) {
            failed.incrementAndGet();
        }
    }

    private long lockAndBumpVersion(Connection conn, String aggregateId) throws SQLException {
        long version;
        try (PreparedStatement select = conn.prepareStatement(SELECT_FOR_UPDATE_SQL)) {
            select.setString(1, aggregateId);
            try (ResultSet rs = select.executeQuery()) {
                if (!rs.next()) {
                    throw new SQLException("bench_aggregate row missing for " + aggregateId + " (not seeded)");
                }
                version = rs.getLong("version") + 1;
            }
        }
        try (PreparedStatement update = conn.prepareStatement(BUMP_VERSION_SQL)) {
            update.setLong(1, version);
            update.setString(2, aggregateId);
            update.executeUpdate();
        }
        return version;
    }

    private static void seedAggregates(DataSource rawDataSource, List<String> ids) {
        try (Connection conn = rawDataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SEED_SQL)) {
            for (String id : ids) {
                ps.setString(1, id);
                ps.addBatch();
            }
            ps.executeBatch();
        } catch (SQLException e) {
            throw new IllegalStateException("seeding bench_aggregate failed", e);
        }
    }

    private static byte[] referencePayload(int payloadBytes) {
        StringBuilder json = new StringBuilder(payloadBytes + 16);
        json.append("{\"pad\":\"");
        while (json.length() < payloadBytes - 2) {
            json.append('x');
        }
        json.append("\"}");
        return json.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static void parkFor(Duration d) {
        LockSupport.parkNanos(d.toNanos());
    }
}
