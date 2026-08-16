package com.codingful.tandem.benchmark;

import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.jdbc.JdbcOutboxRepository;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;

/**
 * Provokes a <b>genuine</b> write-side ordering violation — two unserialised writers to one aggregate
 * whose commit order inverts their insert order (HLD §4.2) — so a demo can show the relay detecting
 * one rather than describing it.
 *
 * <p>Nothing here is simulated or injected: both events are written through the real
 * {@code JdbcOutboxRepository}, and the reorder comes from the same place it comes from in production
 * — {@code id} is assigned at INSERT while visibility is decided at COMMIT, so the row the relay sees
 * first is the one that committed first, not the one that was inserted first. The earlier {@code seq}
 * is simply held in an open transaction while the relay publishes the later one.
 *
 * <p>Uses a dedicated aggregate id, never one from the load generator's universe, so the two writers'
 * {@code seq} numbering is owned entirely by this class and cannot collide with generated load.
 */
final class OutOfOrderWriter {

    private final DataSource dataSource;
    private final int bucketCount;

    OutOfOrderWriter(DataSource dataSource, int bucketCount) {
        this.dataSource = dataSource;
        this.bucketCount = bucketCount;
    }

    /**
     * Write {@code seq} and {@code seq + 1} for {@code aggregateId} from two concurrent transactions,
     * committing the later one first and holding the earlier one open for {@code inversionWindow} — long
     * enough for the relay to claim, publish and mark the later event DONE before the earlier one even
     * becomes visible.
     *
     * @throws IllegalStateException if the holding transaction does not finish, so a demo phase fails
     *                               loudly instead of quietly showing a flat line
     */
    void writeInverted(String aggregateId, long seq, Duration inversionWindow) throws Exception {
        CountDownLatch committed = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        Thread holder = new Thread(() -> {
            TransactionalUnitOfWork uow = new TransactionalUnitOfWork(dataSource);
            try {
                uow.runInTransaction(conn -> {
                    new JdbcOutboxRepository(uow.transactionAware(), bucketCount)
                            .insert(message(aggregateId, seq));
                    awaitRelease(release);
                    return null;
                });
            } catch (SQLException e) {
                throw new IllegalStateException("the held-open write failed", e);
            } finally {
                committed.countDown();
            }
        }, "tandem-bench-reorder");
        holder.setDaemon(true);
        holder.start();

        // The later seq, on its own connection, committed immediately.
        TransactionalUnitOfWork later = new TransactionalUnitOfWork(dataSource);
        later.runInTransaction(conn -> {
            new JdbcOutboxRepository(later.transactionAware(), bucketCount)
                    .insert(message(aggregateId, seq + 1));
            return null;
        });

        Thread.sleep(inversionWindow.toMillis());
        release.countDown();
        if (!committed.await(30, TimeUnit.SECONDS)) {
            throw new IllegalStateException("the held-open write never committed aggregateId:" + aggregateId);
        }
    }

    private static void awaitRelease(CountDownLatch release) {
        try {
            release.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while holding the earlier write open", e);
        }
    }

    private static OutboxMessage message(String aggregateId, long seq) {
        return OutboxMessage.builder()
                .aggregateId(aggregateId)
                .aggregateType(LoadGenerator.AGGREGATE_TYPE)
                .type("bench.event")
                .seq(seq)
                .payload(("{\"reordered\":true,\"seq\":" + seq + "}").getBytes(StandardCharsets.UTF_8))
                .contentType("application/json")
                .build();
    }
}
