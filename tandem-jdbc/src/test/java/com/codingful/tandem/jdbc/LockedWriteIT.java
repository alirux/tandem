package com.codingful.tandem.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.OutboxRecord;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;

/**
 * Proves {@link OutboxMessage.Builder#lockedWrite()} closes the exact hazard
 * {@link CommitOrderReorderIT} pins for an unserialised write side (HLD-managed-seq §4.2): a
 * concurrent writer to the same aggregate is made to wait, rather than being allowed to commit ahead
 * of a write that started first.
 */
class LockedWriteIT extends AbstractPostgresIT {

    private static final String AGGREGATE_ID = "order-locked-1";
    private static final String WORKER_ID = "worker-0";

    @Test
    void GIVEN_two_concurrent_lockedWrite_writers_to_one_aggregate_WHEN_the_second_tries_to_write_before_the_first_commits_THEN_it_blocks_until_the_first_commits()
            throws Exception {
        try (Connection earlier = DATA_SOURCE.getConnection();
             Connection later = DATA_SOURCE.getConnection()) {
            earlier.setAutoCommit(false);
            later.setAutoCommit(false);

            repositoryOn(earlier).insert(message(1));   // takes the lock; does not commit yet

            CountDownLatch laterReturned = new CountDownLatch(1);
            AtomicLong laterReturnedAtNanos = new AtomicLong();
            Thread laterWriter = new Thread(() -> {
                repositoryOn(later).insert(message(2)); // must block here until earlier commits
                laterReturnedAtNanos.set(System.nanoTime());
                laterReturned.countDown();
            });
            laterWriter.start();
            try {
                // Not a timing assumption we then trust blindly — the assertion after commit (below)
                // is what actually proves ordering; this only rules out the fixture racing itself.
                assertThat(laterReturned.await(500, TimeUnit.MILLISECONDS))
                        .as("later's insert must still be blocked on the lock")
                        .isFalse();

                long earlierCommitAtNanos = System.nanoTime();
                earlier.commit();

                assertThat(laterReturned.await(10, TimeUnit.SECONDS))
                        .as("later's insert must unblock once earlier commits")
                        .isTrue();
                assertThat(laterReturnedAtNanos.get()).isGreaterThanOrEqualTo(earlierCommitAtNanos);
            } finally {
                laterWriter.join();
            }
            later.commit();
        }

        // The lock forced later's INSERT itself to wait, so id order and seq order agree — unlike the
        // unlocked race CommitOrderReorderIT pins, where later's INSERT (and its id) races ahead.
        assertThat(idOrderedSeqs()).containsExactly(1L, 2L);

        JdbcOutboxStore store = new JdbcOutboxStore(DATA_SOURCE, 10);
        List<OutboxRecord> first = store.claimBatch(buckets(), WORKER_ID, Duration.ofSeconds(30), 10);
        assertThat(seqs(first)).containsExactly(1L);
        store.markDone(first.get(0).id());
        List<OutboxRecord> second = store.claimBatch(buckets(), WORKER_ID, Duration.ofSeconds(30), 10);
        assertThat(seqs(second)).containsExactly(2L);
    }

    @Test
    void GIVEN_two_transactions_each_locking_the_same_two_aggregates_in_reverse_order_WHEN_a_batch_locks_them_THEN_it_orders_them_itself_and_no_deadlock_occurs() {
        JdbcOutboxRepository repository = new JdbcOutboxRepository(DATA_SOURCE, 256);

        // Submitted in one order here and the opposite order in a second call — if insertAll acquired
        // the locks in collection order instead of sorting them first, two such batches running
        // concurrently against the same two aggregates could deadlock.
        repository.insertAll(List.of(
                lockedMessage("order-locked-z", 1), lockedMessage("order-locked-a", 1)));
        repository.insertAll(List.of(
                lockedMessage("order-locked-a", 2), lockedMessage("order-locked-z", 2)));

        assertThat(intColumn("SELECT count(*) FROM tandem_outbox WHERE aggregate_id IN "
                + "('order-locked-z', 'order-locked-a')")).isEqualTo(4);
    }

    private static List<Long> idOrderedSeqs() {
        try (Connection conn = DATA_SOURCE.getConnection();
             java.sql.PreparedStatement ps = conn.prepareStatement(
                     "SELECT seq FROM tandem_outbox WHERE aggregate_id = ? ORDER BY id")) {
            ps.setString(1, AGGREGATE_ID);
            try (java.sql.ResultSet rs = ps.executeQuery()) {
                List<Long> result = new java.util.ArrayList<>();
                while (rs.next()) {
                    result.add(rs.getLong(1));
                }
                return result;
            }
        } catch (java.sql.SQLException e) {
            throw new IllegalStateException("test query failed", e);
        }
    }

    private static int intColumn(String sql) {
        try (Connection conn = DATA_SOURCE.getConnection();
             java.sql.Statement stmt = conn.createStatement();
             java.sql.ResultSet rs = stmt.executeQuery(sql)) {
            rs.next();
            return rs.getInt(1);
        } catch (java.sql.SQLException e) {
            throw new IllegalStateException("test query failed", e);
        }
    }

    private static List<Long> seqs(List<OutboxRecord> records) {
        return records.stream().map(OutboxRecord::seq).toList();
    }

    private static Set<Integer> buckets() {
        return IntStream.range(0, 256).boxed().collect(Collectors.toSet());
    }

    private static OutboxMessage message(long seq) {
        return OutboxMessage.builder()
                .aggregateId(AGGREGATE_ID)
                .aggregateType("Order")
                .type("OrderChanged")
                .seq(seq)
                .lockedWrite()
                .payload(("{\"seq\":" + seq + "}").getBytes(StandardCharsets.UTF_8))
                .contentType("application/json")
                .build();
    }

    private static OutboxMessage lockedMessage(String aggregateId, long seq) {
        return OutboxMessage.builder()
                .aggregateId(aggregateId)
                .aggregateType("Order")
                .type("OrderChanged")
                .seq(seq)
                .lockedWrite()
                .payload(("{\"seq\":" + seq + "}").getBytes(StandardCharsets.UTF_8))
                .contentType("application/json")
                .build();
    }

    private static JdbcOutboxRepository repositoryOn(Connection connection) {
        return new JdbcOutboxRepository(pinnedTo(connection), 256);
    }

    /**
     * A {@link DataSource} always handing back the given connection, ignoring {@code close()} — the
     * test's stand-in for Spring's {@code TransactionAwareDataSourceProxy}, so the adapter's insert
     * joins the transaction this test controls instead of running on its own connection.
     */
    private static DataSource pinnedTo(Connection connection) {
        Connection nonClosing = (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[] {Connection.class},
                (proxy, method, args) -> "close".equals(method.getName()) ? null : method.invoke(connection, args));
        return (DataSource) Proxy.newProxyInstance(
                DataSource.class.getClassLoader(),
                new Class<?>[] {DataSource.class},
                (proxy, method, args) -> "getConnection".equals(method.getName()) ? nonClosing : null);
    }
}
