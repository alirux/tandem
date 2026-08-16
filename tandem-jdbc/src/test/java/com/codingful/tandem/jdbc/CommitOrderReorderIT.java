package com.codingful.tandem.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.AggregateId;
import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.ReplayCriteria;
import com.codingful.tandem.test.RecordingDispatcher;
import com.codingful.tandem.test.RecordingMetrics;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Pins the hazard of HLD §4.2: the relay dispatches by {@code id}, and {@code id} is assigned at
 * INSERT while visibility is decided at COMMIT — so two concurrent writers on one aggregate that
 * commit in the opposite order to their inserts are published out of order. Neither
 * {@code UNIQUE(aggregate_id, seq)} nor the head-of-chain gate can catch it: the earlier row is
 * simply not visible to the claim's snapshot yet.
 *
 * <p>Run at {@code bucketCount} 1 and 256 because a single bucket does <b>not</b> repair it: one
 * aggregate always maps to one bucket, so B never changes per-aggregate ordering — the defect is
 * upstream of the relay, in the commit-order gap. Pinning both ends the recurring question of
 * whether serialising the relay would compensate for an unserialised write-side.
 */
class CommitOrderReorderIT extends AbstractPostgresIT {

    private static final String AGGREGATE_ID = "order-1";
    private static final String WORKER_ID = "worker-0";

    @ParameterizedTest
    @ValueSource(ints = {1, 256})
    void GIVEN_two_concurrent_writes_to_one_aggregate_WHEN_the_later_one_commits_first_THEN_the_relay_dispatches_them_out_of_order(
            int bucketCount) throws Exception {
        JdbcOutboxStore store = new JdbcOutboxStore(DATA_SOURCE, 10);
        Set<Integer> allBuckets = buckets(bucketCount);

        try (Connection earlier = DATA_SOURCE.getConnection();
             Connection later = DATA_SOURCE.getConnection()) {
            earlier.setAutoCommit(false);
            later.setAutoCommit(false);

            // seq=1 gets the lower id (inserted first) but stays uncommitted;
            // seq=2 gets the higher id and commits first.
            repositoryOn(earlier, bucketCount).insert(message(1));
            repositoryOn(later, bucketCount).insert(message(2));
            later.commit();

            List<OutboxRecord> whileEarlierIsUncommitted = store.claimBatch(
                    allBuckets, WORKER_ID, Duration.ofSeconds(30), 10);

            earlier.commit();

            List<OutboxRecord> afterEarlierCommits = store.claimBatch(
                    allBuckets, WORKER_ID, Duration.ofSeconds(30), 10);

            // The id order is correct — the write-side numbered them 1 then 2 ...
            assertThat(whileEarlierIsUncommitted).allSatisfy(
                    claimed -> assertThat(claimed.id()).isEqualTo(2L));
            // ... yet seq=2 is dispatched first, and seq=1 only after it commits.
            assertThat(seqs(whileEarlierIsUncommitted)).containsExactly(2L);
            assertThat(seqs(afterEarlierCommits)).containsExactly(1L);
        }
    }

    /**
     * The control: the same two writes, committed in insert order, come out in order — so the
     * reorder above is caused by the commit order, not by the concurrency or the fixture.
     */
    @ParameterizedTest
    @ValueSource(ints = {1, 256})
    void GIVEN_two_writes_to_one_aggregate_WHEN_they_commit_in_insert_order_THEN_the_relay_dispatches_them_in_order(
            int bucketCount) throws Exception {
        JdbcOutboxStore store = new JdbcOutboxStore(DATA_SOURCE, 10);
        Set<Integer> allBuckets = buckets(bucketCount);

        try (Connection earlier = DATA_SOURCE.getConnection();
             Connection later = DATA_SOURCE.getConnection()) {
            earlier.setAutoCommit(false);
            later.setAutoCommit(false);

            repositoryOn(earlier, bucketCount).insert(message(1));
            repositoryOn(later, bucketCount).insert(message(2));
            earlier.commit();
            later.commit();

            // One at a time: the head-of-chain gate holds seq=2 back until seq=1 is DONE.
            List<OutboxRecord> first = store.claimBatch(allBuckets, WORKER_ID, Duration.ofSeconds(30), 10);
            assertThat(seqs(first)).containsExactly(1L);
            store.markDone(first.get(0).id());

            List<OutboxRecord> second = store.claimBatch(allBuckets, WORKER_ID, Duration.ofSeconds(30), 10);
            assertThat(seqs(second)).containsExactly(2L);
        }
    }

    /**
     * The detector against the real hazard, end to end (§3.9, HLD §8): the relay itself is the only
     * witness, since the two rows are left in the table perfectly ordered — {@code seq} 1 on the lower
     * {@code id} — and no later query could tell that they went out the other way round.
     */
    @Test
    void GIVEN_two_concurrent_writes_to_one_aggregate_WHEN_the_later_one_commits_first_THEN_the_relay_reports_an_ordering_violation()
            throws Exception {
        JdbcOutboxStore store = new JdbcOutboxStore(DATA_SOURCE, 10);
        RecordingDispatcher dispatcher = new RecordingDispatcher();
        RecordingMetrics metrics = new RecordingMetrics();
        RelayWorker worker = workerOver(store, dispatcher, metrics);

        try (Connection earlier = DATA_SOURCE.getConnection();
             Connection later = DATA_SOURCE.getConnection()) {
            earlier.setAutoCommit(false);
            later.setAutoCommit(false);

            repositoryOn(earlier, 256).insert(message(1));
            repositoryOn(later, 256).insert(message(2));
            later.commit();

            worker.claimAndDispatch();          // only seq=2 is visible
            worker.flushDone();

            earlier.commit();

            worker.claimAndDispatch();          // seq=1, behind an already-published seq=2
            worker.flushDone();
        }

        assertThat(seqs(dispatcher.dispatched())).containsExactly(2L, 1L);
        assertThat(metrics.seqRegressions()).isEqualTo(1);
        // The evidence the detector had to work without: the persisted rows are in perfect order.
        assertThat(intColumn("SELECT count(*) FROM tandem_outbox o WHERE EXISTS ("
                + " SELECT 1 FROM tandem_outbox e WHERE e.aggregate_id = o.aggregate_id"
                + " AND e.id < o.id AND e.seq > o.seq)")).isZero();
    }

    /**
     * The control that got the first implementation of this detector reverted: an operator replay
     * re-publishes an event behind one already published, and only {@code replays} on the row tells it
     * apart from the violation above.
     */
    @Test
    void GIVEN_an_operator_replays_an_already_published_event_WHEN_the_relay_publishes_it_again_THEN_no_violation_is_reported() {
        JdbcOutboxStore store = new JdbcOutboxStore(DATA_SOURCE, 10);
        RecordingDispatcher dispatcher = new RecordingDispatcher();
        RecordingMetrics metrics = new RecordingMetrics();
        RelayWorker worker = workerOver(store, dispatcher, metrics);
        JdbcOutboxRepository repository = new JdbcOutboxRepository(DATA_SOURCE, 256);
        repository.insert(message(1));
        repository.insert(message(2));
        drain(worker);

        new JdbcReplayService(DATA_SOURCE).replay(
                new ReplayCriteria(AggregateId.of(AGGREGATE_ID), null, 1L, 1L, Set.of(), false));
        drain(worker);

        assertThat(seqs(dispatcher.dispatched())).containsExactly(1L, 2L, 1L);
        assertThat(intColumn("SELECT replays FROM tandem_outbox WHERE id = 1")).isEqualTo(1);
        assertThat(metrics.seqRegressions()).isZero();
    }

    private static RelayWorker workerOver(JdbcOutboxStore store, RecordingDispatcher dispatcher,
                                          RecordingMetrics metrics) {
        RelayConfig cfg = RelayConfig.builder().bucketCount(256).maxAttempts(10).build();
        return new RelayWorker(store, dispatcher, cfg, attempts -> Duration.ofSeconds(10),
                metrics, Clock.systemUTC(), WORKER_ID, () -> buckets(256));
    }

    private static void drain(RelayWorker worker) {
        for (int i = 0; i < 100; i++) {
            int claimed = worker.claimAndDispatch();
            worker.flushDone();
            worker.flushFailures();
            if (claimed == 0 && worker.inFlight() == 0) {
                return;
            }
        }
        throw new IllegalStateException("relay did not quiesce");
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

    private static Set<Integer> buckets(int bucketCount) {
        return java.util.stream.IntStream.range(0, bucketCount).boxed().collect(java.util.stream.Collectors.toSet());
    }

    private static OutboxMessage message(long seq) {
        return OutboxMessage.builder()
                .aggregateId(AGGREGATE_ID)
                .aggregateType("Order")
                .type("OrderChanged")
                .seq(seq)
                .payload(("{\"seq\":" + seq + "}").getBytes(StandardCharsets.UTF_8))
                .contentType("application/json")
                .build();
    }

    private static JdbcOutboxRepository repositoryOn(Connection connection, int bucketCount) {
        return new JdbcOutboxRepository(pinnedTo(connection), bucketCount);
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
