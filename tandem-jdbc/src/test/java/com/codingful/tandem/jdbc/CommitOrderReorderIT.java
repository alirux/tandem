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
import javax.sql.DataSource;
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
