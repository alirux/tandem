package com.codingful.tandem.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Pure unit tests for {@link JdbcOutboxStore}'s argument guards (item 30) — each short-circuits
 * before ever acquiring a connection, so a data source that is never queried is enough; no Docker
 * needed. The database-observable behaviours (claim, mark-done, retry, …) stay in
 * {@code JdbcOutboxStoreIT}.
 */
class JdbcOutboxStoreTest {

    // Never queried: every guard under test returns before any connection is acquired.
    private final SimpleDataSource neverQueried = new SimpleDataSource("jdbc:none", "u", "p");

    @Test
    void GIVEN_a_non_positive_maxAttempts_WHEN_constructed_THEN_it_is_rejected() {
        assertThatThrownBy(() -> new JdbcOutboxStore(neverQueried, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void GIVEN_no_buckets_WHEN_claiming_THEN_nothing_is_claimed_without_touching_the_database() {
        JdbcOutboxStore store = new JdbcOutboxStore(neverQueried, 10);

        assertThat(store.claimBatch(Set.of(), "worker-1", Duration.ofSeconds(60), 10)).isEmpty();
    }

    @Test
    void GIVEN_a_non_positive_batchSize_WHEN_claiming_THEN_nothing_is_claimed_without_touching_the_database() {
        JdbcOutboxStore store = new JdbcOutboxStore(neverQueried, 10);

        assertThat(store.claimBatch(Set.of(1), "worker-1", Duration.ofSeconds(60), 0)).isEmpty();
    }

    @Test
    void GIVEN_no_ids_WHEN_marking_done_in_a_batch_THEN_it_returns_without_touching_the_database() {
        JdbcOutboxStore store = new JdbcOutboxStore(neverQueried, 10);

        store.markDoneBatch(List.of());   // would throw if it reached the (unreachable) database
    }

    @Test
    void GIVEN_a_non_positive_batchSize_WHEN_cleaning_up_THEN_nothing_is_deleted_without_touching_the_database() {
        JdbcOutboxStore store = new JdbcOutboxStore(neverQueried, 10);

        assertThat(store.cleanup(Instant.now(), 0)).isZero();
    }
}
