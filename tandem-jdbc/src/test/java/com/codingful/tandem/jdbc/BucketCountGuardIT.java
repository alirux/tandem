package com.codingful.tandem.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.codingful.tandem.core.exception.TandemConfigurationException;
import com.codingful.tandem.core.exception.TandemException;
import java.util.OptionalInt;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for the bucket-count guard against real PostgreSQL (LLD-bucket-count-guard).
 * {@link AbstractPostgresIT#resetTables} truncates {@code tandem_meta} before each test, so every
 * method starts with no established bucket count.
 */
class BucketCountGuardIT extends AbstractPostgresIT {

    private final JdbcBucketCountStore store = new JdbcBucketCountStore(DATA_SOURCE);

    @Test
    void GIVEN_no_established_value_WHEN_the_guard_checks_THEN_it_seeds_the_configured_value() {
        assertThat(store.read()).isEmpty();

        BucketCountGuard.check(DATA_SOURCE, 256);

        assertThat(store.read()).hasValue(256);
    }

    @Test
    void GIVEN_a_seeded_value_WHEN_the_guard_checks_the_same_value_THEN_it_proceeds() {
        BucketCountGuard.check(DATA_SOURCE, 256);

        assertThatCode(() -> BucketCountGuard.check(DATA_SOURCE, 256)).doesNotThrowAnyException();
        assertThat(store.read()).hasValue(256);
    }

    @Test
    void GIVEN_a_seeded_value_WHEN_the_guard_checks_a_different_value_THEN_it_fails_fast() {
        BucketCountGuard.check(DATA_SOURCE, 256);

        assertThatThrownBy(() -> BucketCountGuard.check(DATA_SOURCE, 512))
                .isInstanceOf(TandemConfigurationException.class)
                .hasMessageContaining("512")
                .hasMessageContaining("256");
        // The stored value is never overwritten by a conflicting check.
        assertThat(store.read()).hasValue(256);
    }

    @Test
    void GIVEN_a_value_already_stored_WHEN_seedIfAbsent_is_called_with_another_THEN_the_existing_value_wins() {
        assertThat(store.seedIfAbsent(256)).isEqualTo(256);
        assertThat(store.seedIfAbsent(512)).isEqualTo(256);   // never overwrites
        assertThat(store.read()).hasValue(256);
    }

    @Test
    void GIVEN_a_non_positive_candidate_WHEN_seedIfAbsent_is_called_THEN_it_is_rejected_before_any_write() {
        assertThatThrownBy(() -> store.seedIfAbsent(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("positive");
        assertThat(store.read()).isEmpty();   // nothing written
    }

    @Test
    void GIVEN_a_non_integer_stored_value_WHEN_the_store_reads_THEN_it_fails_clearly() {
        execute("INSERT INTO tandem_meta (key, value) VALUES ('bucket_count', 'not-a-number')");

        assertThatThrownBy(store::read)
                .isInstanceOf(TandemException.class)
                .hasMessageContaining("not-a-number");
    }

    @Test
    void GIVEN_a_repository_constructor_WHEN_built_with_any_count_THEN_it_does_no_io_and_never_guards() {
        // The constructor must not touch the database (its DataSource may be transaction-scoped, §7):
        // building a repository neither seeds nor validates, even with a count that would conflict.
        BucketCountGuard.check(DATA_SOURCE, 256);   // establish 256 explicitly

        assertThatCode(() -> new JdbcOutboxRepository(DATA_SOURCE, 512)).doesNotThrowAnyException();
        assertThat(store.read()).hasValue(256);   // unchanged by the constructor
    }

    @Test
    void GIVEN_bucket_source_selection_WHEN_forCoordination_is_called_THEN_it_stays_a_pure_selector_and_never_guards() {
        // The relay-side guard is an explicit assembly step, not folded into the pure selection factory
        // (§7): forCoordination must not seed or validate. A mismatched cfg here therefore does NOT throw.
        RelayConfig mismatched = RelayConfig.builder()
                .bucketCount(512).coordination(Coordination.SINGLE).build();

        assertThatCode(() -> BucketSource.forCoordination(mismatched, DATA_SOURCE)).doesNotThrowAnyException();
        assertThat(store.read()).isEmpty();   // untouched by the selector
    }

    @Test
    void GIVEN_no_value_WHEN_the_store_reads_THEN_it_returns_empty() {
        assertThat(store.read()).isEqualTo(OptionalInt.empty());
    }
}
