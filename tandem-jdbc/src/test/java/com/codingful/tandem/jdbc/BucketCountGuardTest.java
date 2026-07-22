package com.codingful.tandem.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.codingful.tandem.core.BucketCountReconciliation;
import com.codingful.tandem.core.exception.TandemConfigurationException;
import com.codingful.tandem.core.port.BucketCountStore;
import java.util.OptionalInt;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the {@link BucketCountGuard} orchestration, using a fake {@link BucketCountStore} so
 * the decision/seed/re-decide flow is exercised without a database. This is the seam the injectable
 * constructor exists for: it deterministically covers the concurrent-race branch (a peer seeds a
 * different value between our read and our seed) that a real-database test cannot force reliably.
 */
class BucketCountGuardTest {

    /** A configurable {@link BucketCountStore} double: a fixed stored value and a fixed seed result. */
    private static final class FakeStore implements BucketCountStore {
        private OptionalInt stored;
        private final int seedReturns;
        private int seedCalls;

        FakeStore(OptionalInt stored, int seedReturns) {
            this.stored = stored;
            this.seedReturns = seedReturns;
        }

        @Override
        public OptionalInt read() {
            return stored;
        }

        @Override
        public int seedIfAbsent(int candidate) {
            seedCalls++;
            // Model the atomic seed: an empty store now holds whatever won the race (seedReturns).
            stored = OptionalInt.of(seedReturns);
            return seedReturns;
        }
    }

    private static BucketCountGuard guard(FakeStore store) {
        return new BucketCountGuard(store, BucketCountReconciliation.seedOrValidate());
    }

    @Test
    void GIVEN_a_stored_value_equal_to_configured_WHEN_checked_THEN_it_proceeds_without_seeding() {
        FakeStore store = new FakeStore(OptionalInt.of(256), 256);

        assertThatCode(() -> guard(store).check(256)).doesNotThrowAnyException();
        assertThat(store.seedCalls).isZero();
    }

    @Test
    void GIVEN_a_stored_value_different_from_configured_WHEN_checked_THEN_it_fails_fast_without_seeding() {
        FakeStore store = new FakeStore(OptionalInt.of(256), 256);

        assertThatThrownBy(() -> guard(store).check(512))
                .isInstanceOf(TandemConfigurationException.class)
                .hasMessageContaining("512")
                .hasMessageContaining("256");
        assertThat(store.seedCalls).isZero();
    }

    @Test
    void GIVEN_no_stored_value_WHEN_checked_THEN_it_seeds_and_proceeds() {
        FakeStore store = new FakeStore(OptionalInt.empty(), 256);   // our seed wins with 256

        assertThatCode(() -> guard(store).check(256)).doesNotThrowAnyException();
        assertThat(store.seedCalls).isEqualTo(1);
    }

    @Test
    void GIVEN_no_stored_value_but_a_peer_seeds_a_different_one_WHEN_checked_THEN_the_race_loser_fails_fast() {
        // We read empty and decide to SEED, but a concurrent process already seeded 512; seedIfAbsent
        // returns that winning value, so the re-decision is a CONFLICT and this process fails loudly.
        FakeStore store = new FakeStore(OptionalInt.empty(), 512);

        assertThatThrownBy(() -> guard(store).check(256))
                .isInstanceOf(TandemConfigurationException.class)
                .hasMessageContaining("256")
                .hasMessageContaining("512");
        assertThat(store.seedCalls).isEqualTo(1);
    }
}
