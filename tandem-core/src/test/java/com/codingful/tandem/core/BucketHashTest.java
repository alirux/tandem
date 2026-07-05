package com.codingful.tandem.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

class BucketHashTest {

    private static final int DEFAULT_BUCKETS = 256;

    @Test
    void GIVEN_the_same_aggregate_id_WHEN_its_bucket_is_computed_repeatedly_THEN_the_bucket_is_stable() {
        int first = BucketHash.bucketFor("order-1", DEFAULT_BUCKETS);
        int second = BucketHash.bucketFor("order-1", DEFAULT_BUCKETS);

        assertThat(first).isEqualTo(second);
    }

    @Test
    void GIVEN_a_known_aggregate_id_WHEN_its_bucket_is_computed_THEN_it_returns_the_pinned_contract_bucket() {
        // The bucket is a long-lived, cross-version stored value (HLD §4.3): pin it so an accidental
        // change to the FNV-1a constants is caught. "order-1" also has a NEGATIVE raw 64-bit hash,
        // so this doubles as the negative-hash case — floorMod still yields a value in [0, B).
        assertThat(BucketHash.bucketFor("order-1", 256)).isEqualTo(109);
        assertThat(BucketHash.bucketFor("order-1", 100)).isEqualTo(9);
    }

    @Test
    void GIVEN_many_ids_and_various_bucket_counts_WHEN_their_buckets_are_computed_THEN_every_result_is_in_range() {
        // Includes non-power-of-two counts and a large sample so negative raw hashes occur — a plain
        // `% n` (instead of floorMod) would yield a negative bucket and fail this.
        for (int bucketCount : new int[] {1, 2, 7, 100, 256, 1000}) {
            for (int i = 0; i < 10_000; i++) {
                int bucket = BucketHash.bucketFor("aggregate-" + i, bucketCount);
                assertThat(bucket).isGreaterThanOrEqualTo(0).isLessThan(bucketCount);
            }
        }
    }

    @Test
    void GIVEN_a_single_bucket_WHEN_buckets_are_computed_THEN_everything_maps_to_zero() {
        for (int i = 0; i < 1_000; i++) {
            assertThat(BucketHash.bucketFor("aggregate-" + i, 1)).isZero();
        }
    }

    @Test
    void GIVEN_ten_thousand_distinct_ids_WHEN_their_buckets_are_computed_THEN_the_distribution_spreads_across_buckets() {
        Set<Integer> used = new HashSet<>();
        for (int i = 0; i < 10_000; i++) {
            used.add(BucketHash.bucketFor("aggregate-" + i, DEFAULT_BUCKETS));
        }
        // A constant-returning hash would use a single bucket; a working hash fills (nearly) all 256.
        assertThat(used).hasSizeGreaterThan(DEFAULT_BUCKETS / 2);
    }

    @Test
    void GIVEN_a_non_positive_bucket_count_WHEN_a_bucket_is_requested_THEN_it_is_rejected() {
        assertThatThrownBy(() -> BucketHash.bucketFor("order-1", 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> BucketHash.bucketFor("order-1", -4))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
