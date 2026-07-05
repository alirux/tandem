package com.codingful.tandem.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.codingful.tandem.core.port.TandemMetrics;
import java.lang.System.Logger;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class BucketSourceTest {

    private static final Logger LOG = System.getLogger(BucketSourceTest.class.getName());
    // Never queried: the SINGLE branch ignores it and the LEASE branch only constructs the manager.
    private final SimpleDataSource dataSource = new SimpleDataSource("jdbc:none", "u", "p");

    @Test
    void GIVEN_the_embedded_source_WHEN_read_THEN_it_owns_every_bucket() {
        BucketSource source = BucketSource.embedded(8);

        assertThat(source.ownedBuckets())
                .containsExactlyInAnyOrderElementsOf(IntStream.range(0, 8).boxed().toList());
    }

    @Test
    void GIVEN_the_embedded_source_WHEN_the_lease_lifecycle_hooks_run_THEN_they_are_no_ops() {
        BucketSource source = BucketSource.embedded(4);

        assertThatCode(() -> {
            source.heartbeat();
            source.validateOnStart(TandemMetrics.NOOP, LOG);
            source.release();
        }).doesNotThrowAnyException();
    }

    @Test
    void GIVEN_coordination_single_WHEN_the_source_is_selected_THEN_it_is_the_embedded_all_buckets_source() {
        RelayConfig cfg = RelayConfig.builder().bucketCount(16).coordination(Coordination.SINGLE).build();

        BucketSource source = BucketSource.forCoordination(cfg, dataSource);

        assertThat(source.ownedBuckets()).hasSize(16);
    }

    @Test
    void GIVEN_coordination_lease_WHEN_the_source_is_selected_THEN_it_is_a_bucket_lease_manager() {
        RelayConfig cfg = RelayConfig.builder()
                .bucketCount(16).coordination(Coordination.LEASE).instanceId("relay-1").build();

        BucketSource source = BucketSource.forCoordination(cfg, dataSource);

        assertThat(source).isInstanceOf(BucketLeaseManager.class);
    }
}
