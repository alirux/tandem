package com.codingful.tandem.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.BucketHash;
import com.codingful.tandem.core.BucketStatusView;
import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.OutboxStatus;
import com.codingful.tandem.core.RelayCoordinationMode;
import com.codingful.tandem.core.RelayStatusView;
import com.codingful.tandem.core.WorkerView;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class JdbcRelayQueryIT extends AbstractPostgresIT {

    private final JdbcOutboxRepository repository = new JdbcOutboxRepository(DATA_SOURCE, 256);
    private final JdbcRelayQuery query = new JdbcRelayQuery(DATA_SOURCE);

    @Test
    void GIVEN_no_relay_has_ever_started_WHEN_the_coordination_mode_is_read_THEN_it_is_SINGLE() {
        assertThat(query.coordinationMode()).isEqualTo(RelayCoordinationMode.SINGLE);
    }

    @Test
    void GIVEN_a_relay_recorded_LEASE_WHEN_the_coordination_mode_is_read_THEN_it_is_LEASE() {
        execute("INSERT INTO tandem_meta (key, value) VALUES ('coordination', 'LEASE')");

        assertThat(query.coordinationMode()).isEqualTo(RelayCoordinationMode.LEASE);
    }

    @Test
    void GIVEN_no_state_recorded_WHEN_the_status_is_read_THEN_it_reports_not_paused_and_zero_SINGLE_fields() {
        RelayStatusView status = query.status();

        assertThat(status.paused()).isFalse();
        assertThat(status.uncoveredBuckets()).isZero();
        assertThat(status.workers()).isZero();
    }

    @Test
    void GIVEN_no_relay_has_ever_started_WHEN_the_status_is_read_THEN_it_is_not_alive() {
        assertThat(query.status().alive()).isFalse();
    }

    @Test
    void GIVEN_a_relay_heartbeated_within_the_threshold_WHEN_the_status_is_read_THEN_it_is_alive() {
        execute("INSERT INTO tandem_meta (key, value) VALUES ('relay_heartbeat_interval_seconds', '5')",
                "INSERT INTO tandem_meta (key, value, updated_at) VALUES ('coordination', 'SINGLE', now())");

        assertThat(query.status().alive()).isTrue();
    }

    @Test
    void GIVEN_a_relay_heartbeated_well_past_the_threshold_WHEN_the_status_is_read_THEN_it_is_not_alive() {
        // Threshold is 3x the published interval (5s here, so 15s) - an hour-old heartbeat is well
        // past it, not a borderline case.
        execute("INSERT INTO tandem_meta (key, value) VALUES ('relay_heartbeat_interval_seconds', '5')",
                "INSERT INTO tandem_meta (key, value, updated_at) VALUES ('coordination', 'SINGLE', now() - interval '1 hour')");

        assertThat(query.status().alive()).isFalse();
    }

    @Test
    void GIVEN_the_relay_is_paused_WHEN_the_status_is_read_THEN_it_reports_paused() {
        execute("INSERT INTO tandem_meta (key, value) VALUES ('relay_paused', 'true')",
                "INSERT INTO tandem_meta (key, value) VALUES ('bucket_count', '256')");

        RelayStatusView status = query.status();

        assertThat(status.paused()).isTrue();
        assertThat(status.bucketCount()).isEqualTo(256);
    }

    @Test
    void GIVEN_LEASE_coordination_WHEN_the_status_is_read_THEN_uncovered_buckets_and_workers_are_counted() {
        execute("INSERT INTO tandem_meta (key, value) VALUES ('coordination', 'LEASE')");
        insertPending("order-1");
        setBucketOwner(bucketOf("order-1"), null);   // uncovered, has pending work
        execute("INSERT INTO tandem_relay_member (owner, lease_until) VALUES ('instance-1', now() + interval '1 minute')");

        RelayStatusView status = query.status();

        assertThat(status.uncoveredBuckets()).isEqualTo(1);
        assertThat(status.workers()).isEqualTo(1);
    }

    @Test
    void GIVEN_an_owned_unpaused_bucket_WHEN_buckets_are_listed_THEN_it_is_covered_and_not_paused() {
        int bucket = bucketOf("order-1");
        setBucketOwner(bucket, "instance-1");

        List<BucketStatusView> buckets = query.buckets(false);

        BucketStatusView view = buckets.stream().filter(b -> b.bucket() == bucket).findFirst().orElseThrow();
        assertThat(view.owner()).isEqualTo("instance-1");
        assertThat(view.covered()).isTrue();
        assertThat(view.paused()).isFalse();
    }

    @Test
    void GIVEN_a_bucket_with_pending_rows_and_no_owner_WHEN_uncovered_only_is_requested_THEN_it_is_included() {
        int bucket = bucketOf("order-1");
        insertPending("order-1");

        List<BucketStatusView> uncovered = query.buckets(true);

        assertThat(uncovered).extracting(BucketStatusView::bucket).contains(bucket);
        BucketStatusView view = uncovered.stream().filter(b -> b.bucket() == bucket).findFirst().orElseThrow();
        assertThat(view.pendingCount()).isEqualTo(1);
        assertThat(view.lagAgeSeconds()).isNotNull();
    }

    @Test
    void GIVEN_a_covered_bucket_WHEN_uncovered_only_is_requested_THEN_it_is_excluded() {
        int bucket = bucketOf("order-1");
        insertPending("order-1");
        setBucketOwner(bucket, "instance-1");

        assertThat(query.buckets(true)).extracting(BucketStatusView::bucket).doesNotContain(bucket);
    }

    @Test
    void GIVEN_an_existing_bucket_number_WHEN_looked_up_THEN_its_status_is_returned() {
        assertThat(query.bucket(5)).isPresent();
    }

    @Test
    void GIVEN_a_bucket_number_outside_the_seeded_range_WHEN_looked_up_THEN_it_is_empty() {
        assertThat(query.bucket(99_999)).isEmpty();
    }

    @Test
    void GIVEN_a_live_member_WHEN_workers_are_listed_THEN_it_is_returned_with_its_bucket_count() {
        execute("INSERT INTO tandem_relay_member (owner, lease_until) VALUES ('instance-1', now() + interval '1 minute')");
        setBucketOwner(1, "instance-1");
        setBucketOwner(2, "instance-1");

        List<WorkerView> workers = query.workers();

        assertThat(workers).hasSize(1);
        assertThat(workers.get(0).instanceId()).isEqualTo("instance-1");
        assertThat(workers.get(0).bucketCount()).isEqualTo(2);
    }

    @Test
    void GIVEN_a_member_whose_presence_has_expired_WHEN_workers_are_listed_THEN_it_is_excluded() {
        execute("INSERT INTO tandem_relay_member (owner, lease_until) VALUES ('dead-instance', now() - interval '1 minute')");

        assertThat(query.workers()).isEmpty();
    }

    private void insertPending(String aggregateId) {
        repository.insert(OutboxMessage.builder()
                .aggregateId(aggregateId).aggregateType("Order").seq(1).payload("{}".getBytes()).build());
    }

    private int bucketOf(String aggregateId) {
        return BucketHash.bucketFor(aggregateId, 256);
    }

    private static void setBucketOwner(int bucket, String owner) {
        if (owner == null) {
            execute("UPDATE tandem_bucket_lease SET owner = NULL, lease_until = NULL WHERE bucket = " + bucket);
        } else {
            execute("UPDATE tandem_bucket_lease SET owner = '" + owner + "', lease_until = now() + interval '1 minute'"
                    + " WHERE bucket = " + bucket);
        }
    }
}
