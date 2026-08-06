package com.codingful.tandem.admin.relay;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.codingful.tandem.admin.relay.dto.BucketStatusResponse;
import com.codingful.tandem.admin.relay.dto.RelayStatusResponse;
import com.codingful.tandem.admin.relay.dto.WorkerInfoResponse;
import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.RelayCoordinationMode;
import com.codingful.tandem.test.InMemoryOutbox;
import com.codingful.tandem.test.InMemoryRelayControl;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the relay-control use cases against a real {@link InMemoryRelayControl} collaborator —
 * no mocks (AGENTS). {@code RelayAdminControllerTest} covers HTTP binding.
 */
class RelayAdminServiceTest {

    private static final int BUCKETS = 8;

    private InMemoryRelayControl control;
    private RelayAdminService service;

    @BeforeEach
    void setUp() {
        control = new InMemoryRelayControl(new InMemoryOutbox(), BUCKETS);
        service = new RelayAdminService(control, control);
    }

    @Test
    void GIVEN_SINGLE_coordination_WHEN_status_is_read_THEN_uncovered_and_workers_are_zero() {
        RelayStatusResponse status = service.status();

        assertThat(status.state()).isEqualTo("RUNNING");
        assertThat(status.uncoveredBuckets()).isZero();
        assertThat(status.workers()).isZero();
    }

    @Test
    void GIVEN_no_bucket_selector_WHEN_paused_THEN_the_whole_relay_is_paused_regardless_of_coordination() {
        RelayStatusResponse status = service.pause(null, "test-user");

        assertThat(status.state()).isEqualTo("PAUSED");
    }

    @Test
    void GIVEN_a_paused_relay_WHEN_resumed_with_no_selector_THEN_it_is_running_again() {
        service.pause(null, "test-user");

        RelayStatusResponse status = service.resume(null, "test-user");

        assertThat(status.state()).isEqualTo("RUNNING");
    }

    @Test
    void GIVEN_no_relay_has_heartbeated_WHEN_status_is_read_THEN_it_reports_DOWN() {
        control.setAlive(false);

        RelayStatusResponse status = service.status();

        assertThat(status.state()).isEqualTo("DOWN");
    }

    @Test
    void GIVEN_a_relay_paused_before_going_down_WHEN_status_is_read_THEN_DOWN_takes_priority_over_PAUSED() {
        service.pause(null, "test-user");

        control.setAlive(false);

        assertThat(service.status().state()).isEqualTo("DOWN");
    }

    @Test
    void GIVEN_SINGLE_coordination_WHEN_a_bucket_selector_is_used_to_pause_THEN_it_is_refused() {
        assertThatThrownBy(() -> service.pause(3, "test-user")).isInstanceOf(RelayCoordinationUnsupportedException.class);
    }

    @Test
    void GIVEN_SINGLE_coordination_WHEN_a_bucket_selector_is_used_to_resume_THEN_it_is_refused() {
        assertThatThrownBy(() -> service.resume(3, "test-user")).isInstanceOf(RelayCoordinationUnsupportedException.class);
    }

    @Test
    void GIVEN_LEASE_coordination_and_a_valid_bucket_WHEN_paused_THEN_the_whole_relay_status_is_returned() {
        control.setCoordinationMode(RelayCoordinationMode.LEASE);

        RelayStatusResponse status = service.pause(3, "test-user");

        assertThat(status).isNotNull();
        assertThat(control.bucket(3).orElseThrow().paused()).isTrue();
    }

    @Test
    void GIVEN_LEASE_coordination_and_a_paused_bucket_WHEN_resumed_THEN_it_is_no_longer_paused() {
        control.setCoordinationMode(RelayCoordinationMode.LEASE);
        service.pause(3, "test-user");

        service.resume(3, "test-user");

        assertThat(control.bucket(3).orElseThrow().paused()).isFalse();
    }

    @Test
    void GIVEN_LEASE_coordination_and_a_bucket_outside_the_range_WHEN_paused_THEN_it_is_refused_as_not_found() {
        control.setCoordinationMode(RelayCoordinationMode.LEASE);

        assertThatThrownBy(() -> service.pause(99, "test-user")).isInstanceOf(RelayBucketNotFoundException.class);
    }

    @Test
    void GIVEN_LEASE_coordination_and_a_bucket_outside_the_range_WHEN_resumed_THEN_it_is_refused_as_not_found() {
        control.setCoordinationMode(RelayCoordinationMode.LEASE);

        assertThatThrownBy(() -> service.resume(99, "test-user")).isInstanceOf(RelayBucketNotFoundException.class);
    }

    @Test
    void GIVEN_SINGLE_coordination_WHEN_buckets_are_listed_THEN_it_is_refused() {
        assertThatThrownBy(() -> service.buckets(false)).isInstanceOf(RelayCoordinationUnsupportedException.class);
    }

    @Test
    void GIVEN_LEASE_coordination_and_an_uncovered_bucket_with_pending_work_WHEN_status_is_read_THEN_it_is_counted() {
        InMemoryOutbox outbox = new InMemoryOutbox(BUCKETS, 10, Clock.systemUTC());
        InMemoryRelayControl withPending = new InMemoryRelayControl(outbox, BUCKETS);
        withPending.setCoordinationMode(RelayCoordinationMode.LEASE);
        RelayAdminService serviceWithPending = new RelayAdminService(withPending, withPending);
        outbox.insert(OutboxMessage.builder()
                .aggregateId("order-1").aggregateType("Order").seq(1).payload("{}".getBytes()).build());

        RelayStatusResponse status = serviceWithPending.status();

        assertThat(status.uncoveredBuckets()).isEqualTo(1);
    }

    @Test
    void GIVEN_LEASE_coordination_WHEN_buckets_are_listed_THEN_every_bucket_is_returned() {
        control.setCoordinationMode(RelayCoordinationMode.LEASE);

        List<BucketStatusResponse> buckets = service.buckets(false);

        assertThat(buckets).hasSize(BUCKETS);
    }

    @Test
    void GIVEN_an_uncovered_bucket_with_pending_work_WHEN_uncovered_only_is_requested_THEN_it_is_included() {
        control.setCoordinationMode(RelayCoordinationMode.LEASE);
        // Matching bucket counts: bucketOf() below must land inside the [0, BUCKETS) the control fake tracks.
        InMemoryOutbox outbox = new InMemoryOutbox(BUCKETS, 10, Clock.systemUTC());
        InMemoryRelayControl withPending = new InMemoryRelayControl(outbox, BUCKETS);
        withPending.setCoordinationMode(RelayCoordinationMode.LEASE);
        RelayAdminService serviceWithPending = new RelayAdminService(withPending, withPending);
        outbox.insert(OutboxMessage.builder()
                .aggregateId("order-1").aggregateType("Order").seq(1).payload("{}".getBytes()).build());
        int bucket = outbox.bucketOf(outbox.all().get(0).id());

        List<BucketStatusResponse> uncovered = serviceWithPending.buckets(true);

        assertThat(uncovered).extracting(BucketStatusResponse::bucket).contains(bucket);
    }

    @Test
    void GIVEN_SINGLE_coordination_WHEN_a_single_bucket_is_read_THEN_it_is_refused() {
        assertThatThrownBy(() -> service.bucket(3)).isInstanceOf(RelayCoordinationUnsupportedException.class);
    }

    @Test
    void GIVEN_LEASE_coordination_and_an_owned_bucket_WHEN_read_THEN_its_status_is_returned() {
        control.setCoordinationMode(RelayCoordinationMode.LEASE);
        control.own(3, "instance-1", Instant.now().plusSeconds(60));

        BucketStatusResponse bucket = service.bucket(3);

        assertThat(bucket.bucket()).isEqualTo(3);
        assertThat(bucket.owner()).isEqualTo("instance-1");
    }

    @Test
    void GIVEN_LEASE_coordination_and_a_bucket_outside_the_range_WHEN_read_THEN_it_is_refused_as_not_found() {
        control.setCoordinationMode(RelayCoordinationMode.LEASE);

        assertThatThrownBy(() -> service.bucket(99)).isInstanceOf(RelayBucketNotFoundException.class);
    }

    @Test
    void GIVEN_SINGLE_coordination_WHEN_a_bucket_is_released_THEN_it_is_refused() {
        assertThatThrownBy(() -> service.releaseBucket(3, "test-user")).isInstanceOf(RelayCoordinationUnsupportedException.class);
    }

    @Test
    void GIVEN_LEASE_coordination_and_an_owned_bucket_WHEN_released_THEN_its_lease_is_cleared() {
        control.setCoordinationMode(RelayCoordinationMode.LEASE);
        control.own(3, "instance-1", Instant.now().plusSeconds(60));

        BucketStatusResponse released = service.releaseBucket(3, "test-user");

        assertThat(released.owner()).isNull();
        assertThat(released.covered()).isFalse();
    }

    @Test
    void GIVEN_LEASE_coordination_and_a_bucket_outside_the_range_WHEN_released_THEN_it_is_refused_as_not_found() {
        control.setCoordinationMode(RelayCoordinationMode.LEASE);

        assertThatThrownBy(() -> service.releaseBucket(99, "test-user")).isInstanceOf(RelayBucketNotFoundException.class);
    }

    @Test
    void GIVEN_SINGLE_coordination_WHEN_workers_are_listed_THEN_it_is_refused() {
        assertThatThrownBy(service::workers).isInstanceOf(RelayCoordinationUnsupportedException.class);
    }

    @Test
    void GIVEN_LEASE_coordination_and_a_live_member_WHEN_workers_are_listed_THEN_it_is_returned() {
        control.setCoordinationMode(RelayCoordinationMode.LEASE);
        control.addMember("instance-1", Instant.now());
        control.own(0, "instance-1", Instant.now().plusSeconds(60));

        List<WorkerInfoResponse> workers = service.workers();

        assertThat(workers).hasSize(1);
        assertThat(workers.get(0).workerId()).isEqualTo("instance-1");
        assertThat(workers.get(0).bucketCount()).isEqualTo(1);
    }
}
