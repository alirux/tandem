package com.codingful.tandem.admin.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.codingful.tandem.admin.outbox.dto.DiscardRequest;
import com.codingful.tandem.admin.outbox.dto.OutboxEntryPageResponse;
import com.codingful.tandem.admin.outbox.dto.OutboxEntryResponse;
import com.codingful.tandem.admin.outbox.dto.OutboxSummaryResponse;
import com.codingful.tandem.admin.outbox.dto.ReplayRequest;
import com.codingful.tandem.admin.outbox.dto.ReplayResultResponse;
import com.codingful.tandem.core.AggregateId;
import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.OutboxSearchCriteria;
import com.codingful.tandem.core.port.OutboxStore;
import com.codingful.tandem.test.InMemoryOutbox;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the slice-1 use cases against a real {@link InMemoryOutbox} collaborator — no
 * mocks (AGENTS). {@code OutboxAdminController}'s own tests cover HTTP binding; these cover the
 * behaviour a controller has nothing to do with.
 */
class OutboxAdminServiceTest {

    private static final Instant NOW = Instant.parse("2026-08-02T12:00:00Z");

    private final InMemoryOutbox outbox = new InMemoryOutbox();
    private final OutboxAdminService service = new OutboxAdminService(
            outbox, outbox, outbox, outbox, new ObjectMapper(), Clock.fixed(NOW, ZoneOffset.UTC));

    private void insert(String aggregateId, long seq, String payload) {
        outbox.insert(OutboxMessage.builder()
                .aggregateId(aggregateId)
                .aggregateType("Order")
                .seq(seq)
                .payload(payload.getBytes())
                .header("correlation-id", "abc")
                .build());
    }

    @Test
    void GIVEN_an_empty_outbox_WHEN_summarized_THEN_every_status_is_zero_and_there_is_no_lag() {
        OutboxSummaryResponse summary = service.summary();

        assertThat(summary.counts()).containsEntry("PENDING", 0L).containsEntry("FAILED", 0L);
        assertThat(summary.lagCount()).isZero();
        assertThat(summary.lagAgeSeconds()).isZero();
    }

    @Test
    void GIVEN_pending_rows_WHEN_summarized_THEN_the_lag_reflects_the_oldest_one() {
        insert("order-1", 1, "{}");   // created at NOW (clock is fixed)

        OutboxSummaryResponse summary = service.summary();

        assertThat(summary.counts()).containsEntry("PENDING", 1L);
        assertThat(summary.lagCount()).isEqualTo(1L);
        assertThat(summary.lagAgeSeconds()).isZero();   // fixed clock: no time has passed
    }

    @Test
    void GIVEN_no_selector_WHEN_searched_THEN_every_row_comes_back_as_a_list_view_with_no_payload() {
        insert("order-1", 1, "{\"secret\":1}");

        OutboxEntryPageResponse page = service.search(OutboxSearchCriteria.builder().build());

        assertThat(page.items()).hasSize(1);
        assertThat(page.items().get(0).payload()).isNull();
        assertThat(page.items().get(0).headers()).isNull();
        assertThat(page.items().get(0).aggregateId()).isEqualTo("order-1");
    }

    @Test
    void GIVEN_a_full_page_WHEN_searched_THEN_the_next_cursor_is_the_last_rows_id() {
        insert("order-1", 1, "{}");
        insert("order-2", 1, "{}");

        OutboxEntryPageResponse page = service.search(OutboxSearchCriteria.builder().limit(1).build());

        assertThat(page.items()).hasSize(1);
        assertThat(page.nextCursor()).isEqualTo(String.valueOf(page.items().get(0).id()));
    }

    @Test
    void GIVEN_fewer_rows_than_the_limit_WHEN_searched_THEN_there_is_no_next_cursor() {
        insert("order-1", 1, "{}");

        OutboxEntryPageResponse page = service.search(OutboxSearchCriteria.builder().limit(50).build());

        assertThat(page.nextCursor()).isNull();
    }

    @Test
    void GIVEN_an_empty_result_WHEN_searched_THEN_there_is_no_next_cursor() {
        OutboxEntryPageResponse page = service.search(OutboxSearchCriteria.builder().build());

        assertThat(page.items()).isEmpty();
        assertThat(page.nextCursor()).isNull();
    }

    @Test
    void GIVEN_an_aggregate_filter_WHEN_searched_THEN_only_matching_rows_come_back() {
        insert("order-1", 1, "{}");
        insert("order-2", 1, "{}");

        OutboxEntryPageResponse page = service.search(
                OutboxSearchCriteria.builder().aggregateId(AggregateId.of("order-1")).build());

        assertThat(page.items()).extracting(OutboxEntryResponse::aggregateId).containsExactly("order-1");
    }

    @Test
    void GIVEN_an_existing_row_WHEN_found_by_id_THEN_the_detail_carries_the_parsed_payload_and_headers() {
        insert("order-1", 1, "{\"amount\":42}");
        long id = outbox.all().get(0).id();

        Optional<OutboxEntryResponse> detail = service.findById(id);

        assertThat(detail).isPresent();
        assertThat(detail.get().payload()).isInstanceOf(ObjectNode.class);
        assertThat(((ObjectNode) detail.get().payload()).get("amount").asInt()).isEqualTo(42);
        assertThat(detail.get().headers()).containsEntry("correlation-id", "abc");
    }

    @Test
    void GIVEN_a_non_json_payload_WHEN_found_by_id_THEN_it_falls_back_to_a_raw_string() {
        insert("order-1", 1, "not-json-at-all");
        long id = outbox.all().get(0).id();

        Optional<OutboxEntryResponse> detail = service.findById(id);

        assertThat(detail.get().payload()).isEqualTo("not-json-at-all");
    }

    @Test
    void GIVEN_a_missing_id_WHEN_found_by_id_THEN_it_is_empty() {
        assertThat(service.findById(999_999L)).isEmpty();
    }

    @Test
    void GIVEN_a_store_that_reports_no_lag_WHEN_summarized_THEN_it_falls_back_to_zero() {
        OutboxAdminService serviceOverNoLagStore = new OutboxAdminService(
                outbox, new NoLagOutboxStore(), outbox, outbox, new ObjectMapper(), Clock.fixed(NOW, ZoneOffset.UTC));

        OutboxSummaryResponse summary = serviceOverNoLagStore.summary();

        assertThat(summary.lagCount()).isZero();
        assertThat(summary.lagAgeSeconds()).isZero();
    }

    @Test
    void GIVEN_a_stale_pending_row_WHEN_summarized_THEN_the_lag_age_reflects_elapsed_time() {
        Clock start = Clock.fixed(NOW.minus(Duration.ofSeconds(30)), ZoneOffset.UTC);
        InMemoryOutbox lateOutbox = new InMemoryOutbox(256, 10, start);
        OutboxAdminService lateService = new OutboxAdminService(
                lateOutbox, lateOutbox, lateOutbox, lateOutbox, new ObjectMapper(), Clock.fixed(NOW, ZoneOffset.UTC));
        lateOutbox.insert(OutboxMessage.builder()
                .aggregateId("order-1").aggregateType("Order").seq(1).payload("{}".getBytes()).build());

        OutboxSummaryResponse summary = lateService.summary();

        assertThat(summary.lagAgeSeconds()).isEqualTo(30.0);
    }

    // --- replayMessage ---

    @Test
    void GIVEN_a_failed_row_WHEN_replayed_THEN_it_is_reset_to_pending() {
        insert("order-1", 1, "{}");
        long id = outbox.all().get(0).id();
        outbox.markFailed(id, "boom");

        OutboxEntryResponse replayed = service.replayMessage(id, "test-user");

        assertThat(replayed.status()).isEqualTo("PENDING");
    }

    @Test
    void GIVEN_a_pending_row_WHEN_replayed_THEN_it_is_refused_as_not_replayable() {
        insert("order-1", 1, "{}");   // still PENDING
        long id = outbox.all().get(0).id();

        assertThatThrownBy(() -> service.replayMessage(id, "test-user")).isInstanceOf(MessageNotReplayableException.class);
    }

    @Test
    void GIVEN_a_missing_id_WHEN_replayed_THEN_it_is_refused_as_not_found() {
        assertThatThrownBy(() -> service.replayMessage(999_999L, "test-user")).isInstanceOf(OutboxMessageNotFoundException.class);
    }

    // --- discardMessage ---

    @Test
    void GIVEN_a_failed_row_WHEN_discarded_with_acknowledgement_THEN_it_is_discarded_with_the_reason_recorded() {
        insert("order-1", 1, "{}");
        long id = outbox.all().get(0).id();
        outbox.markFailed(id, "boom");

        OutboxEntryResponse discarded = service.discardMessage(id, new DiscardRequest(true, "no longer needed"), "test-user");

        assertThat(discarded.status()).isEqualTo("DISCARDED");
        assertThat(discarded.discardReason()).isEqualTo("no longer needed");
        assertThat(discarded.lastError()).isEqualTo("boom");
    }

    @Test
    void GIVEN_the_ordering_break_is_not_acknowledged_WHEN_discarded_THEN_it_is_refused() {
        insert("order-1", 1, "{}");
        long id = outbox.all().get(0).id();
        outbox.markFailed(id, "boom");

        assertThatThrownBy(() -> service.discardMessage(id, new DiscardRequest(false, null), "test-user"))
                .isInstanceOf(OrderingBreakNotAcknowledgedException.class);
    }

    @Test
    void GIVEN_a_pending_row_WHEN_discarded_THEN_it_is_refused_as_not_discardable() {
        insert("order-1", 1, "{}");   // still PENDING
        long id = outbox.all().get(0).id();

        assertThatThrownBy(() -> service.discardMessage(id, new DiscardRequest(true, "irrelevant"), "test-user"))
                .isInstanceOf(MessageNotDiscardableException.class);
    }

    @Test
    void GIVEN_a_missing_id_WHEN_discarded_THEN_it_is_refused_as_not_found() {
        assertThatThrownBy(() -> service.discardMessage(999_999L, new DiscardRequest(true, "irrelevant"), "test-user"))
                .isInstanceOf(OutboxMessageNotFoundException.class);
    }

    // --- replayBulk ---

    @Test
    void GIVEN_a_selector_less_request_WHEN_replayed_in_bulk_THEN_it_is_refused() {
        assertThatThrownBy(() -> service.replayBulk(new ReplayRequest(null, null, null, null, null, false), "test-user"))
                .isInstanceOf(ReplayNoSelectorException.class);
    }

    @Test
    void GIVEN_failed_rows_for_an_aggregate_WHEN_replayed_in_bulk_THEN_they_are_reset_and_the_counts_reported() {
        insert("order-1", 1, "{}");
        long id = outbox.all().get(0).id();
        outbox.markFailed(id, "boom");

        ReplayResultResponse result = service.replayBulk(new ReplayRequest("order-1", null, null, null, null, false), "test-user");

        assertThat(result.matched()).isEqualTo(1);
        assertThat(result.replayed()).isEqualTo(1);
        assertThat(result.dryRun()).isFalse();
        assertThat(outbox.byId(id).status().name()).isEqualTo("PENDING");
    }

    @Test
    void GIVEN_a_dry_run_bulk_replay_WHEN_requested_THEN_nothing_changes_but_the_match_count_is_reported() {
        insert("order-1", 1, "{}");
        long id = outbox.all().get(0).id();
        outbox.markFailed(id, "boom");

        ReplayResultResponse result = service.replayBulk(new ReplayRequest("order-1", null, null, null, null, true), "test-user");

        assertThat(result.matched()).isEqualTo(1);
        assertThat(result.replayed()).isZero();
        assertThat(result.dryRun()).isTrue();
        assertThat(outbox.byId(id).status().name()).isEqualTo("FAILED");
    }

    @Test
    void GIVEN_a_status_selector_WHEN_replayed_in_bulk_THEN_only_that_status_resets() {
        insert("order-1", 1, "{}");
        long id = outbox.all().get(0).id();
        outbox.markFailed(id, "boom");

        ReplayResultResponse result =
                service.replayBulk(new ReplayRequest(null, "Order", null, null, List.of("FAILED"), false), "test-user");

        assertThat(result.matched()).isEqualTo(1);
        assertThat(outbox.byId(id).status().name()).isEqualTo("PENDING");
    }

    @Test
    void GIVEN_an_invalid_status_name_WHEN_replayed_in_bulk_THEN_it_is_rejected() {
        assertThatThrownBy(() -> service.replayBulk(new ReplayRequest("order-1", null, null, null, List.of("NOT_A_STATUS"), false), "test-user"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void GIVEN_only_an_id_range_WHEN_replayed_in_bulk_THEN_it_is_accepted_as_a_selector() {
        insert("order-1", 1, "{}");
        long id = outbox.all().get(0).id();
        outbox.markFailed(id, "boom");

        ReplayResultResponse result = service.replayBulk(new ReplayRequest(null, null, id, id, null, false), "test-user");

        assertThat(result.matched()).isEqualTo(1);
        assertThat(outbox.byId(id).status().name()).isEqualTo("PENDING");
    }

    @Test
    void GIVEN_only_a_to_id_upper_bound_WHEN_replayed_in_bulk_THEN_it_is_accepted_as_a_selector() {
        insert("order-1", 1, "{}");
        long id = outbox.all().get(0).id();
        outbox.markFailed(id, "boom");

        ReplayResultResponse result = service.replayBulk(new ReplayRequest(null, null, null, id, null, false), "test-user");

        assertThat(result.matched()).isEqualTo(1);
        assertThat(outbox.byId(id).status().name()).isEqualTo("PENDING");
    }

    @Test
    void GIVEN_only_statuses_and_no_other_selector_WHEN_replayed_in_bulk_THEN_it_is_accepted_as_a_selector() {
        insert("order-1", 1, "{}");
        long id = outbox.all().get(0).id();
        outbox.markFailed(id, "boom");

        ReplayResultResponse result = service.replayBulk(new ReplayRequest(null, null, null, null, List.of("FAILED"), false), "test-user");

        assertThat(result.matched()).isEqualTo(1);
        assertThat(outbox.byId(id).status().name()).isEqualTo("PENDING");
    }

    @Test
    void GIVEN_an_empty_statuses_list_WHEN_replayed_in_bulk_THEN_it_is_treated_the_same_as_omitted() {
        insert("order-1", 1, "{}");
        long id = outbox.all().get(0).id();
        outbox.markDone(id);

        // Empty statuses (as opposed to omitted/null) must default the same way: every replayable
        // status for the given aggregate, not "no statuses eligible".
        ReplayResultResponse result = service.replayBulk(new ReplayRequest("order-1", null, null, null, List.of(), false), "test-user");

        assertThat(result.matched()).isEqualTo(1);
        assertThat(outbox.byId(id).status().name()).isEqualTo("PENDING");
    }

    /** A store that never overrides {@link OutboxStore#lag()} — exercises the summary's fallback path. */
    private static final class NoLagOutboxStore implements OutboxStore {
        @Override
        public List<OutboxRecord> claimBatch(Set<Integer> buckets, String workerId, Duration lease, int batchSize) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void markDone(long id) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void markForRetry(long id, String error, Duration retryDelay) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void markFailed(long id, String error) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int reclaimExpiredLeases() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int cleanup(Instant doneBefore, int batchSize) {
            throw new UnsupportedOperationException();
        }
    }
}
