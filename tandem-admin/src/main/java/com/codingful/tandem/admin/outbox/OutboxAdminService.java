package com.codingful.tandem.admin.outbox;

import com.codingful.tandem.core.LagSnapshot;
import com.codingful.tandem.core.OutboxRowView;
import com.codingful.tandem.core.OutboxSearchCriteria;
import com.codingful.tandem.core.OutboxStatus;
import com.codingful.tandem.core.port.OutboxQuery;
import com.codingful.tandem.core.port.OutboxStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Clock;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Slice-1 (reads) use cases for the Admin API (HLD-admin-api §4) — framework-agnostic, so they are
 * unit-testable without HTTP; {@link OutboxAdminController} is the REST driving adapter over this
 * class. Delegates to {@link OutboxQuery} for counts/search/detail and to {@link OutboxStore#lag()}
 * for the backlog reading {@link OutboxSummaryResponse} needs — the one genuine port reuse this slice
 * has (everything else on {@link OutboxStore} is relay-shaped and unused here).
 */
final class OutboxAdminService {

    private final OutboxQuery outboxQuery;
    private final OutboxStore outboxStore;
    private final ObjectMapper objectMapper;
    private final Clock clock;

    /**
     * @param outboxQuery  the read side (counts, search, single-row detail)
     * @param outboxStore  only {@link OutboxStore#lag()} is called, for the summary's backlog reading
     * @param objectMapper used to render a row's payload as parsed JSON rather than raw bytes
     * @param clock        the time {@link #summary()} measures backlog age against; inject a fixed
     *                     clock in tests for a deterministic reading
     */
    OutboxAdminService(OutboxQuery outboxQuery, OutboxStore outboxStore, ObjectMapper objectMapper, Clock clock) {
        this.outboxQuery = Objects.requireNonNull(outboxQuery, "outboxQuery");
        this.outboxStore = Objects.requireNonNull(outboxStore, "outboxStore");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /** {@code GET /outbox/summary}: counts per status plus the current backlog reading. */
    OutboxSummaryResponse summary() {
        Map<String, Long> counts = new LinkedHashMap<>();
        for (Map.Entry<OutboxStatus, Long> entry : outboxQuery.statusCounts().entrySet()) {
            counts.put(entry.getKey().name(), entry.getValue());
        }
        LagSnapshot lag = outboxStore.lag().orElseGet(() -> new LagSnapshot(0, Optional.empty()));
        return new OutboxSummaryResponse(counts, lag.pending(), lag.ageSecondsAt(Instant.now(clock)));
    }

    /**
     * {@code GET /outbox/messages}: search the outbox by the given criteria.
     *
     * <p>The next cursor is set whenever a full page came back — the standard cursor-pagination
     * heuristic. It costs one extra request that returns an empty final page when the outbox has
     * exactly a multiple of {@code limit} matching rows; the alternative (fetching {@code limit + 1}
     * to know for certain) is not worth the extra row for a first cut.
     */
    OutboxEntryPageResponse search(OutboxSearchCriteria criteria) {
        List<OutboxRowView> rows = outboxQuery.search(criteria);
        List<OutboxEntryResponse> items = rows.stream().map(OutboxEntryResponse::fromView).toList();
        // A full page implies at least one row, since criteria.limit() is always >= 1 (validated) —
        // rows.size() == criteria.limit() alone is enough to know rows is non-empty here.
        String nextCursor = rows.size() == criteria.limit()
                ? String.valueOf(rows.get(rows.size() - 1).id())
                : null;
        return new OutboxEntryPageResponse(items, nextCursor);
    }

    /** {@code GET /outbox/messages/{id}}: the full detail of one row, or empty if it does not exist. */
    Optional<OutboxEntryResponse> findById(long id) {
        return outboxQuery.findById(id).map(detail -> OutboxEntryResponse.fromDetail(detail, objectMapper));
    }
}
