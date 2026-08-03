package com.codingful.tandem.admin.outbox;

import com.codingful.tandem.core.AggregateId;
import com.codingful.tandem.core.LagSnapshot;
import com.codingful.tandem.core.OutboxRowView;
import com.codingful.tandem.core.OutboxSearchCriteria;
import com.codingful.tandem.core.OutboxStatus;
import com.codingful.tandem.core.ReplayCriteria;
import com.codingful.tandem.core.ReplayResult;
import com.codingful.tandem.core.port.DiscardService;
import com.codingful.tandem.core.port.OutboxQuery;
import com.codingful.tandem.core.port.OutboxStore;
import com.codingful.tandem.core.port.ReplayService;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Clock;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Slice 1 (reads) and slice 2 (replay/discard) use cases for the Admin API (HLD-admin-api §4) —
 * framework-agnostic, so they are unit-testable without HTTP; {@link OutboxAdminController} is the
 * REST driving adapter over this class. Delegates to {@link OutboxQuery} for counts/search/detail, to
 * {@link OutboxStore#lag()} for the backlog reading {@link OutboxSummaryResponse} needs, to
 * {@link ReplayService} for replay (reused almost 1:1 from the existing per-aggregate replay), and to
 * {@link DiscardService} for discard (new in slice 2, IMPLEMENTATION-PLAN-admin-api.md §8).
 */
final class OutboxAdminService {

    private final OutboxQuery outboxQuery;
    private final OutboxStore outboxStore;
    private final ReplayService replayService;
    private final DiscardService discardService;
    private final ObjectMapper objectMapper;
    private final Clock clock;

    /**
     * @param outboxQuery     the read side (counts, search, single-row detail)
     * @param outboxStore     only {@link OutboxStore#lag()} is called, for the summary's backlog reading
     * @param replayService   resets a matching selection of {@code DONE}/{@code FAILED} rows to {@code PENDING}
     * @param discardService  transitions one {@code FAILED} row to {@code DISCARDED}
     * @param objectMapper    used to render a row's payload as parsed JSON rather than raw bytes
     * @param clock           the time {@link #summary()} measures backlog age against; inject a fixed
     *                        clock in tests for a deterministic reading
     */
    OutboxAdminService(OutboxQuery outboxQuery, OutboxStore outboxStore, ReplayService replayService,
            DiscardService discardService, ObjectMapper objectMapper, Clock clock) {
        this.outboxQuery = Objects.requireNonNull(outboxQuery, "outboxQuery");
        this.outboxStore = Objects.requireNonNull(outboxStore, "outboxStore");
        this.replayService = Objects.requireNonNull(replayService, "replayService");
        this.discardService = Objects.requireNonNull(discardService, "discardService");
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

    /**
     * {@code POST /outbox/messages/{id}/replay}: resets one {@code DONE}/{@code FAILED} row to
     * {@code PENDING}. {@code fromId = toId = id} both scopes the replay to this one row and
     * satisfies {@link ReplayCriteria}'s "at least one selector" invariant for free; no status
     * filter is needed since {@code ReplayService} already restricts to {@code DONE}/{@code FAILED}.
     */
    OutboxEntryResponse replayMessage(long id) {
        ReplayResult result = replayService.replay(new ReplayCriteria(null, null, id, id, Set.of(), false));
        if (result.replayed() == 0) {
            requireExists(id, () -> new MessageNotReplayableException(id));
        }
        return findById(id).orElseThrow(() -> new OutboxMessageNotFoundException(id));
    }

    /** {@code POST /outbox/messages/{id}/discard}: marks one {@code FAILED} row {@code DISCARDED}. */
    OutboxEntryResponse discardMessage(long id, DiscardRequest request) {
        if (!request.acknowledgeOrderingBreak()) {
            throw new OrderingBreakNotAcknowledgedException();
        }
        boolean discarded = discardService.discard(id, request.reason());
        if (!discarded) {
            requireExists(id, () -> new MessageNotDiscardableException(id));
        }
        return findById(id).orElseThrow(() -> new OutboxMessageNotFoundException(id));
    }

    /**
     * {@code POST /outbox/replay}: replays every {@code DONE}/{@code FAILED} row matching the given
     * criteria, or previews the match count when {@code dryRun} is set.
     */
    ReplayResultResponse replayBulk(ReplayRequest request) {
        Set<OutboxStatus> statuses = parseStatuses(request.statuses());
        boolean hasSelector = request.aggregateId() != null
                || request.aggregateType() != null
                || request.fromId() != null
                || request.toId() != null
                || !statuses.isEmpty();
        if (!hasSelector) {
            throw new ReplayNoSelectorException();
        }
        ReplayCriteria criteria = new ReplayCriteria(
                request.aggregateId() == null ? null : AggregateId.of(request.aggregateId()),
                request.aggregateType(),
                request.fromId(),
                request.toId(),
                statuses,
                request.dryRun());
        return ReplayResultResponse.from(replayService.replay(criteria));
    }

    /**
     * Called only on the failure path (zero rows affected) to tell a missing id (404) apart from an
     * id that exists but is in the wrong state (409) — the success path never pays for this read.
     */
    private void requireExists(long id, Supplier<RuntimeException> wrongStateException) {
        if (outboxQuery.findById(id).isEmpty()) {
            throw new OutboxMessageNotFoundException(id);
        }
        throw wrongStateException.get();
    }

    private static Set<OutboxStatus> parseStatuses(List<String> statuses) {
        if (statuses == null || statuses.isEmpty()) {
            return Set.of();
        }
        Set<OutboxStatus> parsed = new LinkedHashSet<>();
        for (String status : statuses) {
            try {
                parsed.add(OutboxStatus.valueOf(status));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid status: " + status, e);
            }
        }
        return parsed;
    }
}
