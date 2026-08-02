package com.codingful.tandem.admin.outbox;

import com.codingful.tandem.core.AggregateId;
import com.codingful.tandem.core.OutboxSearchCriteria;
import com.codingful.tandem.core.OutboxStatus;
import java.time.Instant;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST driving adapter for the outbox read endpoints (slice 1, HLD-admin-api §2/§4), realising
 * {@code admin-api.openapi.yaml}'s {@code Outbox} tag reads. Delegates every use case to
 * {@link OutboxAdminService}; this class only binds HTTP onto it. The base path is configurable
 * (default {@code /tandem/admin}, HLD §3) with the fixed {@code /v1} contract version.
 */
@RestController
@RequestMapping("${tandem.admin.base-path:/tandem/admin}/v1/outbox")
class OutboxAdminController {

    private final OutboxAdminService service;

    OutboxAdminController(OutboxAdminService service) {
        this.service = service;
    }

    @GetMapping("/summary")
    OutboxSummaryResponse summary() {
        return service.summary();
    }

    @GetMapping("/messages")
    OutboxEntryPageResponse search(
            @RequestParam(name = "status", required = false) String status,
            @RequestParam(name = "aggregateId", required = false) String aggregateId,
            @RequestParam(name = "aggregateType", required = false) String aggregateType,
            @RequestParam(name = "type", required = false) String type,
            @RequestParam(name = "createdFrom", required = false)
                    @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant createdFrom,
            @RequestParam(name = "createdTo", required = false)
                    @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant createdTo,
            @RequestParam(name = "limit", defaultValue = "" + OutboxSearchCriteria.DEFAULT_LIMIT) int limit,
            @RequestParam(name = "cursor", required = false) String cursor) {

        OutboxSearchCriteria criteria = OutboxSearchCriteria.builder()
                .status(status == null ? null : parseStatus(status))
                .aggregateId(aggregateId == null ? null : AggregateId.of(aggregateId))
                .aggregateType(aggregateType)
                .type(type)
                .createdFrom(createdFrom)
                .createdTo(createdTo)
                .afterId(cursor == null ? null : parseCursor(cursor))
                .limit(limit)
                .build();
        return service.search(criteria);
    }

    @GetMapping("/messages/{id}")
    OutboxEntryResponse getById(@PathVariable("id") long id) {
        return service.findById(id).orElseThrow(() -> new OutboxMessageNotFoundException(id));
    }

    private static OutboxStatus parseStatus(String status) {
        try {
            return OutboxStatus.valueOf(status);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid status: " + status, e);
        }
    }

    private static long parseCursor(String cursor) {
        try {
            return Long.parseLong(cursor);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid cursor: " + cursor, e);
        }
    }
}
