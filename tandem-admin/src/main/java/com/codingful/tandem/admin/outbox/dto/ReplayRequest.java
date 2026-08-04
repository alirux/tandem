package com.codingful.tandem.admin.outbox.dto;

import java.util.List;

/**
 * Wire representation of the OpenAPI {@code ReplayRequest} schema ({@code POST /outbox/replay}).
 * Deliberately independent of {@code tandem-core}'s {@link com.codingful.tandem.core.ReplayCriteria}
 * (HLD-admin-api §4): {@code statuses} is rendered/read as status names, not the core enum, and the
 * "at least one selector" invariant is enforced by the outbox use case, not by this record.
 */
public record ReplayRequest(
        String aggregateId,
        String aggregateType,
        Long fromId,
        Long toId,
        List<String> statuses,
        boolean dryRun) {
}
