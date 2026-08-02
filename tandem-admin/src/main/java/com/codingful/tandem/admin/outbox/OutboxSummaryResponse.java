package com.codingful.tandem.admin.outbox;

import java.util.Map;

/**
 * Wire representation of {@code GET /outbox/summary} (the OpenAPI {@code OutboxSummary} schema).
 * Deliberately independent of {@code tandem-core}'s types: {@code counts} keys are plain status
 * names, not the core {@code OutboxStatus} enum, so an additive change to that enum never silently
 * changes this contract (HLD-admin-api §4).
 */
record OutboxSummaryResponse(Map<String, Long> counts, long lagCount, double lagAgeSeconds) {
}
