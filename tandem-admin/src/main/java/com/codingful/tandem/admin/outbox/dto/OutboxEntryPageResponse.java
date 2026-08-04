package com.codingful.tandem.admin.outbox.dto;

import java.util.List;

/** Wire representation of the OpenAPI {@code OutboxEntryPage} schema — a page of results, not a page number. */
public record OutboxEntryPageResponse(List<OutboxEntryResponse> items, String nextCursor) {
}
