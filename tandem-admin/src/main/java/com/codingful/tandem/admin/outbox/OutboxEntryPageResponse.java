package com.codingful.tandem.admin.outbox;

import java.util.List;

/** Wire representation of the OpenAPI {@code OutboxEntryPage} schema — a page of results, not a page number. */
record OutboxEntryPageResponse(List<OutboxEntryResponse> items, String nextCursor) {
}
