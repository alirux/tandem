package com.codingful.tandem.admin.outbox;

/** Wire representation of the OpenAPI {@code DiscardRequest} schema ({@code POST /outbox/messages/{id}/discard}). */
record DiscardRequest(boolean acknowledgeOrderingBreak, String reason) {
}
