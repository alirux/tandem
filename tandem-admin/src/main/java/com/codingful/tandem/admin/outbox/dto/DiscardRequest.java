package com.codingful.tandem.admin.outbox.dto;

/** Wire representation of the OpenAPI {@code DiscardRequest} schema ({@code POST /outbox/messages/{id}/discard}). */
public record DiscardRequest(boolean acknowledgeOrderingBreak, String reason) {
}
