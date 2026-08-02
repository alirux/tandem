package com.codingful.tandem.admin.outbox;

import com.codingful.tandem.core.OutboxRowDetail;
import com.codingful.tandem.core.OutboxRowView;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;

/**
 * Wire representation of the OpenAPI {@code OutboxEntry} schema — one shape for both the search
 * result (list view, {@code headers}/{@code payload} absent) and the single-message detail, exactly
 * as the contract specifies them. Deliberately independent of {@code tandem-core}'s types
 * (HLD-admin-api §4): {@code status} is rendered as its name, not the core enum, and {@code payload}
 * is rendered as parsed JSON (or, if not valid JSON, as a raw string) rather than as bytes.
 *
 * <p>{@code @JsonInclude(NON_NULL)}: {@code payload}'s schema is {@code oneOf: [object, string]},
 * which — an OpenAPI 3.0 nullable/oneOf limitation — does not accept an explicit JSON {@code null}
 * even though the field itself is {@code nullable}. Omitting the key entirely is what the contract's
 * own description already says ("omitted in list view") and is what conformance testing requires
 * (caught by {@code openapi-request-validator}, HLD-admin-api §5). Applied to every optional field
 * uniformly rather than singling out {@code payload}, since every one of them is schema-valid either
 * way (all are optional, non-{@code oneOf} types) and a smaller body is the more honest rendering of
 * "this row has no value here".
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
record OutboxEntryResponse(
        long id,
        String aggregateId,
        String aggregateType,
        String type,
        long seq,
        String status,
        int attempts,
        String lastError,
        Instant nextAttemptAt,
        String lockedBy,
        Instant lockedUntil,
        Instant createdAt,
        Map<String, String> headers,
        Object payload) {

    /** From a list-view row — {@code headers}/{@code payload} are {@code null} (HLD-admin-api §4). */
    static OutboxEntryResponse fromView(OutboxRowView view) {
        return new OutboxEntryResponse(
                view.id(),
                view.aggregateId().value(),
                view.aggregateType(),
                view.type(),
                view.seq(),
                view.status().name(),
                view.attempts(),
                view.lastError(),
                view.nextAttemptAt(),
                view.lockedBy(),
                view.lockedUntil(),
                view.createdAt(),
                null,
                null);
    }

    /** From a single-message detail row — carries {@code headers} and the rendered {@code payload}. */
    static OutboxEntryResponse fromDetail(OutboxRowDetail detail, ObjectMapper objectMapper) {
        return new OutboxEntryResponse(
                detail.id(),
                detail.aggregateId().value(),
                detail.aggregateType(),
                detail.type(),
                detail.seq(),
                detail.status().name(),
                detail.attempts(),
                detail.lastError(),
                detail.nextAttemptAt(),
                detail.lockedBy(),
                detail.lockedUntil(),
                detail.createdAt(),
                detail.headers(),
                renderPayload(detail.payload(), objectMapper));
    }

    /**
     * The stored payload is JSON (the default {@code JSONB} column), so it is rendered as a parsed
     * JSON value — matching the contract's {@code oneOf: [object, string]} — rather than as an
     * escaped string. Falls back to a raw string if a non-default serializer ever stores something
     * else there.
     */
    private static Object renderPayload(byte[] payload, ObjectMapper objectMapper) {
        try {
            return objectMapper.readTree(payload);
        } catch (IOException e) {
            return new String(payload, StandardCharsets.UTF_8);
        }
    }
}
