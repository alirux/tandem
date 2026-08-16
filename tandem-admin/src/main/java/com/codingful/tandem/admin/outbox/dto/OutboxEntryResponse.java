package com.codingful.tandem.admin.outbox.dto;

import com.codingful.tandem.core.OutboxRowDetail;
import com.codingful.tandem.core.OutboxRowView;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonRawValue;
import java.time.Instant;
import java.util.Map;

/**
 * Wire representation of the OpenAPI {@code OutboxEntry} schema — one shape for both the search
 * result (list view, {@code headers}/{@code payload} absent) and the single-message detail, exactly
 * as the contract specifies them. Deliberately independent of {@code tandem-core}'s types
 * (HLD-admin-api §4): {@code status} is rendered as its name, not the core enum, and {@code payload}
 * is rendered as JSON (or, if the stored bytes are not JSON, as a string) rather than as bytes.
 *
 * <p><b>No JSON databind type appears here, by design.</b> Spring Boot 3 carries Jackson 2 and
 * Spring Boot 4 carries Jackson 3, which share only their annotations — so {@code payload} is a JSON
 * text fragment ({@link PayloadJson}) written through {@code @JsonRawValue}, not a parsed tree, and
 * the DTO renders identically under either generation. Naming a databind type here is what
 * previously reached a {@code @Bean} signature and stopped the whole module from starting on Boot 4.
 *
 * <p>{@code @JsonFormat(shape = STRING)} on the timestamps for the same reason: it holds the
 * contract's {@code date-time} rendering whatever the host application's mapper is configured to do,
 * instead of depending on {@code WRITE_DATES_AS_TIMESTAMPS} being disabled somewhere the module no
 * longer controls.
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
public record OutboxEntryResponse(
        long id,
        String aggregateId,
        String aggregateType,
        String type,
        long seq,
        String status,
        int attempts,
        int replays,
        String lastError,
        String discardReason,
        @JsonFormat(shape = JsonFormat.Shape.STRING) Instant nextAttemptAt,
        String lockedBy,
        @JsonFormat(shape = JsonFormat.Shape.STRING) Instant lockedUntil,
        @JsonFormat(shape = JsonFormat.Shape.STRING) Instant createdAt,
        String correlationId,
        Map<String, String> headers,
        @JsonRawValue String payload) {

    /** From a list-view row — {@code headers}/{@code payload} are {@code null} (HLD-admin-api §4). */
    public static OutboxEntryResponse fromView(OutboxRowView view) {
        return new OutboxEntryResponse(
                view.id(),
                view.aggregateId().value(),
                view.aggregateType(),
                view.type(),
                view.seq(),
                view.status().name(),
                view.attempts(),
                view.replays(),
                view.lastError(),
                view.discardReason(),
                view.nextAttemptAt(),
                view.lockedBy(),
                view.lockedUntil(),
                view.createdAt(),
                view.correlationId(),
                null,
                null);
    }

    /** From a single-message detail row — carries {@code headers} and the rendered {@code payload}. */
    public static OutboxEntryResponse fromDetail(OutboxRowDetail detail) {
        return new OutboxEntryResponse(
                detail.id(),
                detail.aggregateId().value(),
                detail.aggregateType(),
                detail.type(),
                detail.seq(),
                detail.status().name(),
                detail.attempts(),
                detail.replays(),
                detail.lastError(),
                detail.discardReason(),
                detail.nextAttemptAt(),
                detail.lockedBy(),
                detail.lockedUntil(),
                detail.createdAt(),
                detail.correlationId(),
                detail.headers(),
                PayloadJson.render(detail.payload()));
    }
}
