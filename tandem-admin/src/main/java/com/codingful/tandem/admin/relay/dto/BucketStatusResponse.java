package com.codingful.tandem.admin.relay.dto;

import com.codingful.tandem.core.BucketStatusView;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.Instant;

/**
 * Wire representation of the OpenAPI {@code BucketStatus} schema. {@code @JsonFormat(shape = STRING)}
 * holds the contract's {@code date-time} rendering whatever the host application's mapper does with
 * {@code WRITE_DATES_AS_TIMESTAMPS} — see {@code OutboxEntryResponse}.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record BucketStatusResponse(int bucket, String owner,
        @JsonFormat(shape = JsonFormat.Shape.STRING) Instant leaseUntil, boolean covered, boolean paused,
        long pendingCount, Double lagAgeSeconds) {

    public static BucketStatusResponse from(BucketStatusView view) {
        return new BucketStatusResponse(view.bucket(), view.owner(), view.leaseUntil(), view.covered(),
                view.paused(), view.pendingCount(), view.lagAgeSeconds());
    }
}
