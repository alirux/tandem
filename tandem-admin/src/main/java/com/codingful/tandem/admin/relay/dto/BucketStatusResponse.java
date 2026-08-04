package com.codingful.tandem.admin.relay.dto;

import com.codingful.tandem.core.BucketStatusView;
import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.Instant;

/** Wire representation of the OpenAPI {@code BucketStatus} schema. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record BucketStatusResponse(int bucket, String owner, Instant leaseUntil, boolean covered, boolean paused,
        long pendingCount, Double lagAgeSeconds) {

    public static BucketStatusResponse from(BucketStatusView view) {
        return new BucketStatusResponse(view.bucket(), view.owner(), view.leaseUntil(), view.covered(),
                view.paused(), view.pendingCount(), view.lagAgeSeconds());
    }
}
