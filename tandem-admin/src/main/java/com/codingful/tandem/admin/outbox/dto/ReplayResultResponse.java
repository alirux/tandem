package com.codingful.tandem.admin.outbox.dto;

import com.codingful.tandem.core.ReplayResult;

/** Wire representation of the OpenAPI {@code ReplayResult} schema ({@code POST /outbox/replay}). */
public record ReplayResultResponse(long matched, long replayed, boolean dryRun) {

    public static ReplayResultResponse from(ReplayResult result) {
        return new ReplayResultResponse(result.matched(), result.replayed(), result.dryRun());
    }
}
