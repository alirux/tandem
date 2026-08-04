package com.codingful.tandem.admin.relay.dto;

import com.codingful.tandem.core.WorkerView;
import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.Instant;

/**
 * Wire representation of the OpenAPI {@code WorkerInfo} schema. Named {@code workerId} by the
 * contract, but the database can only observe relay <b>instances</b> — see {@link WorkerView}'s javadoc.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record WorkerInfoResponse(String workerId, Instant lastHeartbeat, int bucketCount) {

    public static WorkerInfoResponse from(WorkerView view) {
        return new WorkerInfoResponse(view.instanceId(), view.lastHeartbeat(), view.bucketCount());
    }
}
