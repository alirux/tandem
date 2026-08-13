package com.codingful.tandem.admin.relay.dto;

import com.codingful.tandem.core.WorkerView;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.Instant;

/**
 * Wire representation of the OpenAPI {@code WorkerInfo} schema. Named {@code workerId} by the
 * contract, but the database can only observe relay <b>instances</b> — see {@link WorkerView}'s javadoc.
 * {@code @JsonFormat(shape = STRING)} holds the contract's {@code date-time} rendering whatever the
 * host application's mapper does with {@code WRITE_DATES_AS_TIMESTAMPS}.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record WorkerInfoResponse(String workerId,
        @JsonFormat(shape = JsonFormat.Shape.STRING) Instant lastHeartbeat, int bucketCount) {

    public static WorkerInfoResponse from(WorkerView view) {
        return new WorkerInfoResponse(view.instanceId(), view.lastHeartbeat(), view.bucketCount());
    }
}
