package com.codingful.tandem.admin.relay.dto;

import com.codingful.tandem.core.RelayStatusView;

/**
 * Wire representation of the OpenAPI {@code RelayStatus} schema — deliberately independent of
 * {@code tandem-jdbc}'s own in-process {@code RelayStatus} (HLD-admin-api §4.1): {@code state} here is
 * the Admin API's desired-state reading ({@code RUNNING}/{@code PAUSED}), not a worker-thread liveness
 * snapshot.
 */
public record RelayStatusResponse(String state, int bucketCount, int uncoveredBuckets, int workers) {

    public static RelayStatusResponse from(RelayStatusView view) {
        return new RelayStatusResponse(view.paused() ? "PAUSED" : "RUNNING",
                view.bucketCount(), view.uncoveredBuckets(), view.workers());
    }
}
