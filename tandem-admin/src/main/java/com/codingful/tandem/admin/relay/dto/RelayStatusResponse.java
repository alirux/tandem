package com.codingful.tandem.admin.relay.dto;

import com.codingful.tandem.core.RelayStatusView;

/**
 * Wire representation of the OpenAPI {@code RelayStatus} schema — deliberately independent of
 * {@code tandem-jdbc}'s own in-process {@code RelayStatus} (HLD-admin-api §4.1): {@code state} here is
 * the Admin API's own reading ({@code RUNNING}/{@code PAUSED}/{@code DOWN}), not a worker-thread
 * liveness snapshot. {@code DOWN} takes priority over {@code PAUSED} - "is anything running at all" is
 * the more important fact once no instance has heartbeated recently.
 */
public record RelayStatusResponse(String state, int bucketCount, int uncoveredBuckets, int workers) {

    public static RelayStatusResponse from(RelayStatusView view) {
        String state = !view.alive() ? "DOWN" : (view.paused() ? "PAUSED" : "RUNNING");
        return new RelayStatusResponse(state, view.bucketCount(), view.uncoveredBuckets(), view.workers());
    }
}
