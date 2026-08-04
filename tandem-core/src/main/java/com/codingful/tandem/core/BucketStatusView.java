package com.codingful.tandem.core;

import java.time.Instant;

/**
 * One virtual bucket's admin-observable state, behind
 * {@link com.codingful.tandem.core.port.RelayQuery#buckets} and
 * {@link com.codingful.tandem.core.port.RelayQuery#bucket} (HLD-admin-api §4.1). {@link RelayCoordinationMode#LEASE}
 * only — {@code tandem_bucket_lease} carries no meaning under {@code SINGLE}.
 *
 * @param bucket       the virtual bucket number, {@code [0, bucketCount)}
 * @param owner        the instance id holding the lease, or {@code null} if unowned
 * @param leaseUntil   the lease's expiry, or {@code null} if unowned
 * @param covered      whether the bucket currently has a live (non-expired) owner
 * @param paused       whether this bucket is deliberately paused ({@code tandem_bucket_lease.paused}) —
 *                     the owning worker keeps renewing the lease while paused, so {@code covered} can
 *                     be {@code true} even when {@code paused} is {@code true}
 * @param pendingCount {@code PENDING} rows currently in this bucket
 * @param lagAgeSeconds age of the oldest {@code PENDING} row in this bucket, or {@code null} if none
 */
public record BucketStatusView(int bucket, String owner, Instant leaseUntil, boolean covered, boolean paused,
        long pendingCount, Double lagAgeSeconds) {
}
