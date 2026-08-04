package com.codingful.tandem.core;

import java.time.Instant;

/**
 * One live relay instance, behind {@link com.codingful.tandem.core.port.RelayQuery#workers}
 * (HLD-admin-api §4.1), read from {@code tandem_relay_member}. {@link RelayCoordinationMode#LEASE}
 * only — {@code SINGLE} registers no presence row.
 *
 * <p>Named for the contract's {@code WorkerInfo} schema, but the granularity the database can actually
 * observe is the relay <b>instance</b>, not its individual worker threads — {@code tandem_relay_member}
 * has one row per instance, however many {@code workersPerInstance} it runs internally.
 *
 * @param instanceId    the relay instance's identity ({@code tandem_relay_member.owner})
 * @param lastHeartbeat this instance's last presence renewal
 * @param bucketCount   buckets this instance currently owns ({@code tandem_bucket_lease} rows where
 *                      {@code owner} matches, with a live lease)
 */
public record WorkerView(String instanceId, Instant lastHeartbeat, int bucketCount) {
}
