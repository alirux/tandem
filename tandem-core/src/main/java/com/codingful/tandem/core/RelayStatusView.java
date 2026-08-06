package com.codingful.tandem.core;

/**
 * The relay's admin-observable state, behind {@link com.codingful.tandem.core.port.RelayQuery#status()}
 * (HLD-admin-api §4.1) — deliberately independent of {@code tandem-jdbc}'s own in-process
 * {@code RelayStatus} (a database-free liveness snapshot for the running instance itself); this is a
 * DB-derived reading, meaningful from a standalone admin that has never talked to the relay process.
 *
 * @param alive            whether any relay instance has heartbeated recently — true under both
 *                         {@link RelayCoordinationMode#SINGLE} and {@code LEASE}, since every instance
 *                         touches the same heartbeat regardless of mode; false with no relay ever
 *                         started at all
 * @param paused           the whole-relay desired state ({@code tandem_meta.relay_paused})
 * @param bucketCount      the fixed total virtual bucket count ({@code tandem_meta.bucket_count})
 * @param uncoveredBuckets buckets with {@code PENDING} rows but no live owner; always {@code 0} under
 *                         {@link RelayCoordinationMode#SINGLE}, which owns every bucket in-process
 * @param workers          live relay instances ({@code tandem_relay_member}); always {@code 0} under
 *                         {@link RelayCoordinationMode#SINGLE}, which registers no presence row — this
 *                         does not mean no relay is running (check {@link #alive} instead), only that
 *                         this per-instance reading cannot see it
 */
public record RelayStatusView(boolean alive, boolean paused, int bucketCount, int uncoveredBuckets, int workers) {
}
