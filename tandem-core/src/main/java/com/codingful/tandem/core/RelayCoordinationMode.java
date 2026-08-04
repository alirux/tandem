package com.codingful.tandem.core;

/**
 * The relay's bucket-coordination strategy, as the Admin API can observe it (HLD-admin-api §4.1).
 * Deliberately independent of {@code tandem-jdbc}'s own {@code Coordination} enum, which is the
 * relay engine's internal configuration type — this is the admin read model's own vocabulary,
 * mapped from the value the relay publishes to {@code tandem_meta} at startup, not a shared type.
 */
public enum RelayCoordinationMode {

    /** No relay of this Tandem version has recorded a mode in this database — treated as {@code SINGLE}. */
    SINGLE,

    /** Bucket ownership is partitioned via {@code tandem_bucket_lease}; per-bucket admin operations work. */
    LEASE
}
