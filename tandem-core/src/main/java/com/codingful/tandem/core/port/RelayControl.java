package com.codingful.tandem.core.port;

import com.codingful.tandem.core.RelayCoordinationMode;

/**
 * The Admin API's write side over relay-control state (HLD-admin-api §4.1): pausing/resuming the
 * whole relay or one bucket, and force-releasing a bucket's lease. Implemented by {@code tandem-jdbc}'s
 * {@code JdbcRelayControl}. Per-bucket operations are meaningful only under
 * {@link RelayCoordinationMode#LEASE} — callers must check {@link RelayQuery#coordinationMode()} first.
 */
public interface RelayControl {

    /** Sets the whole-relay desired state to paused ({@code tandem_meta.relay_paused}). */
    void pauseAll();

    /** Sets the whole-relay desired state to running. */
    void resumeAll();

    /**
     * @param bucket the virtual bucket number
     * @return {@code true} if the bucket was found and paused; {@code false} if outside {@code [0, bucketCount)}
     */
    boolean pauseBucket(int bucket);

    /**
     * @param bucket the virtual bucket number
     * @return {@code true} if the bucket was found and resumed; {@code false} if outside {@code [0, bucketCount)}
     */
    boolean resumeBucket(int bucket);

    /**
     * Clears the bucket's ownership lease so another worker can claim it on its next heartbeat.
     *
     * @param bucket the virtual bucket number
     * @return {@code true} if the bucket was found and released; {@code false} if outside {@code [0, bucketCount)}
     */
    boolean releaseBucket(int bucket);
}
