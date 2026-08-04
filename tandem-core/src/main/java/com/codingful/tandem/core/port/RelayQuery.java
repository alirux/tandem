package com.codingful.tandem.core.port;

import com.codingful.tandem.core.BucketStatusView;
import com.codingful.tandem.core.RelayCoordinationMode;
import com.codingful.tandem.core.RelayStatusView;
import com.codingful.tandem.core.WorkerView;
import java.util.List;
import java.util.Optional;

/**
 * The Admin API's read side over relay-control state (HLD-admin-api §4.1): coordination mode, whole-relay
 * status, and per-bucket/per-worker observability. Implemented by {@code tandem-jdbc}'s
 * {@code JdbcRelayQuery}; kept off {@link OutboxStore} for the same reason {@link OutboxQuery} is — these
 * are admin-only reads, never part of the relay's own claim/mark/cleanup contract.
 *
 * <p>{@link #buckets} and {@link #workers} are meaningful only under {@link RelayCoordinationMode#LEASE}
 * — callers must check {@link #coordinationMode()} first; this port does not itself guard against
 * calling them under {@code SINGLE} (that policy belongs to the use case, not the read).
 */
public interface RelayQuery {

    /**
     * @return the coordination mode the relay last recorded at startup, or {@link RelayCoordinationMode#SINGLE}
     *         if no relay of this version has ever recorded one in this database
     */
    RelayCoordinationMode coordinationMode();

    /**
     * @return the whole-relay reading: whether it is paused, its configured bucket count, and — under
     *         {@link RelayCoordinationMode#SINGLE} — {@code 0} for {@code uncoveredBuckets}/{@code workers}
     */
    RelayStatusView status();

    /**
     * @param uncoveredOnly when {@code true}, return only buckets with {@code PENDING} rows but no live owner
     * @return every virtual bucket's status, ordered by bucket number
     */
    List<BucketStatusView> buckets(boolean uncoveredOnly);

    /**
     * @param bucket the virtual bucket number
     * @return the bucket's status, or empty if {@code bucket} is outside {@code [0, bucketCount)}
     */
    Optional<BucketStatusView> bucket(int bucket);

    /** @return every live relay instance, ordered by instance id */
    List<WorkerView> workers();
}
