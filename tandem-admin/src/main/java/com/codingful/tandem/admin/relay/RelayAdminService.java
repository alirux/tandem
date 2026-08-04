package com.codingful.tandem.admin.relay;

import com.codingful.tandem.admin.relay.dto.BucketStatusResponse;
import com.codingful.tandem.admin.relay.dto.RelayStatusResponse;
import com.codingful.tandem.admin.relay.dto.WorkerInfoResponse;
import com.codingful.tandem.core.RelayCoordinationMode;
import com.codingful.tandem.core.port.RelayControl;
import com.codingful.tandem.core.port.RelayQuery;
import java.util.List;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Relay-control use cases for the Admin API (HLD-admin-api §4.1) — framework-agnostic, so they are
 * unit-testable without HTTP; {@link RelayAdminController} is the REST driving adapter over this class.
 *
 * <p>Every per-bucket/per-worker operation checks {@link RelayQuery#coordinationMode()} first and
 * refuses with {@link RelayCoordinationUnsupportedException} under {@link RelayCoordinationMode#SINGLE}
 * — {@code tandem_bucket_lease}/{@code tandem_relay_member} carry no meaning there, and querying them
 * anyway would either fail (absent tables) or render a healthy relay as a total coverage stall
 * (present-but-unused tables), the exact failure mode this check exists to prevent.
 *
 * <p>Every write operation (pause/resume/release) logs at INFO once it succeeds — same audit-trail
 * reasoning as {@code OutboxAdminService}; {@code actor} is the caller's identity when the host
 * authenticates requests, or {@code null} otherwise, extracted by {@link RelayAdminController}.
 */
final class RelayAdminService {

    private static final Logger LOG = LoggerFactory.getLogger(RelayAdminService.class);

    private final RelayQuery relayQuery;
    private final RelayControl relayControl;

    RelayAdminService(RelayQuery relayQuery, RelayControl relayControl) {
        this.relayQuery = Objects.requireNonNull(relayQuery, "relayQuery");
        this.relayControl = Objects.requireNonNull(relayControl, "relayControl");
    }

    /** {@code GET /relay/status}: works under either coordination mode (HLD-admin-api §4.1). */
    RelayStatusResponse status() {
        return RelayStatusResponse.from(relayQuery.status());
    }

    /** {@code POST /relay/pause}: {@code bucket == null} pauses the whole relay, any coordination mode. */
    RelayStatusResponse pause(Integer bucket, String actor) {
        if (bucket == null) {
            relayControl.pauseAll();
        } else {
            requireLeaseCoordination();
            if (!relayControl.pauseBucket(bucket)) {
                throw new RelayBucketNotFoundException(bucket);
            }
        }
        LOG.info("Relay pause applied bucket:{}, actor:{}", bucket == null ? "ALL" : bucket, actor);
        return status();
    }

    /** {@code POST /relay/resume}: {@code bucket == null} resumes the whole relay, any coordination mode. */
    RelayStatusResponse resume(Integer bucket, String actor) {
        if (bucket == null) {
            relayControl.resumeAll();
        } else {
            requireLeaseCoordination();
            if (!relayControl.resumeBucket(bucket)) {
                throw new RelayBucketNotFoundException(bucket);
            }
        }
        LOG.info("Relay resume applied bucket:{}, actor:{}", bucket == null ? "ALL" : bucket, actor);
        return status();
    }

    /** {@code GET /relay/buckets}: {@code LEASE} only. */
    List<BucketStatusResponse> buckets(boolean uncoveredOnly) {
        requireLeaseCoordination();
        return relayQuery.buckets(uncoveredOnly).stream().map(BucketStatusResponse::from).toList();
    }

    /** {@code GET /relay/buckets/{bucket}}: {@code LEASE} only. */
    BucketStatusResponse bucket(int bucket) {
        requireLeaseCoordination();
        return relayQuery.bucket(bucket).map(BucketStatusResponse::from)
                .orElseThrow(() -> new RelayBucketNotFoundException(bucket));
    }

    /** {@code POST /relay/buckets/{bucket}/release}: {@code LEASE} only. */
    BucketStatusResponse releaseBucket(int bucket, String actor) {
        requireLeaseCoordination();
        if (!relayControl.releaseBucket(bucket)) {
            throw new RelayBucketNotFoundException(bucket);
        }
        LOG.info("Relay bucket released bucket:{}, actor:{}", bucket, actor);
        return relayQuery.bucket(bucket).map(BucketStatusResponse::from)
                .orElseThrow(() -> new RelayBucketNotFoundException(bucket));
    }

    /** {@code GET /relay/workers}: {@code LEASE} only. */
    List<WorkerInfoResponse> workers() {
        requireLeaseCoordination();
        return relayQuery.workers().stream().map(WorkerInfoResponse::from).toList();
    }

    private void requireLeaseCoordination() {
        RelayCoordinationMode mode = relayQuery.coordinationMode();
        if (mode != RelayCoordinationMode.LEASE) {
            throw new RelayCoordinationUnsupportedException(mode);
        }
    }
}
