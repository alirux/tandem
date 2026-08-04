package com.codingful.tandem.admin.relay;

import com.codingful.tandem.core.RelayCoordinationMode;

/**
 * Thrown by any per-bucket/per-worker relay operation when the relay this database serves runs
 * {@link RelayCoordinationMode#SINGLE} — those operations need {@code LEASE} (HLD-admin-api §4.1).
 */
final class RelayCoordinationUnsupportedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    RelayCoordinationUnsupportedException(RelayCoordinationMode detected) {
        super("Per-bucket relay control requires LEASE coordination; this relay runs " + detected);
    }
}
