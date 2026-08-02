package com.codingful.tandem.core.port;

import com.codingful.tandem.core.OutboxRowDetail;
import com.codingful.tandem.core.OutboxRowView;
import com.codingful.tandem.core.OutboxSearchCriteria;
import com.codingful.tandem.core.OutboxStatus;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Read-side port for the Admin API (HLD-admin-api §4): counts, search, and single-row lookup over
 * {@code tandem_outbox}. Distinct from {@link OutboxRepository} (write-only) and {@link OutboxStore}
 * (relay-shaped — bucket-scoped and mutating) — neither exposes a plain read. Implemented by
 * {@code tandem-jdbc} and by {@code InMemoryOutbox}, so admin use cases are unit-testable without a
 * database.
 */
public interface OutboxQuery {

    /**
     * Row count per status.
     *
     * @return a map naming every {@link OutboxStatus}, with {@code 0} for any status currently
     *         absent — never a partial map
     */
    Map<OutboxStatus, Long> statusCounts();

    /**
     * Search rows matching the given criteria.
     *
     * @param criteria the filters and the pagination cursor/limit
     * @return up to {@code criteria.limit()} rows, in ascending id order — pass the last row's id as
     *         the next search's {@code afterId} to page forward
     */
    List<OutboxRowView> search(OutboxSearchCriteria criteria);

    /**
     * Look up one row with its full detail.
     *
     * @param id the row id
     * @return the row, or empty if no row with that id exists
     */
    Optional<OutboxRowDetail> findById(long id);
}
