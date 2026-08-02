package com.codingful.tandem.admin;

import com.codingful.tandem.core.OutboxRowDetail;
import com.codingful.tandem.core.OutboxRowView;
import com.codingful.tandem.core.OutboxSearchCriteria;
import com.codingful.tandem.core.OutboxStatus;
import com.codingful.tandem.core.port.OutboxQuery;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** A stand-in {@link OutboxQuery} used only to prove the autoconfiguration backs off when one already exists. */
final class NoopOutboxQuery implements OutboxQuery {

    @Override
    public Map<OutboxStatus, Long> statusCounts() {
        return Map.of();
    }

    @Override
    public List<OutboxRowView> search(OutboxSearchCriteria criteria) {
        return List.of();
    }

    @Override
    public Optional<OutboxRowDetail> findById(long id) {
        return Optional.empty();
    }
}
