package com.codingful.tandem.core.port;

import com.codingful.tandem.core.OutboxMessage;
import java.util.Collection;

/**
 * Write-side port (LLD-core §2.1): insert outbox rows <b>within the caller's transaction</b>, atomically
 * with the business state change. Implemented by {@code tandem-jdbc} and by {@code InMemoryOutbox}.
 */
public interface OutboxRepository {

    /**
     * Insert one message within the caller's transaction.
     *
     * @param message the row to insert
     * @throws com.codingful.tandem.core.exception.OutboxInsertException if the insert fails
     */
    void insert(OutboxMessage message);

    /**
     * Insert several messages within the same transaction.
     *
     * @param messages the rows to insert, in order
     * @throws com.codingful.tandem.core.exception.OutboxInsertException if the insert fails
     */
    void insertAll(Collection<OutboxMessage> messages);
}
