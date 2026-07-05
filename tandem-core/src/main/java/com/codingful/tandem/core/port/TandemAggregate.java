package com.codingful.tandem.core.port;

import com.codingful.tandem.core.OutboxMessage;
import java.util.Collection;

/**
 * Implemented by the client's aggregate to expose the outbox messages it produced (the annotation
 * tier, HLD §3.1, LLD-core §2.6).
 */
public interface TandemAggregate {

    Collection<OutboxMessage> pendingOutboxMessages();
}
