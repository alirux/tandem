/**
 * Tandem's exception hierarchy (LLD-core §3), all rooted at the unchecked
 * {@link com.codingful.tandem.core.exception.TandemException}. Unchecked because the write-side insert
 * already runs inside the caller's transaction, so a checked exception would add ceremony without a
 * meaningful recovery path beyond catching it.
 *
 * <p>{@link com.codingful.tandem.core.exception.DuplicateSeqException} lets a caller detect a
 * {@code (aggregate_id, seq)} conflict without parsing SQL state;
 * {@link com.codingful.tandem.core.exception.OutboxDispatchException} carries the dispatcher's
 * retriable-vs-permanent verdict; {@link com.codingful.tandem.core.exception.TandemConfigurationException}
 * signals a startup invariant the relay fails fast on.
 */
package com.codingful.tandem.core.exception;
