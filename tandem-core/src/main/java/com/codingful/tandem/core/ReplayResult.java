package com.codingful.tandem.core;

/**
 * Outcome of a replay (LLD-core §2.6, §8).
 *
 * @param matched  rows that matched the criteria
 * @param replayed rows actually re-queued for delivery (equals {@code matched} unless {@code dryRun})
 * @param dryRun   whether this was a dry run (no rows changed)
 */
public record ReplayResult(long matched, long replayed, boolean dryRun) {
}
