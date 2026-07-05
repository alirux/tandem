package com.codingful.tandem.benchmark;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * In-process side-channel for {@link BenchmarkConfig.LatencyMode#ACCURATE} (HLD-load-testing.md §2.2,
 * LLD-benchmark §5.1): {@link LoadGenerator} records the post-COMMIT timestamp here, keyed by
 * {@code (aggregateId, seq)}; {@link CorrelationConsumer} looks it up on receive and removes the
 * entry. Because both reads happen in the same JVM, this removes the INSERT→COMMIT skew a header
 * written at INSERT time necessarily carries.
 */
public final class CommitTimestamps {

    private final ConcurrentMap<String, Long> commitNanosByKey = new ConcurrentHashMap<>();

    public void recordCommit(String aggregateId, long seq, long commitNanos) {
        commitNanosByKey.put(key(aggregateId, seq), commitNanos);
    }

    /** Removes and returns the recorded commit time, or {@code -1} if absent. */
    public long takeCommitNanos(String aggregateId, long seq) {
        Long value = commitNanosByKey.remove(key(aggregateId, seq));
        return value == null ? -1 : value;
    }

    private static String key(String aggregateId, long seq) {
        return aggregateId + '#' + seq;
    }
}
