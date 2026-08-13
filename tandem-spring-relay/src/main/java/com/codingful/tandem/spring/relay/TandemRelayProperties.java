package com.codingful.tandem.spring.relay;

import com.codingful.tandem.jdbc.Coordination;
import java.time.Duration;

/**
 * Binds {@code tandem.relay.*} — the relay engine settings (LLD-spring-config §2.2), one key per
 * {@link com.codingful.tandem.jdbc.RelayConfig} setting.
 *
 * <p>Every component is <b>nullable</b> on purpose: an unset property leaves {@code RelayConfig}'s own
 * default in force, so the config object stays the single source of truth for behaviour and the two can
 * never drift. {@code tandem.relay.enabled} is <em>not</em> here — it gates the whole autoconfiguration
 * via {@code @ConditionalOnProperty} rather than binding into a config object.
 *
 * @param coordination       {@code SINGLE} (default) or {@code LEASE} — how concurrent relay instances share buckets
 * @param instanceId         unique instance id; unset → a derived {@code tandem-<host>-<pid>-<rand>}
 * @param bucketLease        LEASE bucket-lease duration; default {@code 30s}
 * @param workersPerInstance worker threads; default {@code availableProcessors() * 2}
 * @param pollInterval       idle backoff between empty claims (±20% jitter); default {@code 100ms}
 * @param batchSize          per-shard in-flight window; default {@code 100}
 * @param rowLease           per-row claim lease; default {@code 60s} (must exceed the delivery timeout)
 * @param maxAttempts        delivery attempts before a row is quarantined; default {@code 10}
 * @param retention          how long DONE rows are kept before cleanup; default {@code 14d}
 * @param cleanupBatchSize   rows deleted per cleanup pass; default {@code 1000}
 * @param reclaimInterval    how often expired row leases are reclaimed, and the cap of the relay's
 *                           backoff after a failing cycle; default {@code 5s}
 * @param cleanupInterval    how often DONE rows are cleaned up; default {@code 15m}
 * @param metricsInterval    how often the relay reports backlog, backlog age and live workers; default {@code 10s}
 * @param logEveryRows       per-worker INFO progress log every N dispatch outcomes (ok + ko); default {@code 10000}
 */
@org.springframework.boot.context.properties.ConfigurationProperties("tandem.relay")
public record TandemRelayProperties(
        Coordination coordination,
        String instanceId,
        Duration bucketLease,
        Integer workersPerInstance,
        Duration pollInterval,
        Integer batchSize,
        Duration rowLease,
        Integer maxAttempts,
        Duration retention,
        Integer cleanupBatchSize,
        Duration reclaimInterval,
        Duration cleanupInterval,
        Duration metricsInterval,
        Long logEveryRows) {
}
