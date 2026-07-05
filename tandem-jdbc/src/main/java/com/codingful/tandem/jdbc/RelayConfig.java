package com.codingful.tandem.jdbc;

import com.codingful.tandem.core.exception.TandemConfigurationException;
import com.codingful.tandem.core.port.TandemMetrics;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Relay engine configuration with the basic-round defaults (LLD-jdbc §6). Immutable; built via
 * {@link #builder()} or {@link #defaults()}.
 *
 * <p>{@code deliveryTimeoutMs} is a <b>fallback</b> for the hard invariant
 * {@code rowLease > delivery.timeout.ms} enforced at startup (§3.5) — the relay itself has no Kafka
 * dependency. When the wired dispatcher reports its own effective timeout (the Kafka dispatcher does,
 * via {@link com.codingful.tandem.core.port.OutboxDispatcher#deliveryTimeoutMillis()}), the relay
 * validates against <b>that</b> authoritative value instead, so this field only matters for a
 * dispatcher that does not report one.
 */
public final class RelayConfig {

    /** Metric/log identifier for the row-lease invariant (LLD-jdbc §3.5). */
    public static final String CHECK_ROW_LEASE = "row_lease_not_above_delivery_timeout";

    /** Upper bound for {@code bucketCount}: the {@code tandem_outbox.bucket} column is {@code SMALLINT}. */
    public static final int MAX_BUCKET_COUNT = 32_767;

    private final int bucketCount;
    private final Coordination coordination;
    private final String instanceId;
    private final Duration bucketLease;
    private final int workersPerInstance;
    private final Duration pollInterval;
    private final int batchSize;
    private final Duration rowLease;
    private final int maxAttempts;
    private final Duration retention;
    private final int cleanupBatchSize;
    private final Duration reclaimInterval;
    private final Duration cleanupInterval;
    private final long deliveryTimeoutMs;

    private RelayConfig(Builder b) {
        this.bucketCount = b.bucketCount;
        this.coordination = b.coordination;
        this.instanceId = b.instanceId != null ? b.instanceId : deriveInstanceId();
        this.bucketLease = b.bucketLease;
        this.workersPerInstance = b.workersPerInstance;
        this.pollInterval = b.pollInterval;
        this.batchSize = b.batchSize;
        this.rowLease = b.rowLease;
        this.maxAttempts = b.maxAttempts;
        this.retention = b.retention;
        this.cleanupBatchSize = b.cleanupBatchSize;
        this.reclaimInterval = b.reclaimInterval;
        this.cleanupInterval = b.cleanupInterval;
        this.deliveryTimeoutMs = b.deliveryTimeoutMs;
    }

    public int bucketCount() {
        return bucketCount;
    }

    /** How concurrent relay instances share buckets — {@code SINGLE} (default) or {@code LEASE} (§3.2). */
    public Coordination coordination() {
        return coordination;
    }

    /**
     * This instance's unique identity, used as the {@code tandem_bucket_lease.owner} under
     * {@link Coordination#LEASE}. Never null — a stable {@code host-pid-<rand>} is derived when unset.
     */
    public String instanceId() {
        return instanceId;
    }

    /**
     * The bucket-ownership lease held under {@link Coordination#LEASE}, renewed each
     * {@link #reclaimInterval()}. <b>Independent</b> of {@link #rowLease()} (the per-row IN_FLIGHT lease).
     */
    public Duration bucketLease() {
        return bucketLease;
    }

    public int workersPerInstance() {
        return workersPerInstance;
    }

    public Duration pollInterval() {
        return pollInterval;
    }

    public int batchSize() {
        return batchSize;
    }

    public Duration rowLease() {
        return rowLease;
    }

    public int maxAttempts() {
        return maxAttempts;
    }

    public Duration retention() {
        return retention;
    }

    public int cleanupBatchSize() {
        return cleanupBatchSize;
    }

    public Duration reclaimInterval() {
        return reclaimInterval;
    }

    public Duration cleanupInterval() {
        return cleanupInterval;
    }

    public long deliveryTimeoutMs() {
        return deliveryTimeoutMs;
    }

    /** The §6 defaults. */
    public static RelayConfig defaults() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Enforce the hard invariant against this config's own {@link #deliveryTimeoutMs()} — the fallback
     * when the dispatcher does not report its effective timeout. Prefer
     * {@link #checkRowLeaseSafe(long, TandemMetrics, Logger)} from the relay, which passes the
     * dispatcher's authoritative value.
     */
    public void checkRowLeaseSafe(TandemMetrics metrics, Logger logger) {
        checkRowLeaseSafe(deliveryTimeoutMs, metrics, logger);
    }

    /**
     * Enforce the hard invariant {@code rowLease > effectiveDeliveryTimeoutMs} (LLD-jdbc §3.5) against
     * the <b>effective</b> delivery timeout — the value the dispatcher actually enforces
     * ({@link com.codingful.tandem.core.port.OutboxDispatcher#deliveryTimeoutMillis()}), so the check
     * cannot pass against a stale configured default while the real producer is unsafe. Records the
     * {@code config.invalid} metric (best-effort, before the throw), logs one ERROR line with the
     * canonical message and structured fields, then throws {@link TandemConfigurationException} with
     * that same message. Called once at relay startup.
     *
     * @param effectiveDeliveryTimeoutMs the delivery timeout to validate against, in milliseconds
     */
    public void checkRowLeaseSafe(long effectiveDeliveryTimeoutMs, TandemMetrics metrics, Logger logger) {
        long rowLeaseMs = rowLease.toMillis();
        if (rowLeaseMs > effectiveDeliveryTimeoutMs) {
            return;
        }
        long recommendedMinRowLeaseMs = 2 * effectiveDeliveryTimeoutMs;
        String message = rowLeaseInvariantMessage(rowLeaseMs, effectiveDeliveryTimeoutMs, recommendedMinRowLeaseMs);
        metrics.recordConfigInvalid(CHECK_ROW_LEASE);   // best-effort, before aborting
        logger.log(Level.ERROR, message
                + " rowLeaseMs:" + rowLeaseMs
                + ", deliveryTimeoutMs:" + effectiveDeliveryTimeoutMs
                + ", recommendedMinRowLeaseMs:" + recommendedMinRowLeaseMs);
        throw new TandemConfigurationException(message);
    }

    /** The single canonical message reused verbatim by the exception and the log (LLD-jdbc §3.5). */
    static String rowLeaseInvariantMessage(long rowLeaseMs, long deliveryTimeoutMs, long recommendedMinRowLeaseMs) {
        return "Unsafe relay config: rowLease (=" + rowLeaseMs + " ms) must be > Kafka producer "
                + "delivery.timeout.ms (=" + deliveryTimeoutMs + " ms). When rowLease <= delivery.timeout.ms, "
                + "a row's lease can expire while its publish is still in progress, so lease-reclaim resets it "
                + "to PENDING and another worker re-publishes it -> duplicate events. Required: rowLease > "
                + "delivery.timeout.ms (recommended rowLease >= 2 x delivery.timeout.ms = "
                + recommendedMinRowLeaseMs + " ms). Fix: raise `tandem.relay.row-lease` above "
                + "delivery.timeout.ms, or lower the producer `delivery.timeout.ms` below rowLease.";
    }

    /**
     * A stable, unique-enough default lease owner: {@code tandem-<host>-<pid>-<rand>}, capped to the
     * {@code tandem_bucket_lease.owner} length (64). Stable for the process lifetime; operators may
     * override via {@link Builder#instanceId(String)} for stability across restarts.
     */
    static String deriveInstanceId() {
        String host = System.getenv().getOrDefault("HOSTNAME",
                System.getenv().getOrDefault("COMPUTERNAME", "host"));
        if (host.length() > 30) {
            host = host.substring(0, 30);
        }
        long pid = ProcessHandle.current().pid();
        String rand = Integer.toHexString(ThreadLocalRandom.current().nextInt() & 0xffff);
        String id = "tandem-" + host + "-" + pid + "-" + rand;
        return id.length() > 64 ? id.substring(0, 64) : id;
    }

    /** Validates a bucket count against the schema bound: {@code 0 < bucketCount <= MAX_BUCKET_COUNT}. */
    static int boundedBucketCount(int bucketCount) {
        if (bucketCount <= 0) {
            throw new IllegalArgumentException("bucketCount must be positive");
        }
        if (bucketCount > MAX_BUCKET_COUNT) {
            throw new IllegalArgumentException("bucketCount exceeds the tandem_outbox.bucket SMALLINT bound ("
                    + MAX_BUCKET_COUNT + "): " + bucketCount);
        }
        return bucketCount;
    }

    public static final class Builder {
        private int bucketCount = 256;
        private Coordination coordination = Coordination.SINGLE;
        private String instanceId = null;   // derived at build() when unset
        private Duration bucketLease = Duration.ofSeconds(30);
        private int workersPerInstance = Math.max(1, Runtime.getRuntime().availableProcessors() * 2);
        private Duration pollInterval = Duration.ofMillis(100);
        private int batchSize = 100;
        private Duration rowLease = Duration.ofSeconds(60);
        private int maxAttempts = 10;
        private Duration retention = Duration.ofDays(14);
        private int cleanupBatchSize = 1000;
        private Duration reclaimInterval = Duration.ofSeconds(5);
        private Duration cleanupInterval = Duration.ofMinutes(15);
        private long deliveryTimeoutMs = 30_000;   // Kafka producer default (LLD-kafka §1)

        private Builder() {
        }

        /**
         * Must never change after first deployment — it is baked into every outbox row's bucket.
         * At most {@link #MAX_BUCKET_COUNT} (the {@code bucket SMALLINT} column). Default 256.
         */
        public Builder bucketCount(int bucketCount) {
            this.bucketCount = boundedBucketCount(bucketCount);
            return this;
        }

        /**
         * How concurrent relay instances share buckets. {@code SINGLE} (default) = one instance owns all
         * buckets, no table; {@code LEASE} = lease-partitioned for any number of instances (§3.2). Must be
         * declared statically — running more than one instance under {@code SINGLE} wastes DB work.
         */
        public Builder coordination(Coordination coordination) {
            this.coordination = Objects.requireNonNull(coordination, "coordination");
            return this;
        }

        /**
         * This instance's unique {@code LEASE} owner id (≤ 64 chars). When unset, a stable
         * {@code host-pid-<rand>} is derived. Ignored under {@code SINGLE}.
         */
        public Builder instanceId(String instanceId) {
            if (instanceId != null) {
                if (instanceId.isBlank()) {
                    throw new IllegalArgumentException("instanceId must not be blank");
                }
                if (instanceId.length() > 64) {
                    throw new IllegalArgumentException("instanceId exceeds the tandem_bucket_lease.owner length (64)");
                }
            }
            this.instanceId = instanceId;
            return this;
        }

        /**
         * The bucket-ownership lease held under {@code LEASE}, renewed each {@code reclaimInterval}.
         * Independent of {@code rowLease}. Default 30s.
         */
        public Builder bucketLease(Duration bucketLease) {
            this.bucketLease = Objects.requireNonNull(bucketLease, "bucketLease");
            return this;
        }

        /** Worker threads in this instance's pool. Default {@code availableProcessors() * 2}. */
        public Builder workersPerInstance(int workersPerInstance) {
            this.workersPerInstance = positive(workersPerInstance, "workersPerInstance");
            return this;
        }

        /** Idle-backoff between claim attempts when nothing was claimed. Default 100ms. */
        public Builder pollInterval(Duration pollInterval) {
            this.pollInterval = Objects.requireNonNull(pollInterval, "pollInterval");
            return this;
        }

        /** Max rows claimed per poll. Default 100. */
        public Builder batchSize(int batchSize) {
            this.batchSize = positive(batchSize, "batchSize");
            return this;
        }

        /**
         * How long a claimed row's lease is held before another worker may reclaim it. Must be
         * {@code > deliveryTimeoutMs} ({@link #checkRowLeaseSafe}) — otherwise a lease can expire
         * while its publish is still in flight, causing a duplicate. Default 60s.
         */
        public Builder rowLease(Duration rowLease) {
            this.rowLease = Objects.requireNonNull(rowLease, "rowLease");
            return this;
        }

        /** Retriable failures allowed before a row is quarantined to {@code FAILED}. Default 10. */
        public Builder maxAttempts(int maxAttempts) {
            this.maxAttempts = positive(maxAttempts, "maxAttempts");
            return this;
        }

        /** How long a {@code DONE} row is kept before {@code cleanup} deletes it. Default 14 days. */
        public Builder retention(Duration retention) {
            this.retention = Objects.requireNonNull(retention, "retention");
            return this;
        }

        /** Max rows deleted per cleanup pass. Default 1000. */
        public Builder cleanupBatchSize(int cleanupBatchSize) {
            this.cleanupBatchSize = positive(cleanupBatchSize, "cleanupBatchSize");
            return this;
        }

        /** How often the maintenance job reclaims expired leases. Default 5s. */
        public Builder reclaimInterval(Duration reclaimInterval) {
            this.reclaimInterval = Objects.requireNonNull(reclaimInterval, "reclaimInterval");
            return this;
        }

        /** How often the maintenance job runs {@code cleanup}. Default 15min. */
        public Builder cleanupInterval(Duration cleanupInterval) {
            this.cleanupInterval = Objects.requireNonNull(cleanupInterval, "cleanupInterval");
            return this;
        }

        /**
         * Fallback {@code delivery.timeout.ms} used to validate {@link #rowLease} at startup
         * ({@link #checkRowLeaseSafe}) only when the wired dispatcher does not report its own effective
         * timeout — this module has no Kafka dependency itself. The Kafka dispatcher does report one, so
         * it overrides this value; setting it then has no effect. Default 30000 (the Kafka producer default).
         */
        public Builder deliveryTimeoutMs(long deliveryTimeoutMs) {
            this.deliveryTimeoutMs = positive(deliveryTimeoutMs, "deliveryTimeoutMs");
            return this;
        }

        public RelayConfig build() {
            return new RelayConfig(this);
        }

        private static int positive(int value, String field) {
            if (value <= 0) {
                throw new IllegalArgumentException(field + " must be positive");
            }
            return value;
        }

        private static long positive(long value, String field) {
            if (value <= 0) {
                throw new IllegalArgumentException(field + " must be positive");
            }
            return value;
        }
    }
}
