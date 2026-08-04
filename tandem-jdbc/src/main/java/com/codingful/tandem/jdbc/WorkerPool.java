package com.codingful.tandem.jdbc;

import com.codingful.tandem.core.port.OutboxDispatcher;
import com.codingful.tandem.core.port.OutboxStore;
import com.codingful.tandem.core.port.TandemMetrics;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Clock;
import java.time.Instant;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.OptionalLong;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * The relay engine (LLD-jdbc §3.1): a pool of supervised worker threads each owning a slice of the
 * instance's buckets and running the continuous claim-while-busy loop, plus periodic lease-reclaim
 * and cleanup jobs. Depends only on the {@code OutboxStore} + {@code OutboxDispatcher} ports — never
 * on {@code tandem-kafka}.
 */
public final class WorkerPool {

    private static final Logger LOG = System.getLogger(WorkerPool.class.getName());

    private final OutboxStore store;
    private final OutboxDispatcher dispatcher;
    private final RelayConfig cfg;
    private final TandemMetrics metrics;
    private final Clock clock;
    private final BackoffStrategy backoff;
    private final BucketSource bucketSource;
    private final RelayControlSource controlSource;
    private final String instanceId;

    private final int workerCount;
    private final RelayWorker[] workers;
    private final Thread[] threads;
    /**
     * Per-worker instant of the last completed claim cycle, for {@link #status()}. An
     * {@link AtomicLongArray} rather than a plain {@code long[]}: array <i>elements</i> are never
     * volatile, so a plain array would let a reader see an arbitrarily stale value — and a status
     * reading whose staleness is itself unbounded cannot answer the question it exists for.
     */
    private final AtomicLongArray lastCycleAtMillis;
    private volatile boolean running;
    private boolean stopping;   // guarded by this: a shutdown is transitioning; start() refuses meanwhile
    private ScheduledExecutorService scheduler;

    /** Embedded topology with the basic-round defaults (no-op metrics, system clock, full-jitter backoff). */
    public WorkerPool(OutboxStore store, OutboxDispatcher dispatcher, RelayConfig cfg) {
        this(store, dispatcher, cfg, TandemMetrics.NOOP, Clock.systemUTC(),
                BackoffStrategy.fullJitter(), BucketSource.embedded(cfg.bucketCount()), RelayControlSource.NOOP);
    }

    /**
     * Full topology constructor — override any of the basic-round defaults. Admin-API pause/resume is a
     * no-op with this overload, since it has no {@link RelayControlSource}; use the eight-argument
     * constructor for that (HLD-admin-api §4.1).
     *
     * @param store        relay-side persistence (poll/claim/update/cleanup)
     * @param dispatcher   the publish port (e.g. {@code KafkaRelay})
     * @param cfg          relay engine configuration
     * @param metrics      metrics sink; {@link TandemMetrics#NOOP} disables it
     * @param clock        used for cleanup's {@code doneBefore} cutoff; override in tests for determinism
     * @param backoff      retry-delay strategy for retriable dispatch failures
     * @param bucketSource which virtual buckets this instance owns ({@link BucketSource#embedded} or
     *                     {@link BucketLeaseManager} for the standalone topology)
     */
    public WorkerPool(OutboxStore store, OutboxDispatcher dispatcher, RelayConfig cfg,
                      TandemMetrics metrics, Clock clock, BackoffStrategy backoff, BucketSource bucketSource) {
        this(store, dispatcher, cfg, metrics, clock, backoff, bucketSource, RelayControlSource.NOOP);
    }

    /**
     * Full topology constructor, with Admin-API pause/resume support (HLD-admin-api §4.1).
     *
     * @param store         relay-side persistence (poll/claim/update/cleanup)
     * @param dispatcher    the publish port (e.g. {@code KafkaRelay})
     * @param cfg           relay engine configuration
     * @param metrics       metrics sink; {@link TandemMetrics#NOOP} disables it
     * @param clock         used for cleanup's {@code doneBefore} cutoff; override in tests for determinism
     * @param backoff       retry-delay strategy for retriable dispatch failures
     * @param bucketSource  which virtual buckets this instance owns ({@link BucketSource#embedded} or
     *                      {@link BucketLeaseManager} for the standalone topology)
     * @param controlSource the Admin API's desired pause state, refreshed on {@code reclaimInterval};
     *                      {@link RelayControlSource#NOOP} disables pause support entirely
     */
    public WorkerPool(OutboxStore store, OutboxDispatcher dispatcher, RelayConfig cfg,
                      TandemMetrics metrics, Clock clock, BackoffStrategy backoff, BucketSource bucketSource,
                      RelayControlSource controlSource) {
        this.store = Objects.requireNonNull(store, "store");
        this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
        this.cfg = Objects.requireNonNull(cfg, "cfg");
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.backoff = Objects.requireNonNull(backoff, "backoff");
        this.bucketSource = Objects.requireNonNull(bucketSource, "bucketSource");
        this.controlSource = Objects.requireNonNull(controlSource, "controlSource");
        // Single instance identity, shared with the LEASE bucket owner (RelayConfig#instanceId), so a
        // worker/thread-name/log line correlates directly to tandem_bucket_lease.owner (LLD-jdbc §3.2).
        this.instanceId = cfg.instanceId();
        this.workerCount = cfg.workersPerInstance();
        this.workers = new RelayWorker[workerCount];
        this.threads = new Thread[workerCount];
        this.lastCycleAtMillis = new AtomicLongArray(workerCount);
    }

    /** Validate config (fail-fast), then start the worker threads and the maintenance jobs. Idempotent guard. */
    public synchronized void start() {
        if (running) {
            return;
        }
        if (stopping) {
            // A shutdown is mid-join on another thread; silently interleaving a fresh start would race
            // it, so the call is refused — visibly, since the caller's relay will simply not be running.
            LOG.log(Level.WARNING, "start() ignored: a stop() is still in progress instanceId:" + instanceId);
            return;
        }
        // hard invariant rowLease > delivery.timeout.ms (§3.5), validated against the dispatcher's own
        // effective timeout when it reports one — so it cannot pass against a stale configured default.
        OptionalLong dispatcherTimeout = dispatcher.deliveryTimeoutMillis();
        if (dispatcherTimeout.isPresent() && dispatcherTimeout.getAsLong() != cfg.deliveryTimeoutMs()) {
            LOG.log(Level.WARNING, "Validating rowLease against the dispatcher's reported delivery timeout,"
                    + " not the configured value dispatcherDeliveryTimeoutMs:" + dispatcherTimeout.getAsLong()
                    + ", configuredDeliveryTimeoutMs:" + cfg.deliveryTimeoutMs());
        }
        cfg.checkRowLeaseSafe(dispatcherTimeout.orElse(cfg.deliveryTimeoutMs()), metrics, LOG);
        bucketSource.validateOnStart(metrics, LOG);   // LEASE precondition: lease table seeded (§3.2)
        controlSource.onStart();   // publishes this instance's coordination mode (HLD-admin-api §4.1)
        controlSource.refresh();   // known pause state from the first cycle, not just after the first tick
        running = true;
        LOG.log(Level.INFO, "Starting relay instanceId:" + instanceId + ", workers:" + workerCount
                + ", coordination:" + cfg.coordination());
        for (int i = 0; i < workerCount; i++) {
            int index = i;
            String workerId = "tandem-relay-" + instanceId + "-w" + index;
            workers[i] = new RelayWorker(store, dispatcher, cfg, backoff, metrics, workerId,
                    () -> sliceFor(index));
            startWorkerThread(index);
        }
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> daemon(r, "tandem-relay-" + instanceId + "-jobs"));
        long reclaimMs = cfg.reclaimInterval().toMillis();
        scheduler.scheduleWithFixedDelay(this::reclaimTick, reclaimMs, reclaimMs, TimeUnit.MILLISECONDS);
        long cleanupMs = cfg.cleanupInterval().toMillis();
        scheduler.scheduleWithFixedDelay(this::cleanupTick, cleanupMs, cleanupMs, TimeUnit.MILLISECONDS);
        scheduler.scheduleWithFixedDelay(this::heartbeatTick, reclaimMs, reclaimMs, TimeUnit.MILLISECONDS);
        scheduler.scheduleWithFixedDelay(this::controlTick, reclaimMs, reclaimMs, TimeUnit.MILLISECONDS);
        if (metrics.isEnabled()) {
            // Scheduled only when an adapter is wired: with the no-op default the lag query never runs,
            // so an outbox nobody is watching pays nothing for the gauges (§4). The first reading is
            // taken immediately rather than one interval in: a relay that just started may be inheriting
            // a backlog from the instance it replaced, and that is the moment worth seeing.
            long metricsMs = cfg.metricsInterval().toMillis();
            scheduler.scheduleWithFixedDelay(this::metricsTick, 0, metricsMs, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * This worker's slice of the instance's currently-owned, not-paused buckets:
     * {@code bucket % workerCount == index}. Ownership is queried fresh every call (as it always was);
     * pause is a cached, in-memory read (HLD-admin-api §4.1) — never a query on this hot path.
     */
    private Set<Integer> sliceFor(int index) {
        if (controlSource.wholeRelayPaused()) {
            return Set.of();
        }
        Set<Integer> owned = bucketSource.ownedBuckets();
        Set<Integer> mine = new HashSet<>();
        for (int bucket : owned) {
            if (Math.floorMod(bucket, workerCount) == index && !controlSource.bucketPaused(bucket)) {
                mine.add(bucket);
            }
        }
        return mine;
    }

    private void startWorkerThread(int index) {
        // Starting counts as this worker's first cycle, so a worker that hangs on its very first claim
        // shows an ageing timestamp rather than none at all — and a restarted one does not inherit the
        // dead thread's staleness (§3.8).
        lastCycleAtMillis.set(index, clock.millis());
        Thread t = new Thread(() -> runWorker(index), "tandem-relay-" + instanceId + "-w" + index);
        t.setDaemon(true);
        // Supervised: an Error that escapes the loop kills the thread; restart it so its buckets are
        // not abandoned (critical for the embedded topology, which has no lease table to self-heal, §3.1).
        // The restart takes this monitor and re-checks running, so every threads[] write/read is under
        // the lock (visibility) and a restart cannot slip in after shutdown began. beginHalt() holds the
        // lock only for the brief transition, not the join, so this handler never stalls behind a join;
        // if it does restart just as shutdown starts, that worker is a daemon that exits on running=false.
        t.setUncaughtExceptionHandler((thread, error) -> {
            LOG.log(Level.ERROR, "Relay worker died; restarting workerIndex:" + index, error);
            synchronized (this) {
                if (running) {
                    startWorkerThread(index);
                }
            }
        });
        threads[index] = t;
        t.start();
    }

    private void runWorker(int index) {
        RelayWorker worker = workers[index];
        while (running) {
            try {
                int claimed = worker.claimAndDispatch();
                worker.flushDone();
                worker.flushFailures();
                // Progress, deliberately marked only on a cycle that completed without throwing: a
                // worker whose every iteration fails is not making progress, and status() must say so
                // (§3.8). Claiming nothing is still progress — an idle relay is a working relay.
                lastCycleAtMillis.set(index, clock.millis());
                if (!running) {
                    break;
                }
                if (claimed > 0 && LOG.isLoggable(Level.DEBUG)) {
                    LOG.log(Level.DEBUG, "Worker claimed rows workerIndex:" + index + ", claimed:" + claimed);
                }
                if (claimed == 0) {
                    // No work claimed: idle-backoff when nothing is in flight, else a brief pause so
                    // async completions can land before we re-flush (§3.1 — pollInterval is idle backoff).
                    sleep(worker.inFlight() == 0 ? cfg.pollInterval().toMillis() : 5);
                }
            } catch (Exception perIteration) {
                LOG.log(Level.ERROR, "Relay worker iteration failed workerIndex:" + index
                        + ", instanceId:" + instanceId, perIteration);
                sleep(cfg.pollInterval().toMillis());
            }
        }
        worker.flushDone();       // best-effort drain of any acked ids on shutdown
        worker.flushFailures();   // and of any captured failures, so no outcome is lost to the lease
    }

    private void reclaimTick() {
        try {
            int reclaimed = store.reclaimExpiredLeases();
            if (reclaimed > 0) {
                if (metrics.isEnabled()) {
                    metrics.incrementLeaseExpired(reclaimed);
                }
                if (LOG.isLoggable(Level.DEBUG)) {
                    LOG.log(Level.DEBUG, "Reclaimed expired leases count:" + reclaimed);
                }
            }
        } catch (Exception e) {
            LOG.log(Level.ERROR, "Lease reclaim failed", e);
        }
    }

    private void cleanupTick() {
        try {
            Instant doneBefore = clock.instant().minus(cfg.retention());
            store.cleanup(doneBefore, cfg.cleanupBatchSize());
        } catch (Exception e) {
            LOG.log(Level.ERROR, "Cleanup failed", e);
        }
    }

    /**
     * Read the lag and failed-count gauges and report how many workers are actually alive and how long
     * the slowest of them has gone without progress (§4, §3.8). Failing here must never disturb
     * delivery — an unreadable gauge is a monitoring problem, not a relay problem — so, like the other
     * maintenance jobs, it swallows the failure after logging it.
     */
    private void metricsTick() {
        try {
            store.lag().ifPresent(lag -> {
                metrics.recordLag(lag.pending());
                metrics.recordLagAgeSeconds(lag.ageSecondsAt(clock.instant()));
            });
            store.failedCount().ifPresent(metrics::recordFailed);
            store.blockedCount().ifPresent(metrics::recordBlocked);
            bucketSource.uncoveredBuckets().ifPresent(metrics::recordUncoveredBuckets);
            // One status() call for both gauges below: it is already an in-process, database-free
            // snapshot (§3.8), so there is no cost split to weigh — only two readings of one moment
            // to keep consistent with each other, exactly like the single lag() query above.
            RelayStatus status = status();
            metrics.recordActiveWorkers(status.workersAlive());
            metrics.recordWorkerCycleAgeSeconds(status.oldestWorkerCycleAgeSecondsAt(clock.instant()));
        } catch (Exception e) {
            LOG.log(Level.ERROR, "Reading relay metrics failed", e);
        }
    }

    /**
     * This instance's current in-process state (§3.8) — what an application's own readiness/liveness
     * probe reads. <b>Never queries the database</b>, by contract, so it is safe to call at probe
     * frequency; see {@link RelayStatus} for what that deliberately excludes and why Tandem draws no
     * health verdict of its own.
     *
     * <p>Alive, not configured: a worker whose thread died and has not been restarted yet must not
     * count, and its stale cycle timestamp must not drag {@code oldestWorkerCycle} down either.
     *
     * @return an immutable reading, taken under the same monitor the worker-thread bookkeeping uses so
     *         the counts and the timestamp describe one consistent moment
     */
    public synchronized RelayStatus status() {
        RelayStatus.State state = running
                ? RelayStatus.State.RUNNING
                : (stopping ? RelayStatus.State.STOPPING : RelayStatus.State.STOPPED);
        int alive = 0;
        long oldest = Long.MAX_VALUE;
        for (int i = 0; i < workerCount; i++) {
            Thread t = threads[i];
            if (t != null && t.isAlive()) {
                alive++;
                oldest = Math.min(oldest, lastCycleAtMillis.get(i));
            }
        }
        return new RelayStatus(instanceId, state, cfg.coordination(), workerCount, alive,
                alive == 0 ? Optional.empty() : Optional.of(Instant.ofEpochMilli(oldest)));
    }

    private void heartbeatTick() {
        try {
            bucketSource.heartbeat();
        } catch (Exception e) {
            LOG.log(Level.ERROR, "Bucket heartbeat failed", e);
        }
    }

    /** Refreshes the cached Admin-API pause state (HLD-admin-api §4.1). Never disturbs delivery on failure. */
    private void controlTick() {
        try {
            controlSource.refresh();
        } catch (Exception e) {
            LOG.log(Level.ERROR, "Relay control refresh failed", e);
        }
    }

    /** Graceful shutdown: stop polling, let workers finish their cycle, release buckets. In-flight rows are recovered by lease reclaim (§3.1/§3.5). */
    public void stop() {
        Thread[] toJoin = beginHalt();
        if (toJoin == null) {
            return;
        }
        try {
            joinAll(toJoin);   // outside the lock, so a worker's uncaught-handler restart never stalls on it
            try {
                bucketSource.release();
            } catch (Exception e) {
                LOG.log(Level.ERROR, "Releasing buckets failed", e);
            }
            LOG.log(Level.INFO, "Relay stopped instanceId:" + instanceId);
        } finally {
            finishHalt();
        }
    }

    /**
     * Ungraceful stop: halts the worker threads and scheduler exactly like {@link #stop()}, but
     * deliberately <b>skips</b> {@code bucketSource.release()} — simulating an instance that crashed
     * rather than shut down cleanly, so under {@code LEASE} its bucket ownership and presence
     * ({@code tandem_relay_member}) are discovered stale only once their leases expire (§3.2), not
     * released immediately as a graceful {@link #stop()} would. Real crashes need no explicit call at
     * all (the process is simply gone); this exists so tests can exercise the lease-expiry self-heal
     * path against a live, running instance rather than driving {@code BucketSource} heartbeats by
     * hand (contrast {@code BucketLeaseManagerIT}, which does exactly that at a lower level). Under
     * {@code SINGLE}, where {@code release()} is a no-op, {@code kill()} and {@link #stop()} are
     * equivalent.
     */
    public void kill() {
        Thread[] toJoin = beginHalt();
        if (toJoin == null) {
            return;
        }
        try {
            joinAll(toJoin);
        } finally {
            finishHalt();
        }
    }

    /**
     * Locked transition step: clear {@code running}, stop the scheduler and interrupt the workers,
     * and return a snapshot of the threads to join. Returns {@code null} if already stopped. The
     * <b>join happens outside</b> this critical section (see {@link #stop()}) so a worker whose
     * uncaught-exception handler is trying to restart — which also needs this monitor — never blocks
     * behind a 10s join. {@code stopping} keeps {@link #start()} from interleaving until the join is done.
     */
    private synchronized Thread[] beginHalt() {
        if (!running) {
            return null;
        }
        running = false;
        stopping = true;
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
        Thread[] snapshot = threads.clone();
        for (Thread t : snapshot) {
            if (t != null) {
                t.interrupt();
            }
        }
        return snapshot;
    }

    private synchronized void finishHalt() {
        stopping = false;
    }

    private static void joinAll(Thread[] toJoin) {
        for (Thread t : toJoin) {
            if (t != null) {
                joinQuietly(t);
            }
        }
    }

    private static Thread daemon(Runnable r, String name) {
        Thread t = new Thread(r, name);
        t.setDaemon(true);
        return t;
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void joinQuietly(Thread t) {
        try {
            t.join(TimeUnit.SECONDS.toMillis(10));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
