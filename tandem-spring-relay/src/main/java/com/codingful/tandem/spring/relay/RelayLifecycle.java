package com.codingful.tandem.spring.relay;

import com.codingful.tandem.jdbc.BucketCountGuard;
import com.codingful.tandem.jdbc.WorkerPool;
import java.util.Objects;
import javax.sql.DataSource;
import org.springframework.context.SmartLifecycle;

/**
 * Ties the relay's {@link WorkerPool} to the Spring application lifecycle (LLD-spring-config §4.5): it
 * starts after the context is built and stops before it tears down, so in-flight sends drain gracefully.
 * The {@code WorkerPool} stays a plain JDBC type, unaware of Spring; this thin adapter is the only
 * Spring-aware piece. {@link #start()} runs the bucket-count guard before the pool starts, so a divergent
 * bucket count fails application startup (LLD-spring-config §3) rather than surfacing at runtime; the
 * pool's own start then performs the row-lease and lease-table fail-fasts.
 */
class RelayLifecycle implements SmartLifecycle {

    private final WorkerPool workerPool;
    private final DataSource dataSource;
    private final int bucketCount;
    private volatile boolean running;

    RelayLifecycle(WorkerPool workerPool, DataSource dataSource, int bucketCount) {
        this.workerPool = Objects.requireNonNull(workerPool, "workerPool");
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.bucketCount = bucketCount;
    }

    @Override
    public void start() {
        BucketCountGuard.check(dataSource, bucketCount);
        workerPool.start();
        running = true;
    }

    @Override
    public void stop() {
        workerPool.stop();
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}
