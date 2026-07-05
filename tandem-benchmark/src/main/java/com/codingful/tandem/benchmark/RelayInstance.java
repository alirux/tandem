package com.codingful.tandem.benchmark;

import com.codingful.tandem.jdbc.BucketSource;
import com.codingful.tandem.jdbc.WorkerPool;
import com.codingful.tandem.kafka.KafkaRelay;

/**
 * One simulated relay instance — its own Kafka producer and {@link BucketSource} — built by
 * {@link BenchmarkEnvironment#newRelayInstance}. Used by scenarios that run more than one relay
 * instance against one shared outbox (S8: {@code Coordination.LEASE}, HLD §3.2 axis 2), where each
 * simulated instance needs its own producer (as a real, separate relay process would) but shares the
 * environment's {@code DataSource}/{@code JdbcOutboxStore} (as real instances share one DB).
 */
public record RelayInstance(WorkerPool pool, BucketSource bucketSource, KafkaRelay producer) {
}
