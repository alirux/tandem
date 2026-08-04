package com.codingful.tandem.sample.spring;

import com.codingful.tandem.test.TandemTestContainer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * [DEMO-ONLY] The {@code lease} profile's runner ({@code run-lease.sh}): demonstrates the Admin API's
 * relay-control endpoints that only mean anything under {@code LEASE} coordination —
 * {@code GET /relay/buckets}, {@code GET /relay/buckets/{bucket}}, {@code GET /relay/workers},
 * {@code POST /relay/buckets/{bucket}/release}
 * — against a real {@code tandem_bucket_lease}/{@code tandem_relay_member}, which {@code SINGLE} (the
 * default profile's demo) never touches at all. {@link SampleRunner} covers the write-side tiers and
 * the endpoints that work under either coordination mode (reads, replay/discard, whole-relay
 * pause/resume); this runner does not repeat that, it only covers what {@code LEASE} adds.
 */
@Component
@Profile("lease")
class LeaseRelayDemoRunner implements CommandLineRunner {

    private final OrderService orders;
    private final TandemTestContainer infrastructure;

    LeaseRelayDemoRunner(OrderService orders, TandemTestContainer infrastructure) {
        this.orders = orders;
        this.infrastructure = infrastructure;
    }

    @Override
    public void run(String... args) throws Exception {
        banner();
        orders.place("order-lease-1");
        orders.place("order-lease-2");
        System.out.println("► Waiting for the relay to claim its buckets under LEASE coordination...");
        int ownedBucket = awaitAnOwnedBucket(Duration.ofSeconds(15));
        String instanceId = ownerOf(ownedBucket);
        System.out.printf("  bucket %d is owned by %s%n%n", ownedBucket, instanceId);
        printAdminApiHints(ownedBucket);
    }

    /** Poll tandem_bucket_lease until the relay's heartbeat (reclaimInterval, sped up above) claims one. */
    private int awaitAnOwnedBucket(Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            try (Connection connection = infrastructure.dataSource().getConnection();
                    PreparedStatement statement = connection.prepareStatement(
                            "SELECT bucket FROM tandem_bucket_lease WHERE owner IS NOT NULL AND lease_until > now() LIMIT 1");
                    ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getInt("bucket");
                }
            }
            Thread.sleep(200);
        }
        throw new IllegalStateException("no bucket was claimed within " + timeout + " - is coordination=LEASE set?");
    }

    private String ownerOf(int bucket) throws Exception {
        try (Connection connection = infrastructure.dataSource().getConnection();
                PreparedStatement statement = connection.prepareStatement(
                        "SELECT owner FROM tandem_bucket_lease WHERE bucket = ?")) {
            statement.setInt(1, bucket);
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                return resultSet.getString("owner");
            }
        }
    }

    private static void printAdminApiHints(int ownedBucket) {
        System.out.println("► Admin API — relay control under LEASE (the app keeps running as a web server):\n");
        System.out.println("  curl http://localhost:8080/tandem/admin/v1/relay/status");
        System.out.println("  curl http://localhost:8080/tandem/admin/v1/relay/workers");
        System.out.println("  curl http://localhost:8080/tandem/admin/v1/relay/buckets");
        System.out.printf("%n  bucket %d is currently owned above — read it on its own:%n", ownedBucket);
        System.out.printf("  curl http://localhost:8080/tandem/admin/v1/relay/buckets/%d%n", ownedBucket);
        System.out.println("\n  force-release it and watch it get reassigned:");
        System.out.printf("  curl -X POST http://localhost:8080/tandem/admin/v1/relay/buckets/%d/release%n", ownedBucket);
    }

    private static void banner() {
        System.out.println("=".repeat(60));
        System.out.println(" Tandem — relay control under LEASE coordination");
        System.out.println("=".repeat(60));
        System.out.println();
        System.out.println(" Multiple relay instances would share buckets via tandem_bucket_lease;");
        System.out.println(" this single instance still uses the same table, so the Admin API's");
        System.out.println(" per-bucket/per-worker endpoints have real data to answer from.");
        System.out.println();
    }
}
