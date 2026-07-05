package com.codingful.tandem.benchmark;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.sql.DataSource;

/**
 * Direct-SQL lag observation over {@code tandem_outbox} (HLD-load-testing.md §2.1; LLD-benchmark §6,
 * §6.1): the product exposes no per-shard lag metric and no caller ever populates
 * {@code TandemMetrics.recordLagAgeSeconds}, so the harness measures the pending backlog itself —
 * overall (feeds {@link RampController}) and per-bucket (S3).
 */
public final class LagProbe {

    // status: 0=PENDING, 1=IN_FLIGHT, 3=FAILED — "not yet DONE" (LLD-jdbc schema comment).
    private static final String OVERALL_SQL =
            "SELECT count(*) AS pending, "
                    + "coalesce(extract(epoch FROM now() - min(created_at)), 0) AS oldest_age_seconds "
                    + "FROM tandem_outbox WHERE status IN (0, 1, 3)";

    // PENDING/IN_FLIGHT only — excludes FAILED, which is terminal and will never drain on its own
    // (LLD-jdbc §3.4.2): a permanently failed row must not stall waitForDrain — it should surface
    // promptly as a missing key in the correctness check instead. Scoped to one scenario's own
    // aggregate-id namespace (LLD-benchmark §8, AggregateSelector):
    // when several scenarios share one tandem_outbox table (SmokeLoadTest), another scenario's
    // permanently-stuck poisoned backlog (S6) must not make THIS scenario's drain wait hang forever.
    private static final String IN_PROGRESS_FOR_NAMESPACE_SQL =
            "SELECT count(*) AS pending, "
                    + "coalesce(extract(epoch FROM now() - min(created_at)), 0) AS oldest_age_seconds "
                    + "FROM tandem_outbox WHERE status IN (0, 1) AND aggregate_id LIKE ?";

    private static final String PER_BUCKET_SQL =
            "SELECT bucket, count(*) AS pending, "
                    + "coalesce(extract(epoch FROM now() - min(created_at)), 0) AS oldest_age_seconds "
                    + "FROM tandem_outbox WHERE status IN (0, 1, 3) GROUP BY bucket ORDER BY bucket";

    private static final String PENDING_EXCLUDING_SQL =
            "SELECT count(*) FROM tandem_outbox WHERE status IN (0, 1) AND aggregate_id <> ? AND aggregate_id LIKE ?";

    private static final String HAS_FAILED_ROW_SQL =
            "SELECT EXISTS(SELECT 1 FROM tandem_outbox WHERE aggregate_id = ? AND status = 3)";

    private final DataSource dataSource;

    public LagProbe(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public record Lag(long pending, Duration age) {
    }

    /** Global pending count + age of the oldest pending row, across all buckets. */
    public Lag overall() {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(OVERALL_SQL);
             ResultSet rs = ps.executeQuery()) {
            rs.next();
            return toLag(rs);
        } catch (SQLException e) {
            throw new IllegalStateException("lag probe (overall) failed", e);
        }
    }

    /**
     * Pending + in-flight only (excludes {@code FAILED}), scoped to one scenario's {@code namespace}
     * (its aggregate-id prefix) — what {@code waitForDrain} polls. Scoping matters because several
     * scenarios can share one {@code tandem_outbox} table (SmokeLoadTest): another scenario's
     * permanently-stuck poisoned backlog (S6) must not count as "not yet drained" here.
     */
    public Lag inProgressForNamespace(String namespace) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(IN_PROGRESS_FOR_NAMESPACE_SQL)) {
            ps.setString(1, namespace + "-%");
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return toLag(rs);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("lag probe (in-progress-for-namespace) failed", e);
        }
    }

    /** Per-bucket pending count + oldest-row age; only buckets with pending rows appear. */
    public Map<Integer, Lag> perBucket() {
        Map<Integer, Lag> result = new LinkedHashMap<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(PER_BUCKET_SQL);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                result.put(rs.getInt("bucket"), toLag(rs));
            }
        } catch (SQLException e) {
            throw new IllegalStateException("lag probe (per-bucket) failed", e);
        }
        return result;
    }

    /**
     * Pending (PENDING/IN_FLIGHT) rows in {@code namespace} belonging to any aggregate other than
     * {@code aggregateId} (S6: "did everyone else in my own scenario drain?"). Scoped to
     * {@code namespace} for the same reason as {@link #inProgressForNamespace}: another scenario's
     * own stuck backlog must not count as "not yet drained" here.
     */
    public long pendingExcludingAggregate(String namespace, String aggregateId) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(PENDING_EXCLUDING_SQL)) {
            ps.setString(1, aggregateId);
            ps.setString(2, namespace + "-%");
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("lag probe (pendingExcludingAggregate) failed", e);
        }
    }

    /** Whether {@code aggregateId} has at least one {@code FAILED} row (S6: confirms the poison gate tripped). */
    public boolean hasFailedRow(String aggregateId) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(HAS_FAILED_ROW_SQL)) {
            ps.setString(1, aggregateId);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getBoolean(1);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("lag probe (hasFailedRow) failed", e);
        }
    }

    private static Lag toLag(ResultSet rs) throws SQLException {
        long millis = (long) (rs.getDouble("oldest_age_seconds") * 1000);
        return new Lag(rs.getLong("pending"), Duration.ofMillis(millis));
    }
}
