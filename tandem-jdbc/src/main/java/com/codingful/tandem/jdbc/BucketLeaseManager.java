package com.codingful.tandem.jdbc;

import com.codingful.tandem.core.exception.TandemConfigurationException;
import com.codingful.tandem.core.exception.TandemException;
import com.codingful.tandem.core.port.TandemMetrics;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import javax.sql.DataSource;

/**
 * {@code LEASE}-mode bucket assignment via the {@code tandem_bucket_lease} table (LLD-jdbc §3.2): a
 * decentralized greedy + lease scheme that converges to a fair, self-healing split with no central
 * coordinator. Each {@link #heartbeat()} registers this instance's presence, renews its bucket leases,
 * recomputes its fair share {@code ceil(B / live_members)}, and releases excess or claims free/expired
 * buckets accordingly. A dead instance's leases simply expire and are reclaimed by the survivors.
 *
 * <p><b>Presence is decoupled from ownership</b> (via {@code tandem_relay_member}): the fair-share
 * divisor counts <i>live members</i>, not bucket owners. An instance that currently owns zero buckets
 * has no {@code tandem_bucket_lease} row and would otherwise be invisible to peers — so an incumbent
 * holding every bucket would never learn a newcomer exists and never release its share (a stable
 * scale-up starvation). Self-registration makes a zero-owned joiner visible, so the incumbent releases
 * and the fleet rebalances (LLD-jdbc §3.2).
 */
public final class BucketLeaseManager implements BucketSource {

    /** Metric/log identifier for the {@code LEASE} lease-table precondition (LLD-jdbc §3.2/§3.5). */
    public static final String CHECK_BUCKET_LEASE_SEEDED = "bucket_lease_not_seeded";

    private static final String COUNT_SQL = "SELECT count(*) FROM tandem_bucket_lease";

    // Presence probe for validateOnStart — verifies the membership table exists (fails otherwise).
    private static final String MEMBER_PROBE_SQL = "SELECT 1 FROM tandem_relay_member WHERE false";

    // Register/renew this instance's presence, decoupled from bucket ownership (§3.2).
    private static final String REGISTER_MEMBER_SQL =
            "INSERT INTO tandem_relay_member (owner, lease_until, updated_at)"
                    + " VALUES (?, now() + (? * interval '1 millisecond'), now())"
                    + " ON CONFLICT (owner) DO UPDATE SET lease_until = excluded.lease_until, updated_at = now()";

    // Drop members whose presence lease has expired (dead instances), keeping the table bounded.
    private static final String PRUNE_MEMBERS_SQL =
            "DELETE FROM tandem_relay_member WHERE lease_until < now()";

    // Fair-share divisor: live relay instances (includes self, just registered).
    private static final String LIVE_MEMBERS_SQL =
            "SELECT count(*) FROM tandem_relay_member WHERE lease_until > now()";

    private static final String DELETE_MEMBER_SQL =
            "DELETE FROM tandem_relay_member WHERE owner = ?";

    private static final String RENEW_SQL =
            "UPDATE tandem_bucket_lease SET lease_until = now() + (? * interval '1 millisecond'), updated_at = now()"
                    + " WHERE owner = ?";

    private static final String OWNED_COUNT_SQL =
            "SELECT count(*) FROM tandem_bucket_lease WHERE owner = ? AND lease_until > now()";

    private static final String OWNED_BUCKETS_SQL =
            "SELECT bucket FROM tandem_bucket_lease WHERE owner = ? AND lease_until > now()";

    private static final String RELEASE_EXCESS_SQL =
            "UPDATE tandem_bucket_lease SET owner = NULL, lease_until = NULL, updated_at = now()"
                    + " WHERE bucket IN (SELECT bucket FROM tandem_bucket_lease WHERE owner = ? ORDER BY bucket DESC LIMIT ?)";

    private static final String CLAIM_DEFICIT_SQL =
            "UPDATE tandem_bucket_lease SET owner = ?, lease_until = now() + (? * interval '1 millisecond'), updated_at = now()"
                    + " WHERE bucket IN (SELECT bucket FROM tandem_bucket_lease"
                    + "                   WHERE owner IS NULL OR lease_until < now()"
                    + "                   ORDER BY bucket LIMIT ? FOR UPDATE SKIP LOCKED)";

    private static final String RELEASE_ALL_SQL =
            "UPDATE tandem_bucket_lease SET owner = NULL, lease_until = NULL, updated_at = now() WHERE owner = ?";

    private final DataSource dataSource;
    private final String ownerId;
    private final int bucketCount;
    private final long leaseMillis;

    /**
     * @param dataSource  the {@code tandem_bucket_lease} table's connection source
     * @param ownerId     this instance's unique identifier, written to the {@code owner} column
     * @param bucketCount the fixed total bucket count shared by every instance (must match {@link RelayConfig#bucketCount()})
     * @param lease       how long a renewed lease is held before it is eligible for reclaim
     * @throws IllegalArgumentException if {@code ownerId} exceeds 64 chars or {@code bucketCount <= 0}
     */
    public BucketLeaseManager(DataSource dataSource, String ownerId, int bucketCount, Duration lease) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.ownerId = Objects.requireNonNull(ownerId, "ownerId");
        if (ownerId.length() > 64) {
            throw new IllegalArgumentException("ownerId exceeds the tandem_bucket_lease.owner length (64)");
        }
        if (bucketCount <= 0) {
            throw new IllegalArgumentException("bucketCount must be positive");
        }
        this.bucketCount = bucketCount;
        this.leaseMillis = lease.toMillis();
    }

    /**
     * Fail-fast if the {@code tandem_bucket_lease} table is absent or not seeded to exactly
     * {@code bucketCount} rows — the common "enabled LEASE but only applied the {@code tandem_outbox}
     * DDL" (or a mismatched {@code B}) misconfiguration. Records {@link #CHECK_BUCKET_LEASE_SEEDED},
     * logs one ERROR line, then throws {@link TandemConfigurationException}.
     */
    @Override
    public void validateOnStart(TandemMetrics metrics, Logger logger) {
        int seeded;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(COUNT_SQL);
             ResultSet rs = ps.executeQuery()) {
            rs.next();
            seeded = rs.getInt(1);
        } catch (SQLException e) {
            failLeaseTable(metrics, logger, "the tandem_bucket_lease table is missing or unreadable ("
                    + e.getMessage() + ")", e);
            return;   // unreachable — failLeaseTable always throws
        }
        if (seeded != bucketCount) {
            failLeaseTable(metrics, logger, "tandem_bucket_lease is seeded with " + seeded
                    + " rows but coordination=LEASE requires exactly bucketCount=" + bucketCount, null);
        }
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(MEMBER_PROBE_SQL)) {
            ps.executeQuery().close();
        } catch (SQLException e) {
            failLeaseTable(metrics, logger, "the tandem_relay_member table is missing or unreadable ("
                    + e.getMessage() + ")", e);
        }
    }

    /**
     * @param cause the caught exception that led to this failure, or {@code null} when the failure was
     *              detected without one (e.g. a row-count mismatch) — always passed to the logger when present
     *              so the stack trace is not lost (HLD-logging.md §4).
     */
    private void failLeaseTable(TandemMetrics metrics, Logger logger, String detail, SQLException cause) {
        String message = "Unsafe relay config: " + detail
                + ". Apply the LEASE baseline DDL (schema/postgres/tandem-baseline.sql seeds "
                + "tandem_bucket_lease with B rows) and ensure bucketCount matches every instance, "
                + "or set coordination=SINGLE for a single-instance relay.";
        metrics.recordConfigInvalid(CHECK_BUCKET_LEASE_SEEDED);   // best-effort, before aborting
        String logMessage = message + " bucketCount:" + bucketCount + ", owner:" + ownerId;
        if (cause != null) {
            logger.log(Level.ERROR, logMessage, cause);
        } else {
            logger.log(Level.ERROR, logMessage);
        }
        throw new TandemConfigurationException(message);
    }

    @Override
    public Set<Integer> ownedBuckets() {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(OWNED_BUCKETS_SQL)) {
            ps.setString(1, ownerId);
            Set<Integer> owned = new HashSet<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    owned.add(rs.getInt(1));
                }
            }
            return owned;
        } catch (SQLException e) {
            throw new TandemException("reading owned buckets failed", e);
        }
    }

    @Override
    public void heartbeat() {
        try (Connection conn = dataSource.getConnection()) {
            registerMember(conn);          // make this instance visible before counting (§3.2)
            pruneExpiredMembers(conn);     // drop dead instances so the divisor is accurate
            renew(conn);
            int liveMembers = countLiveMembers(conn);
            int target = (int) Math.ceil((double) bucketCount / liveMembers);
            int owned = ownedCount(conn);
            if (owned > target) {
                releaseExcess(conn, owned - target);
            } else if (owned < target) {
                claimDeficit(conn, target - owned);
            }
        } catch (SQLException e) {
            throw new TandemException("bucket lease heartbeat failed", e);
        }
    }

    @Override
    public void release() {
        try (Connection conn = dataSource.getConnection()) {
            releaseOwnedBuckets(conn);
            deleteMember(conn);   // drop presence immediately so peers rebalance without waiting for expiry
        } catch (SQLException e) {
            throw new TandemException("releasing bucket leases failed", e);
        }
    }

    private void renew(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(RENEW_SQL)) {
            ps.setLong(1, leaseMillis);
            ps.setString(2, ownerId);
            ps.executeUpdate();
        }
    }

    private void registerMember(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(REGISTER_MEMBER_SQL)) {
            ps.setString(1, ownerId);
            ps.setLong(2, leaseMillis);
            ps.executeUpdate();
        }
    }

    private void pruneExpiredMembers(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(PRUNE_MEMBERS_SQL)) {
            ps.executeUpdate();
        }
    }

    private void deleteMember(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(DELETE_MEMBER_SQL)) {
            ps.setString(1, ownerId);
            ps.executeUpdate();
        }
    }

    /**
     * Live relay instances — the fair-share divisor. Counts members (presence), not bucket owners, so a
     * zero-owned joiner is still counted (it just registered). At least 1 (self, LLD-jdbc §3.2).
     */
    private int countLiveMembers(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(LIVE_MEMBERS_SQL);
             ResultSet rs = ps.executeQuery()) {
            rs.next();
            return Math.max(1, rs.getInt(1));
        }
    }

    private int ownedCount(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(OWNED_COUNT_SQL)) {
            ps.setString(1, ownerId);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getInt(1);
            }
        }
    }

    private void releaseOwnedBuckets(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(RELEASE_ALL_SQL)) {
            ps.setString(1, ownerId);
            ps.executeUpdate();
        }
    }

    private void releaseExcess(Connection conn, int excess) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(RELEASE_EXCESS_SQL)) {
            ps.setString(1, ownerId);
            ps.setInt(2, excess);
            ps.executeUpdate();
        }
    }

    private void claimDeficit(Connection conn, int deficit) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(CLAIM_DEFICIT_SQL)) {
            ps.setString(1, ownerId);
            ps.setLong(2, leaseMillis);
            ps.setInt(3, deficit);
            ps.executeUpdate();
        }
    }
}
