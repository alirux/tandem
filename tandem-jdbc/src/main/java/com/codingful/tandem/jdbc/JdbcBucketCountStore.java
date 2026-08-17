package com.codingful.tandem.jdbc;

import com.codingful.tandem.core.exception.TandemException;
import com.codingful.tandem.core.port.BucketCountStore;
import java.util.Objects;
import java.util.OptionalInt;
import javax.sql.DataSource;

/**
 * PostgreSQL adapter for {@link BucketCountStore} (LLD-bucket-count-guard §4), backed by the
 * {@code tandem_meta} table under the key {@code bucket_count}. Each call runs in its own short
 * transaction (autocommit), like {@link JdbcOutboxStore}. Depends only on {@code java.sql}.
 */
public final class JdbcBucketCountStore implements BucketCountStore {

    /** The {@code tandem_meta} key holding the effective bucket count. */
    static final String BUCKET_COUNT_KEY = "bucket_count";

    private static final String READ_SQL =
            "SELECT value FROM tandem_meta WHERE key = '" + BUCKET_COUNT_KEY + "'";

    // Atomic seed-if-absent: insert only when no row exists; ON CONFLICT DO NOTHING makes a concurrent
    // first-init collapse to a single stored value. The follow-up read returns whoever won.
    private static final String SEED_SQL =
            "INSERT INTO tandem_meta (key, value) VALUES ('" + BUCKET_COUNT_KEY + "', ?)"
                    + " ON CONFLICT (key) DO NOTHING";

    private final DataSource dataSource;

    public JdbcBucketCountStore(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    @Override
    public OptionalInt read() {
        return Jdbc.run(dataSource, "Reading tandem_meta bucket_count failed", conn ->
                Jdbc.withStatement(conn, READ_SQL, ps -> Jdbc.withResultSet(ps, rs ->
                        rs.next() ? OptionalInt.of(parse(rs.getString(1))) : OptionalInt.empty())));
    }

    @Override
    public int seedIfAbsent(int candidate) {
        if (candidate <= 0) {
            throw new IllegalArgumentException("candidate bucketCount must be positive: " + candidate);
        }
        Jdbc.exec(dataSource, "Seeding tandem_meta bucket_count failed", conn ->
                Jdbc.exec(conn, SEED_SQL, ps -> {
                    ps.setString(1, Integer.toString(candidate));
                    ps.executeUpdate();   // 0 rows if a value was already present — that's the expected race loser
                }));
        // Re-read the now-effective value (this call's candidate, or the winner's if we lost the race).
        return read().orElseThrow(() ->
                new TandemException("tandem_meta bucket_count missing immediately after seed"));
    }

    private static int parse(String value) {
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            throw new TandemException("tandem_meta bucket_count is not an integer: '" + value + "'", e);
        }
    }
}
