package com.codingful.tandem.jdbc;

import com.codingful.tandem.core.port.DiscardService;
import java.util.Objects;
import javax.sql.DataSource;

/**
 * Marks a {@code FAILED} row {@code DISCARDED} (HLD-admin-api §4, {@link DiscardService}). A single
 * conditional {@code UPDATE} — guarded by {@code status = 3} in the {@code WHERE} clause — so the
 * precondition check and the transition happen atomically, with no read-then-write race between an
 * admin request and a concurrent relay/operator action on the same row.
 */
public final class JdbcDiscardService implements DiscardService {

    private static final String DISCARD_SQL =
            "UPDATE tandem_outbox SET status = 4, discard_reason = ? WHERE id = ? AND status = 3";

    private final DataSource dataSource;

    /** @param dataSource used for the discard UPDATE */
    public JdbcDiscardService(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    @Override
    public boolean discard(long id, String reason) {
        return Jdbc.run(dataSource, "discard failed for id " + id, conn ->
                Jdbc.withStatement(conn, DISCARD_SQL, ps -> {
                    ps.setString(1, reason);
                    ps.setLong(2, id);
                    return ps.executeUpdate() > 0;
                }));
    }
}
