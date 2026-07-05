package com.codingful.tandem.jdbc;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Base for the JDBC integration tests: a single shared PostgreSQL container (Testcontainers) with the
 * committed baseline DDL applied, reset before each test. Tagged {@code integration} so it runs under
 * {@code integrationTest} (needs Docker), not the Docker-free {@code test} task.
 */
@Tag("integration")
abstract class AbstractPostgresIT {

    // Singleton container pattern: started once for the whole test run, reused across IT classes,
    // torn down by the Testcontainers reaper at JVM exit.
    private static final PostgreSQLContainer<?> POSTGRES;

    static final DataSource DATA_SOURCE;

    static {
        POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");
        POSTGRES.start();
        DATA_SOURCE = new SimpleDataSource(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
        applyBaselineSchema(DATA_SOURCE);
    }

    @BeforeEach
    void resetTables() {
        execute("TRUNCATE tandem_outbox RESTART IDENTITY",
                "UPDATE tandem_bucket_lease SET owner = NULL, lease_until = NULL",
                "TRUNCATE tandem_relay_member");
    }

    static void execute(String... statements) {
        try (Connection conn = DATA_SOURCE.getConnection();
             Statement stmt = conn.createStatement()) {
            for (String sql : statements) {
                stmt.execute(sql);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("test SQL failed", e);
        }
    }

    private static void applyBaselineSchema(DataSource dataSource) {
        String ddl = readBaseline();
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(ddl);   // the PostgreSQL driver runs the multi-statement script in one call
        } catch (SQLException e) {
            throw new IllegalStateException("applying baseline schema failed", e);
        }
    }

    /** Locate {@code schema/postgres/tandem-baseline.sql} by walking up from the working directory. */
    private static String readBaseline() {
        Path dir = Path.of("").toAbsolutePath();
        while (dir != null) {
            Path candidate = dir.resolve("schema/postgres/tandem-baseline.sql");
            if (Files.exists(candidate)) {
                try {
                    return Files.readString(candidate, StandardCharsets.UTF_8);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            dir = dir.getParent();
        }
        throw new IllegalStateException("could not locate schema/postgres/tandem-baseline.sql");
    }
}
