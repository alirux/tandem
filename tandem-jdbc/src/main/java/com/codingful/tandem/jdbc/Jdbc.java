package com.codingful.tandem.jdbc;

import com.codingful.tandem.core.exception.TandemException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.Function;
import javax.sql.DataSource;

/**
 * Opens a connection, runs a JDBC operation against it, and closes it — the one place in the adapter
 * layer that translates a checked {@link SQLException} into an unchecked one. Every {@code Jdbc*}
 * class in this package routes its database calls through {@link #run}, so the translation invariant
 * (a raw {@code SQLException} never escapes an adapter method) is proven by one test class
 * ({@code JdbcTest}) instead of once per call site (backlog item 30).
 */
final class Jdbc {

    // javac warns that the SqlOperation<T> and SqlAction overloads below are "potentially ambiguous"
    // (same parameter shape, differing only in the functional interface's return type). Every call
    // site in this package resolves deterministically — a block lambda with no return statement is
    // void-compatible only, so it can never match SqlOperation<T> — so the warning is inherent to the
    // design, not a real ambiguity; kept because the alternative (a single SqlOperation<T> and a
    // `return null;` at every void call site) is worse boilerplate than it avoids.

    private Jdbc() {
    }

    /** A JDBC operation that produces a result and may fail with a checked {@link SQLException}. */
    @FunctionalInterface
    interface SqlOperation<T> {
        T run(Connection connection) throws SQLException;
    }

    /** A JDBC operation with no result — the {@code void} counterpart of {@link SqlOperation}. */
    @FunctionalInterface
    interface SqlAction {
        void run(Connection connection) throws SQLException;
    }

    /**
     * Runs {@code operation} against a connection acquired from {@code dataSource}, wrapping any
     * {@code SQLException} — from acquiring the connection or from {@code operation} itself — as a
     * {@link TandemException} carrying {@code failureMessage}.
     */
    static <T> T run(DataSource dataSource, String failureMessage, SqlOperation<T> operation) {
        return run(dataSource, operation, e -> new TandemException(failureMessage, e));
    }

    /** {@link #run(DataSource, String, SqlOperation)} for an operation with no result. */
    static void run(DataSource dataSource, String failureMessage, SqlAction action) {
        run(dataSource, toOperation(action), e -> new TandemException(failureMessage, e));
    }

    /**
     * Runs {@code operation}, translating any {@code SQLException} with {@code exceptionMapper}
     * instead of the default {@link TandemException} wrap — for adapters that distinguish failure
     * causes (e.g. {@link JdbcOutboxRepository} on a unique-violation SQLSTATE, or
     * {@link BucketLeaseManager} on a missing coordination table).
     */
    static <T> T run(DataSource dataSource, SqlOperation<T> operation,
            Function<SQLException, RuntimeException> exceptionMapper) {
        try (Connection connection = dataSource.getConnection()) {
            return operation.run(connection);
        } catch (SQLException e) {
            throw exceptionMapper.apply(e);
        }
    }

    /** {@link #run(DataSource, SqlOperation, Function)} for an operation with no result. */
    static void run(DataSource dataSource, SqlAction action, Function<SQLException, RuntimeException> exceptionMapper) {
        run(dataSource, toOperation(action), exceptionMapper);
    }

    private static SqlOperation<Void> toOperation(SqlAction action) {
        return connection -> {
            action.run(connection);
            return null;
        };
    }
}
