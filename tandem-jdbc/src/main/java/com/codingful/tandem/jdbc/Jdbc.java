package com.codingful.tandem.jdbc;

import com.codingful.tandem.core.exception.TandemException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Function;
import javax.sql.DataSource;

/**
 * Opens a connection, runs a JDBC operation against it, and closes it — the one place in the adapter
 * layer that translates a checked {@link SQLException} into an unchecked one. Every {@code Jdbc*}
 * class in this package routes its database calls through {@link #run}, so the translation invariant
 * (a raw {@code SQLException} never escapes an adapter method) is proven by one test class
 * ({@code JdbcTest}) instead of once per call site (backlog item 30).
 *
 * <p>{@link #withStatement} and {@link #withResultSet} are the same idea one level down — a
 * <i>loan pattern</i> for the {@code PreparedStatement}/{@code ResultSet} steps of the chain. Nobody
 * outside this class ever writes {@code conn.prepareStatement(...)} or {@code ps.executeQuery()}
 * directly, so there is no call site where a future edit can forget the enclosing
 * {@code try (...)} — the closing is inside the one implementation of each helper, not repeated at
 * every use. `-Xlint:all` (this project's only static check) has no "unclosed resource" category, so
 * this is a structural guarantee, not one the compiler would otherwise catch.
 */
final class Jdbc {

    // run/withStatement/withResultSet are T-returning ONLY — the void counterpart of each is a
    // DIFFERENT name (exec, one overload per resource type) rather than an overload of the same name.
    // A same-named void/T-returning pair looked harmless at first (a block lambda with an explicit
    // `return` seemed obviously T-only) but isn't: a single-EXPRESSION lambda body that is itself a
    // method call — `conn -> Jdbc.withStatement(...)`, exactly what nesting these helpers produces —
    // is "void-compatible" AND "value-compatible" at once per JLS §15.27.3, so javac cannot pick an
    // overload and nesting fails to compile. Different names sidestep the rule instead of fighting it.

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
     * A JDBC step from some already-open resource {@code A} to a result — the generic counterpart of
     * {@link SqlOperation}, used where the input isn't always a {@link Connection} (a
     * {@link PreparedStatement} for {@link #withStatement}, a {@link ResultSet} for
     * {@link #withResultSet}).
     */
    @FunctionalInterface
    interface SqlFunction<A, R> {
        R apply(A a) throws SQLException;
    }

    /** A JDBC step with no result — the {@code void} counterpart of {@link SqlFunction}. */
    @FunctionalInterface
    interface SqlConsumer<A> {
        void accept(A a) throws SQLException;
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
    static void exec(DataSource dataSource, String failureMessage, SqlAction action) {
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
    static void exec(DataSource dataSource, SqlAction action, Function<SQLException, RuntimeException> exceptionMapper) {
        run(dataSource, toOperation(action), exceptionMapper);
    }

    private static SqlOperation<Void> toOperation(SqlAction action) {
        return connection -> {
            action.run(connection);
            return null;
        };
    }

    /** Runs {@code body} against a statement prepared from {@code sql} on {@code connection}, then closes it. */
    static <R> R withStatement(Connection connection, String sql, SqlFunction<PreparedStatement, R> body) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            return body.apply(statement);
        }
    }

    /** {@link #withStatement(Connection, String, SqlFunction)} for a step with no result. */
    static void exec(Connection connection, String sql, SqlConsumer<PreparedStatement> body) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            body.accept(statement);
        }
    }

    /** Runs {@code body} against the result set produced by executing {@code statement}, then closes it. */
    static <R> R withResultSet(PreparedStatement statement, SqlFunction<ResultSet, R> body) throws SQLException {
        try (ResultSet resultSet = statement.executeQuery()) {
            return body.apply(resultSet);
        }
    }

    /** {@link #withResultSet(PreparedStatement, SqlFunction)} for a step with no result. */
    static void exec(PreparedStatement statement, SqlConsumer<ResultSet> body) throws SQLException {
        try (ResultSet resultSet = statement.executeQuery()) {
            body.accept(resultSet);
        }
    }
}
