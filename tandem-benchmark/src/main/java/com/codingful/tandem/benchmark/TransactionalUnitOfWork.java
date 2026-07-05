package com.codingful.tandem.benchmark;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;
import javax.sql.DataSource;

/**
 * Binds one physical connection per thread for the duration of {@link #runInTransaction} and hands
 * back a {@link #transactionAware()} view whose {@code getConnection()} returns that bound
 * connection with a no-op {@code close()} — the minimal transaction-aware {@link DataSource}
 * {@code JdbcOutboxRepository}'s javadoc assumes (LLD-jdbc §2), reimplemented here without pulling in
 * Spring. {@link LoadGenerator} needs this so the real {@code repository.insert} joins the same
 * transaction as the domain {@code SELECT ... FOR UPDATE} (LLD-benchmark §4.1).
 */
final class TransactionalUnitOfWork {

    @FunctionalInterface
    interface SqlWork<T> {
        T run(Connection conn) throws SQLException;
    }

    private final DataSource delegate;
    private final ThreadLocal<Connection> bound = new ThreadLocal<>();
    private final DataSource transactionAwareView;

    TransactionalUnitOfWork(DataSource delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.transactionAwareView = noCloseProxyDataSource();
    }

    /** Given to {@code JdbcOutboxRepository} — valid only while called from inside {@link #runInTransaction}. */
    DataSource transactionAware() {
        return transactionAwareView;
    }

    /** Opens one connection, binds it for this thread, runs {@code work}, commits, then unbinds and closes. */
    <T> T runInTransaction(SqlWork<T> work) throws SQLException {
        Connection conn = delegate.getConnection();
        conn.setAutoCommit(false);
        bound.set(conn);
        try {
            T result = work.run(conn);
            conn.commit();
            return result;
        } catch (SQLException | RuntimeException e) {
            safeRollback(conn);
            throw e;
        } finally {
            bound.remove();
            closeQuietly(conn);
        }
    }

    private DataSource noCloseProxyDataSource() {
        return (DataSource) Proxy.newProxyInstance(
                DataSource.class.getClassLoader(),
                new Class<?>[] {DataSource.class},
                (proxy, method, args) -> {
                    if ("getConnection".equals(method.getName()) && method.getParameterCount() == 0) {
                        Connection conn = bound.get();
                        if (conn == null) {
                            throw new IllegalStateException("no transaction bound on this thread");
                        }
                        return noCloseConnectionProxy(conn);
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Connection noCloseConnectionProxy(Connection target) {
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[] {Connection.class},
                (proxy, method, args) -> {
                    if ("close".equals(method.getName())) {
                        return null;
                    }
                    try {
                        return method.invoke(target, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    private static void safeRollback(Connection conn) {
        try {
            conn.rollback();
        } catch (SQLException ignored) {
            // best-effort — the connection is discarded either way
        }
    }

    private static void closeQuietly(Connection conn) {
        try {
            conn.setAutoCommit(true);
            conn.close();
        } catch (SQLException ignored) {
            // best-effort
        }
    }
}
