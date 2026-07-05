package com.codingful.tandem.jdbc;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Logger;
import javax.sql.DataSource;

/**
 * A minimal {@link DataSource} over {@link DriverManager} for tests — each {@code getConnection()}
 * opens a fresh autocommit connection, matching how the JDBC adapter uses a short connection per
 * operation. Not a pool; tests close what they open.
 */
final class SimpleDataSource implements DataSource {

    private final String url;
    private final String user;
    private final String password;

    SimpleDataSource(String url, String user, String password) {
        this.url = url;
        this.user = user;
        this.password = password;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }

    @Override
    public Connection getConnection(String username, String pwd) throws SQLException {
        return DriverManager.getConnection(url, username, pwd);
    }

    @Override
    public PrintWriter getLogWriter() {
        return null;
    }

    @Override
    public void setLogWriter(PrintWriter out) {
    }

    @Override
    public void setLoginTimeout(int seconds) {
    }

    @Override
    public int getLoginTimeout() {
        return 0;
    }

    @Override
    public Logger getParentLogger() {
        return Logger.getLogger("tandem-test-datasource");
    }

    @Override
    public <T> T unwrap(Class<T> iface) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return false;
    }
}
