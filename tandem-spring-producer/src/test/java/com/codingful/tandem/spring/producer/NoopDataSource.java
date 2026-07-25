package com.codingful.tandem.spring.producer;

import java.sql.Connection;
import org.springframework.jdbc.datasource.AbstractDataSource;

/**
 * A real {@link javax.sql.DataSource} that is never connected to — used by the wiring tests, where a
 * {@code DataSource} bean must merely be <em>present</em> for the autoconfiguration to apply but no
 * connection is ever opened (a user-supplied {@code InMemoryOutbox} replaces the JDBC write-side, so the
 * bucket-count guard never runs). Extends Spring's {@link AbstractDataSource} so only the connection
 * methods need a body; opening a connection is a test bug, so it fails loudly rather than returning one.
 */
final class NoopDataSource extends AbstractDataSource {

    @Override
    public Connection getConnection() {
        throw new UnsupportedOperationException("NoopDataSource must not be connected to in a wiring test");
    }

    @Override
    public Connection getConnection(String username, String password) {
        return getConnection();
    }
}
