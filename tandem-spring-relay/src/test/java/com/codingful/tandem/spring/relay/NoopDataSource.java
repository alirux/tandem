package com.codingful.tandem.spring.relay;

import java.sql.Connection;
import org.springframework.jdbc.datasource.AbstractDataSource;

/**
 * A real {@link javax.sql.DataSource} that is never connected to — used by the conditional wiring tests,
 * where a {@code DataSource} bean must merely be <em>present</em> but the relay backs off (disabled) so
 * no connection is opened. Opening one is a test bug, so it fails loudly.
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
