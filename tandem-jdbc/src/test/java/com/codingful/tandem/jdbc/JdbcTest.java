package com.codingful.tandem.jdbc;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.codingful.tandem.core.exception.TandemException;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;

/**
 * Pins the one behaviour every {@code Jdbc*} adapter in this package depends on {@link Jdbc#run}/
 * {@link Jdbc#exec(DataSource, String, Jdbc.SqlAction)} for: a {@link SQLException} — from acquiring
 * the connection, or from the operation itself, both reach the same {@code catch} — never escapes an
 * adapter method raw. Needs no Docker: {@code getConnection()} against an unroutable address fails
 * fast, which is enough to exercise the wrapping without a real database (the pattern every
 * {@code Jdbc*IT} used to repeat once per method — item 30).
 *
 * <p>{@link Jdbc#withStatement} / {@link Jdbc#withResultSet} (and their {@code exec} void
 * counterparts) are deliberately not re-pinned here: they only wrap a
 * {@code try (PreparedStatement/ResultSet ...)}, and proving that closes correctly would mean testing
 * this class's own reimplementation of a guarantee {@code try-with-resources} already gives for free —
 * every {@code Jdbc*IT} already exercises them through real SQL, which is the same proof the plain
 * nested {@code try} blocks they replaced had.
 */
class JdbcTest {

    // Port 1 is a reserved, never-listening port, so getConnection() fails immediately with no
    // network timeout — the same address every existing Jdbc*IT "unreachable database" test used.
    private final SimpleDataSource unreachable = new SimpleDataSource("jdbc:postgresql://127.0.0.1:1/none", "none", "none");

    @Test
    void GIVEN_the_database_is_unreachable_WHEN_an_operation_runs_THEN_the_sqlexception_is_wrapped_as_a_tandem_exception() {
        assertThatThrownBy(() -> Jdbc.run(unreachable, "the operation", (Jdbc.SqlOperation<Object>) conn -> conn))
                .isInstanceOf(TandemException.class)
                .hasMessage("the operation")
                .hasCauseInstanceOf(SQLException.class);
    }

    @Test
    void GIVEN_the_database_is_unreachable_WHEN_a_void_operation_runs_THEN_the_sqlexception_is_wrapped_as_a_tandem_exception() {
        assertThatThrownBy(() -> Jdbc.exec(unreachable, "the void operation", (Jdbc.SqlAction) conn -> { }))
                .isInstanceOf(TandemException.class)
                .hasMessage("the void operation")
                .hasCauseInstanceOf(SQLException.class);
    }

    @Test
    void GIVEN_a_custom_exception_mapper_WHEN_the_operation_fails_THEN_the_mapper_translates_it_instead_of_the_default_wrap() {
        RuntimeException marker = new IllegalStateException("translated");

        assertThatThrownBy(() -> Jdbc.run(unreachable,
                (Jdbc.SqlOperation<Object>) conn -> conn,
                e -> marker))
                .isSameAs(marker);
    }

    @Test
    void GIVEN_a_custom_exception_mapper_WHEN_a_void_operation_fails_THEN_the_mapper_translates_it_instead_of_the_default_wrap() {
        RuntimeException marker = new IllegalStateException("translated");

        assertThatThrownBy(() -> Jdbc.exec(unreachable, (Jdbc.SqlAction) conn -> { }, e -> marker))
                .isSameAs(marker);
    }

    // The success path (operation runs, its result is returned unchanged) is not re-pinned here: every
    // Jdbc*IT already exercises it through real SQL, and duplicating it against a hand-rolled fake
    // Connection would only restate that wiring, not add a case that could fail under mutation
    // (AGENTS Testing §3).
}
