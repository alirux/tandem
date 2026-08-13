package com.codingful.tandem.admin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/**
 * Pins what this source set <i>is</i>, so every other test in it means what it claims. The module's
 * Boot 4 defect shipped because the only Boot 4 task on the build put Jackson 2 on the classpath by
 * hand — a classpath no stock application has. If that ever happened here, these tests would go on
 * passing while proving nothing, so the classpath itself is asserted rather than assumed.
 */
class StockBootFourClasspathTest {

    @Test
    void GIVEN_this_source_set_WHEN_jackson_two_is_looked_up_THEN_it_is_absent_as_it_is_in_a_stock_application() {
        assertThatThrownBy(() -> Class.forName("com.fasterxml.jackson.databind.ObjectMapper"))
                .isInstanceOf(ClassNotFoundException.class);
    }

    @Test
    void GIVEN_this_source_set_WHEN_the_json_binding_is_looked_up_THEN_it_is_jackson_three() throws Exception {
        assertThat(Class.forName("tools.jackson.databind.ObjectMapper")).isNotNull();
    }

    @Test
    void GIVEN_this_source_set_WHEN_the_shared_annotations_are_looked_up_THEN_they_are_present() throws Exception {
        // The one Jackson 2 artifact a stock Boot 4 application still carries, and the only one the
        // module compiles against — the whole design rests on it being there.
        assertThat(Class.forName("com.fasterxml.jackson.annotation.JsonRawValue")).isNotNull();
    }
}
