package com.codingful.tandem.spring.relay;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import org.junit.jupiter.api.Test;

class TandemKafkaPropertiesTest {

    @Test
    void GIVEN_no_producer_properties_WHEN_bound_THEN_the_producer_map_is_empty_not_null() {
        TandemKafkaProperties properties =
                new TandemKafkaProperties(URI.create("/tandem/test"), "application/json", null, "-topic", null);

        assertThat(properties.producer()).isEmpty();
    }
}
