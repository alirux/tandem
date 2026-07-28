package com.codingful.tandem.spring.producer;

import static com.codingful.tandem.test.spring.ConfigurationMetadataReference.boundKeys;
import static com.codingful.tandem.test.spring.ConfigurationMetadataReference.referencedKeys;
import static com.codingful.tandem.test.spring.ConfigurationMetadataReference.resource;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Keeps the shipped reference configuration honest (LLD-spring-config §2.4): it is a <em>rendering</em>
 * of the property contract, so it may never list a key the module does not bind, nor omit one it does.
 * Both directions are checked against the generated configuration metadata — the same artifact the IDE
 * reads — so adding, renaming or removing a property without touching the reference file fails here
 * rather than silently shipping a reference that lies. The write side binds one namespace today; the
 * check is what keeps that true of the file as the contract grows. Shared with
 * {@code tandem-spring-relay}'s equivalent test (a {@code tandem-test} fixture).
 */
class ReferenceConfigurationTest {

    private final String metadata = resource(
            ReferenceConfigurationTest.class, "/META-INF/spring-configuration-metadata.json");
    private final String reference = resource(ReferenceConfigurationTest.class, "/tandem-producer-reference.yml");

    @Test
    void GIVEN_the_bound_property_contract_WHEN_compared_with_the_reference_THEN_every_key_is_documented() {
        assertThat(boundKeys(metadata)).isNotEmpty().allSatisfy(key -> assertThat(reference).contains(key));
    }

    @Test
    void GIVEN_the_reference_WHEN_compared_with_the_bound_property_contract_THEN_it_invents_no_key() {
        List<String> bound = boundKeys(metadata);

        assertThat(referencedKeys(reference)).isNotEmpty().allSatisfy(key ->
                assertThat(bound).as("key %s is documented but bound by nothing", key)
                        .anySatisfy(boundKey -> assertThat(boundKey).startsWith(key)));
    }
}
