package com.codingful.tandem.spring.relay;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.port.TandemMetrics;
import com.codingful.tandem.micrometer.MicrometerTandemMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/**
 * Proves the cross-class {@code @ConditionalOnMissingBean} ordering LLD-micrometer §5 relies on:
 * {@link TandemMicrometerAutoConfiguration} is declared {@code before} {@link TandemRelayAutoConfiguration},
 * and Spring Boot's real deferred-import ordering (not source declaration order within one class, which
 * is never guaranteed) is what makes the Micrometer bean win when it can, and the no-op fall back
 * cleanly otherwise. No Docker needed — no relay is started here.
 */
class TandemMicrometerAutoConfigurationTest {

    // TandemRelayAutoConfiguration is itself @ConditionalOnSingleCandidate(DataSource.class), so without
    // one NONE of its beans — including the no-op fallback under test here — are contributed at all.
    // UnstartedLifecycle (from TandemRelayAutoConfigurationTest) keeps the real RelayLifecycle from
    // opening a database connection at context refresh — this test is about which TandemMetrics bean
    // wins, not about the relay actually starting.
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(
                    TandemMicrometerAutoConfiguration.class, TandemRelayAutoConfiguration.class))
            .withBean(DataSource.class, NoopDataSource::new)
            .withUserConfiguration(TandemRelayAutoConfigurationTest.UnstartedLifecycle.class)
            .withPropertyValues(
                    "tandem.kafka.source=/tandem/test",
                    "tandem.kafka.producer[bootstrap.servers]=localhost:9092");

    @Test
    void GIVEN_a_meter_registry_bean_WHEN_the_context_starts_THEN_the_micrometer_adapter_wins_over_the_noop_default() {
        runner.withBean(MeterRegistry.class, SimpleMeterRegistry::new)
                .run(context -> {
                    assertThat(context).hasSingleBean(TandemMetrics.class);
                    assertThat(context.getBean(TandemMetrics.class)).isInstanceOf(MicrometerTandemMetrics.class);
                });
    }

    @Test
    void GIVEN_tandem_metrics_max_publish_latency_set_WHEN_a_sample_is_recorded_THEN_the_histogram_uses_it() {
        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

        runner.withBean(MeterRegistry.class, () -> registry)
                .withPropertyValues("tandem.metrics.max-publish-latency=90s")
                .run(context -> {
                    TandemMetrics metrics = context.getBean(TandemMetrics.class);
                    metrics.recordPublishLatency(Duration.ofSeconds(80));
                    assertThat(highestFiniteBucket(registry)).isEqualTo(90.0);
                });
    }

    private static double highestFiniteBucket(PrometheusMeterRegistry registry) {
        Pattern finiteBucket = Pattern.compile("tandem_outbox_publish_latency_seconds_bucket\\{le=\"([0-9.]+)\"}");
        Matcher matcher = finiteBucket.matcher(registry.scrape());
        double highest = -1;
        while (matcher.find()) {
            highest = Math.max(highest, Double.parseDouble(matcher.group(1)));
        }
        return highest;
    }

    @Test
    void GIVEN_no_meter_registry_bean_WHEN_the_context_starts_THEN_it_falls_back_to_the_noop_default() {
        runner.run(context -> {
            assertThat(context).hasSingleBean(TandemMetrics.class);
            assertThat(context.getBean(TandemMetrics.class)).isSameAs(TandemMetrics.NOOP);
        });
    }

    @Test
    void GIVEN_an_applications_own_TandemMetrics_bean_WHEN_the_context_starts_THEN_it_wins_over_both_defaults() {
        TandemMetrics custom = new TandemMetrics() {
        };

        runner.withBean(MeterRegistry.class, SimpleMeterRegistry::new)
                .withBean(TandemMetrics.class, () -> custom)
                .run(context -> assertThat(context.getBean(TandemMetrics.class)).isSameAs(custom));
    }

    /**
     * {@code AutoConfigurations.of(...)} above bypasses this resource entirely, so nothing else in the
     * suite would catch this class being left out of it — a real Spring Boot app's {@code @EnableAutoConfiguration}
     * discovers autoconfigurations from exactly this file, and a forgotten line here means the class
     * compiles, every other test still passes, and the bean simply never appears at runtime.
     */
    @Test
    void GIVEN_the_imports_resource_WHEN_read_THEN_it_lists_both_autoconfigurations() throws IOException {
        String path = "META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports";
        try (InputStream in = getClass().getClassLoader().getResourceAsStream(path)) {
            assertThat(in).as("resource on the test classpath: %s", path).isNotNull();
            String contents = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            assertThat(contents)
                    .contains(TandemRelayAutoConfiguration.class.getName())
                    .contains(TandemMicrometerAutoConfiguration.class.getName());
        }
    }
}
