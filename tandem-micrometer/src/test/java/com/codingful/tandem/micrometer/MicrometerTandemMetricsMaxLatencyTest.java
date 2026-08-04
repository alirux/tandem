package com.codingful.tandem.micrometer;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/**
 * {@code publish.latency}'s histogram ceiling, kept separate from {@link MicrometerTandemMetricsTest}:
 * it needs a registry that actually materializes histogram buckets ({@link PrometheusMeterRegistry}),
 * unlike every other test in this module, which uses {@code SimpleMeterRegistry} — a registry that
 * tracks count/sum only and never renders real bucket data regardless of the {@code Timer}'s config.
 */
class MicrometerTandemMetricsMaxLatencyTest {

    private static final Pattern FINITE_BUCKET = Pattern.compile(
            "tandem_outbox_publish_latency_seconds_bucket\\{le=\"([0-9.]+)\"}");

    @Test
    void GIVEN_no_ceiling_given_WHEN_a_sample_is_recorded_THEN_the_histogram_defaults_to_a_five_minute_ceiling() {
        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        MicrometerTandemMetrics metrics = new MicrometerTandemMetrics(registry);

        metrics.recordPublishLatency(Duration.ofSeconds(200));

        assertThat(highestFiniteBucket(registry)).isEqualTo(300.0);
    }

    @Test
    void GIVEN_a_custom_ceiling_WHEN_a_sample_is_recorded_THEN_the_histogram_uses_it_instead_of_the_default() {
        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        MicrometerTandemMetrics metrics = new MicrometerTandemMetrics(registry, Duration.ofSeconds(90));

        metrics.recordPublishLatency(Duration.ofSeconds(80));

        assertThat(highestFiniteBucket(registry)).isEqualTo(90.0);
    }

    private static double highestFiniteBucket(PrometheusMeterRegistry registry) {
        Matcher matcher = FINITE_BUCKET.matcher(registry.scrape());
        double highest = -1;
        while (matcher.find()) {
            highest = Math.max(highest, Double.parseDouble(matcher.group(1)));
        }
        return highest;
    }
}
