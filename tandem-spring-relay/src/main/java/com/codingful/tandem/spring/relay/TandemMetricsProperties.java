package com.codingful.tandem.spring.relay;

import com.codingful.tandem.micrometer.MicrometerTandemMetrics;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Binds {@code tandem.metrics.*} — settings for the {@code tandem-micrometer} adapter (LLD-spring-config
 * §2.5), kept separate from {@code tandem.relay.*} since it configures the metrics adapter, not the relay
 * engine itself.
 *
 * <p>{@code maxPublishLatency} is nullable, same reasoning as {@link TandemRelayProperties}: an unset
 * property leaves {@link MicrometerTandemMetrics}'s own default in force.
 *
 * @param maxPublishLatency the {@code publish.latency} histogram ceiling; default
 *                          {@link MicrometerTandemMetrics#DEFAULT_MAX_EXPECTED_PUBLISH_LATENCY}
 */
@ConfigurationProperties("tandem.metrics")
public record TandemMetricsProperties(Duration maxPublishLatency) {
}
