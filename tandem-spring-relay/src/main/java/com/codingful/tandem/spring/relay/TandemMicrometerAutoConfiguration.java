package com.codingful.tandem.spring.relay;

import com.codingful.tandem.core.port.TandemMetrics;
import com.codingful.tandem.micrometer.MicrometerTandemMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Contributes a Micrometer-backed {@link TandemMetrics} bean ahead of
 * {@link TandemRelayAutoConfiguration}'s own no-op default (LLD-micrometer §5, Q31).
 *
 * <p>A separate {@code @AutoConfiguration} class, not a second {@code @Bean} method inside
 * {@link TandemRelayAutoConfiguration}: Spring only <em>guarantees</em>
 * {@link ConditionalOnMissingBean} ordering across explicitly ordered auto-configuration classes
 * ({@code before}/{@code after}), never across multiple {@code @Bean} methods declared in one class —
 * so this class is ordered {@code before} that one, and its own no-op bean is left untouched; Boot's
 * guaranteed cross-class ordering means it sees this bean already registered (when the conditions
 * below were met) and backs off on its own.
 *
 * <p>{@code before = TandemRelayAutoConfiguration.class} is a direct class literal, deliberately —
 * unlike the {@code afterName} string-based ordering that class uses against Spring's own relocatable
 * autoconfigurations (LLD-spring-config §1.1). That rule exists because Spring's classes move between
 * Boot generations; this one is Tandem's own, always in the same package on both.
 */
@AutoConfiguration(before = TandemRelayAutoConfiguration.class)
@ConditionalOnClass({MeterRegistry.class, MicrometerTandemMetrics.class})
@EnableConfigurationProperties(TandemMetricsProperties.class)
public class TandemMicrometerAutoConfiguration {

    @Bean
    @ConditionalOnBean(MeterRegistry.class)
    @ConditionalOnMissingBean(TandemMetrics.class)
    TandemMetrics tandemMicrometerMetrics(MeterRegistry registry, TandemMetricsProperties properties) {
        return properties.maxPublishLatency() != null
                ? new MicrometerTandemMetrics(registry, properties.maxPublishLatency())
                : new MicrometerTandemMetrics(registry);
    }
}
