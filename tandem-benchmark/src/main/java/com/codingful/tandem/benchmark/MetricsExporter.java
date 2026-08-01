package com.codingful.tandem.benchmark;

import com.codingful.tandem.core.port.TandemMetrics;
import com.codingful.tandem.micrometer.MicrometerTandemMetrics;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

/**
 * One relay instance's scrape endpoint: a {@link MicrometerTandemMetrics} over a Prometheus registry,
 * published at {@code /metrics} by the JDK's own HTTP server (LLD-benchmark §6.3). No Spring, no
 * Actuator — this module is a plain leaf app, and the point is to exercise the framework-agnostic
 * adapter exactly as a manually assembled relay would.
 *
 * <p><b>One exporter per relay instance, deliberately.</b> Several of the port's signals are
 * per-instance ({@code workers.active}) while others are a global reading every instance reports
 * ({@code lag.count}), and a single shared registry would silently collapse the first kind to
 * whichever instance wrote last. Separate endpoints reproduce the real topology — N relay processes,
 * N scrape targets — which is also the only way a dashboard's aggregation choices can be judged.
 */
public final class MetricsExporter implements AutoCloseable {

    private final PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    private final MicrometerTandemMetrics metrics = new MicrometerTandemMetrics(registry);
    private final HttpServer server;
    private volatile boolean closed;

    /**
     * @param relayId the {@code relay} label Prometheus attaches to this target's series
     */
    public MetricsExporter(String relayId) {
        try {
            // Port 0 = let the OS assign a free one: the demo starts several of these and must not
            // collide with whatever else the developer machine is running.
            server = HttpServer.create(new InetSocketAddress("0.0.0.0", 0), 0);
        } catch (IOException e) {
            throw new UncheckedIOException("binding the metrics endpoint for " + relayId + " failed", e);
        }
        server.createContext("/metrics", exchange -> {
            byte[] body = registry.scrape().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        });
        server.start();
    }

    /** The {@link TandemMetrics} to wire into the relay instance this exporter represents. */
    public TandemMetrics metrics() {
        return metrics;
    }

    public int port() {
        return server.getAddress().getPort();
    }

    /** The exposition text a scrape would return right now — printed once so the meter names can be read. */
    public String scrape() {
        return registry.scrape();
    }

    /** Idempotent: the demo closes a crashed instance's endpoint early, and again on the way out. */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        server.stop(0);
        registry.close();
    }
}
