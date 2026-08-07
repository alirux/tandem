package com.codingful.tandem.benchmark;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

/**
 * A real Prometheus scraping the relay's Micrometer endpoints, and a Grafana pre-provisioned with a
 * dashboard over it (LLD-benchmark §6.3) — optionally with a Tempo receiving the JVM's spans over OTLP
 * (§6.4), so one Grafana serves both signals. Started alongside the benchmark's own Postgres/Kafka
 * containers, on a network of its own — it never talks to them, only to the JVM.
 *
 * <p>The Prometheus configuration is generated here rather than checked in, because the scrape
 * targets are the {@link MetricsExporter}s' OS-assigned ports; the Grafana provisioning files and the
 * dashboard itself are static classpath resources.
 *
 * <p><b>The two signals travel in opposite directions</b>, which is why only one of them needs
 * {@code host.testcontainers.internal}: Prometheus reaches <i>into</i> the JVM to scrape it, while the
 * JVM pushes spans <i>out</i> to Tempo's mapped OTLP port like any other client of a container.
 */
public final class ObservabilityStack implements AutoCloseable {

    private static final Logger LOG = System.getLogger(ObservabilityStack.class.getName());

    private static final DockerImageName PROMETHEUS_IMAGE = DockerImageName.parse("prom/prometheus:v2.53.3");
    // 11.6.0, not 11.3.0: on 11.3.0 a dashboard-scoped annotation is fetched but never painted on a
    // panel, so the phase markers below are silently invisible (§6.3). A/B-verified on this exact
    // dashboard — same Prometheus, same annotation, lines only on 11.6.0.
    private static final DockerImageName GRAFANA_IMAGE = DockerImageName.parse("grafana/grafana:11.6.0");
    private static final DockerImageName TEMPO_IMAGE = DockerImageName.parse("grafana/tempo:2.7.2");
    private static final int PROMETHEUS_PORT = 9090;
    private static final int GRAFANA_PORT = 3000;
    private static final int TEMPO_HTTP_PORT = 3200;
    private static final int TEMPO_OTLP_HTTP_PORT = 4318;

    /** Fine enough that a phase lasting a few seconds is several points rather than one. */
    private static final Duration SCRAPE_INTERVAL = Duration.ofSeconds(1);

    /** Kept on every phase annotation for filtering; not what makes it render — see {@link #annotate}. */
    private static final String PHASE_ANNOTATION_TAG = "tandem-phase";

    /** Must match the dashboard JSON's {@code "uid"} (§6.3) — see {@link #annotate}. */
    private static final String DASHBOARD_UID = "tandem-relay";

    private final Network network = Network.newNetwork();
    private final GenericContainer<?> prometheus;
    private final GenericContainer<?> grafana;
    private final GenericContainer<?> tempo;   // null unless tracing is requested (§6.4)
    private final HttpClient http = HttpClient.newHttpClient();

    /**
     * @param exporters the relay instances' scrape endpoints, in the order their {@code relay} label
     *                  should read {@code relay-1}, {@code relay-2}, …
     */
    public ObservabilityStack(List<MetricsExporter> exporters) {
        this(exporters, false);
    }

    /**
     * @param exporters   see {@link #ObservabilityStack(List)}
     * @param withTracing starts a Tempo container alongside Prometheus and provisions it as a second
     *                    Grafana datasource, plus the traces dashboard (§6.4). {@code false} reproduces
     *                    the metrics-only stack exactly — {@link TracingDashboardDemo} is the only
     *                    caller that passes {@code true}, so {@link MetricsDashboardDemo} never pays for
     *                    a container it doesn't use.
     */
    public ObservabilityStack(List<MetricsExporter> exporters, boolean withTracing) {
        // Makes the JVM's own ports reachable from inside the containers as host.testcontainers.internal.
        exporters.forEach(exporter -> Testcontainers.exposeHostPorts(exporter.port()));

        prometheus = new GenericContainer<>(PROMETHEUS_IMAGE)
                .withNetwork(network)
                .withNetworkAliases("prometheus")
                .withExposedPorts(PROMETHEUS_PORT)
                .withCopyToContainer(Transferable.of(prometheusConfig(exporters)), "/etc/prometheus/prometheus.yml")
                .waitingFor(Wait.forHttp("/-/ready").forPort(PROMETHEUS_PORT));

        tempo = withTracing
                ? new GenericContainer<>(TEMPO_IMAGE)
                        .withNetwork(network)
                        .withNetworkAliases("tempo")
                        .withCommand("-config.file=/etc/tempo/tempo.yml")
                        .withExposedPorts(TEMPO_HTTP_PORT, TEMPO_OTLP_HTTP_PORT)
                        .withCopyToContainer(Transferable.of(tempoConfig()), "/etc/tempo/tempo.yml")
                        .waitingFor(Wait.forHttp("/ready").forPort(TEMPO_HTTP_PORT))
                : null;

        GenericContainer<?> grafanaContainer = new GenericContainer<>(GRAFANA_IMAGE)
                .withNetwork(network)
                .withExposedPorts(GRAFANA_PORT)
                // Anonymous admin: the demo's whole value is being able to open the dashboard and look,
                // and a login prompt in front of a throwaway container is friction for nothing.
                .withEnv("GF_AUTH_ANONYMOUS_ENABLED", "true")
                .withEnv("GF_AUTH_ANONYMOUS_ORG_ROLE", "Admin")
                .withEnv("GF_AUTH_DISABLE_LOGIN_FORM", "true")
                // Grafana's own floor on the refresh picker is 5s by default (both the dropdown options
                // and what it will actually honor) regardless of what the dashboard JSON's "refresh"
                // field says — this is a server-side setting, not a per-dashboard one.
                .withEnv("GF_DASHBOARDS_MIN_REFRESH_INTERVAL", "1s")
                .withCopyToContainer(Transferable.of(datasourceProvisioning(withTracing)),
                        "/etc/grafana/provisioning/datasources/datasources.yml")
                .withCopyToContainer(Transferable.of(dashboardProvisioning()),
                        "/etc/grafana/provisioning/dashboards/tandem.yml")
                .withCopyToContainer(Transferable.of(dashboardJson()),
                        "/var/lib/grafana/dashboards/tandem-dashboard.json")
                .waitingFor(Wait.forHttp("/api/health").forPort(GRAFANA_PORT));
        if (withTracing) {
            grafanaContainer.withCopyToContainer(Transferable.of(tracesDashboardJson()),
                    "/var/lib/grafana/dashboards/tandem-traces-dashboard.json");
        }
        grafana = grafanaContainer;
    }

    public ObservabilityStack start() {
        prometheus.start();
        if (tempo != null) {
            tempo.start();
        }
        grafana.start();
        return this;
    }

    /**
     * The OTLP/HTTP endpoint the JVM exports spans to, reachable from the host because Tempo's port is
     * mapped like any other Testcontainers port — unlike Prometheus scraping, this direction needs no
     * {@code host.testcontainers.internal} bridge (see the class javadoc).
     *
     * @throws IllegalStateException if this stack was built without {@code withTracing}
     */
    public String tempoOtlpEndpoint() {
        if (tempo == null) {
            throw new IllegalStateException("this ObservabilityStack was not started with tracing");
        }
        return "http://" + tempo.getHost() + ":" + tempo.getMappedPort(TEMPO_OTLP_HTTP_PORT);
    }

    /** Where to point a browser for the traces dashboard — only valid alongside {@link #tempoOtlpEndpoint()}. */
    public String tracesDashboardUrl() {
        return grafanaBaseUrl() + "/d/tandem-traces/tandem-traces?from=now-10m&to=now";
    }

    /** Where to point a browser — the dashboard itself, not Grafana's home page. */
    public String dashboardUrl() {
        return grafanaBaseUrl() + "/d/tandem-relay/tandem-relay?refresh=2s&from=now-10m&to=now";
    }

    public String grafanaBaseUrl() {
        return "http://" + grafana.getHost() + ":" + grafana.getMappedPort(GRAFANA_PORT);
    }

    public String prometheusUrl() {
        return "http://" + prometheus.getHost() + ":" + prometheus.getMappedPort(PROMETHEUS_PORT);
    }

    /**
     * Marks the current instant on every graph as a vertical line — what the demo calls at the start of
     * each scripted phase (§6.3), so the panels can be read against "what was being done to the relay
     * right here" without cross-checking the console log. Best-effort: an annotation is a viewing aid,
     * never something worth failing the run over, so a request failure is logged and swallowed rather
     * than propagated.
     *
     * <p><b>{@code dashboardUID} is what makes this render, not the tag.</b> Confirmed the hard way: a
     * global annotation matched by a dashboard-level tag-query (type {@code "tags"}) is fetched by the
     * frontend (visible in the network panel, correct time/text/tags) but never painted on a timeseries
     * panel — a real Grafana behavior, not a config typo, reproduced on both 11.3.0 and 11.6.0 against a
     * minimal one-panel dashboard. Every dashboard load also fires an *implicit*, always-on query for
     * annotations scoped to {@code dashboardUID == <this dashboard>} (the built-in "Annotations & Alerts"
     * layer) — scoping to it instead is what actually draws the line. The dashboard JSON therefore
     * declares no custom annotation query at all (§6.3); scoping here is sufficient on its own.
     */
    public void annotate(String text) {
        String body = "{\"dashboardUID\":\"" + DASHBOARD_UID + "\",\"time\":" + System.currentTimeMillis()
                + ",\"tags\":[\"" + PHASE_ANNOTATION_TAG + "\"],\"text\":\"" + escapeJson(text) + "\"}";
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(grafanaBaseUrl() + "/api/annotations"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        try {
            HttpResponse<Void> response = http.send(request, HttpResponse.BodyHandlers.discarding());
            if (response.statusCode() >= 300) {
                LOG.log(Level.WARNING, "Grafana annotation request failed statusCode:" + response.statusCode());
            }
        } catch (IOException e) {
            LOG.log(Level.WARNING, "Grafana annotation request failed", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static String escapeJson(String text) {
        return text.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static String prometheusConfig(List<MetricsExporter> exporters) {
        String targets = IntStream.range(0, exporters.size())
                .mapToObj(i -> """
                              - targets: ['host.testcontainers.internal:%d']
                                labels:
                                  relay: 'relay-%d'
                        """.formatted(exporters.get(i).port(), i + 1))
                .collect(Collectors.joining());
        return """
                global:
                  scrape_interval: %ds
                  evaluation_interval: %ds
                scrape_configs:
                  - job_name: 'tandem-relay'
                    metrics_path: '/metrics'
                    static_configs:
                %s"""
                .formatted(SCRAPE_INTERVAL.toSeconds(), SCRAPE_INTERVAL.toSeconds(), targets);
    }

    private static String datasourceProvisioning(boolean withTracing) {
        String prometheusEntry = """
                  - name: Prometheus
                    uid: tandem-prometheus
                    type: prometheus
                    access: proxy
                    url: http://prometheus:9090
                    isDefault: true
                """;
        // Points Grafana's trace view at Tempo's own search API, so a Prometheus-sourced correlation-id
        // is one click away from its spans without leaving the dashboard (§6.4).
        String tempoEntry = withTracing
                ? """
                  - name: Tempo
                    uid: tandem-tempo
                    type: tempo
                    access: proxy
                    url: http://tempo:%d
                """.formatted(TEMPO_HTTP_PORT)
                : "";
        return "apiVersion: 1\ndatasources:\n" + prometheusEntry + tempoEntry;
    }

    private static String dashboardProvisioning() {
        return """
                apiVersion: 1
                providers:
                  - name: 'tandem'
                    type: file
                    allowUiUpdates: true
                    options:
                      path: /var/lib/grafana/dashboards
                """;
    }

    private static String dashboardJson() {
        return classpathResource("/grafana/tandem-dashboard.json");
    }

    private static String tracesDashboardJson() {
        return classpathResource("/grafana/tandem-traces-dashboard.json");
    }

    private static String classpathResource(String resource) {
        try (InputStream in = ObservabilityStack.class.getResourceAsStream(resource)) {
            if (in == null) {
                throw new IllegalStateException(resource + " not found on classpath");
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Minimal single-binary config: local disk storage (WAL + blocks under {@code /tmp/tempo}, a
     * throwaway container's own filesystem — nothing is retained past {@link #close()}), OTLP/HTTP
     * receiver only (the one protocol {@code tandem-tracing-otel}'s exporter speaks), no metrics-
     * generator or compactor tuning — this is a demo stack, not a production deployment.
     *
     * <p><b>The OTLP HTTP receiver's {@code endpoint} must be explicit.</b> Left unset, Tempo 2.7.2
     * binds it to {@code localhost:4318} <i>inside the container</i> — verified directly: {@code docker
     * logs} showed {@code endpoint=localhost:4318}, and every export from outside the container
     * (including through Docker's own published port) got {@code Connection reset}, not a clean
     * refusal, because the TCP handshake completes against Docker's proxy before the loopback-only bind
     * inside drops it. {@code 0.0.0.0:4318} is required for the JVM — reachable only via the mapped
     * port, never {@code host.testcontainers.internal} — to reach it at all (see the class javadoc on
     * why this direction differs from Prometheus's).
     */
    private static String tempoConfig() {
        return """
                server:
                  http_listen_port: %d
                distributor:
                  receivers:
                    otlp:
                      protocols:
                        http:
                          endpoint: 0.0.0.0:%d
                storage:
                  trace:
                    backend: local
                    local:
                      path: /tmp/tempo/blocks
                    wal:
                      path: /tmp/tempo/wal
                """.formatted(TEMPO_HTTP_PORT, TEMPO_OTLP_HTTP_PORT);
    }

    @Override
    public void close() {
        grafana.stop();
        if (tempo != null) {
            tempo.stop();
        }
        prometheus.stop();
        network.close();
    }
}
