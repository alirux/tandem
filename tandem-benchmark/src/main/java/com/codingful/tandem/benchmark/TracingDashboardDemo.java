package com.codingful.tandem.benchmark;

import com.codingful.tandem.core.TandemContext;
import com.codingful.tandem.core.TandemHeaders;
import com.codingful.tandem.core.TandemSpanAttributes;
import com.codingful.tandem.core.port.TracePropagator;
import com.codingful.tandem.tracing.otel.OtelTandemSpanRecorder;
import com.codingful.tandem.tracing.otel.OtelTracePropagator;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

/**
 * Drives one real trace end to end — write, outbox dwell, relay publish, consume — through a real
 * OpenTelemetry SDK exporting to Tempo, read on the same Grafana the metrics demo uses
 * ({@link MetricsDashboardDemo}, LLD-benchmark §6.3), over a Tempo datasource (§6.4). Same intent as
 * that demo, applied to the other signal: this **measures nothing and gates nothing**; it shows what an
 * adopter's tracing backend actually receives once instrumented mode is on.
 *
 * <p>The four span kinds a trace is stitched from here, and who emits each one:
 * <ul>
 *   <li><b>write</b> — an {@code INTERNAL} span this demo opens around each unit of work, current on the
 *       thread that calls {@code repository.insert} (via {@link LoadGenerator}'s new
 *       {@code WriteSpanScope} hook), so {@link OtelTracePropagator#capture()} has something real to
 *       read. Not part of the shipped product — a real caller's own domain-transaction span plays this
 *       role, which is exactly why HLD-tracing.md never specifies one: Tandem only ever captures
 *       whatever is already current.</li>
 *   <li><b>outbox dwell</b> — the gap between the write span ending and the publish span starting. There
 *       is deliberately no span for it; the gap itself, read on the waterfall, is what makes the async
 *       boundary the whole design note is about visible.</li>
 *   <li><b>{@code tandem.relay.publish}</b> — emitted by {@link OtelTandemSpanRecorder}, wired into this
 *       demo's one relay instance (instrumented mode).</li>
 *   <li><b>consume</b> — a {@code CONSUMER} span this demo opens after extracting {@code traceparent}
 *       back out of the Kafka headers the relay already copied the row's captured context into
 *       (HLD-tracing.md §3) — no product code involved, a consumer instrumenting itself would do the
 *       same thing.</li>
 * </ul>
 *
 * <p>Also demonstrates the correlation-id join key (HLD-tracing.md §4.1, item 7's slice): every unit of
 * work carries {@code correlation-id = "biz-" + aggregateId} (one simulated business operation per
 * aggregate — genuinely 1:n, since an aggregate's later events share it), captured the dependency-free
 * way via {@link TandemContext} and merged with the trace context through
 * {@link TracePropagator#composite}. The run prints the exact Admin API search an operator would run
 * next, without starting {@code tandem-admin} itself — this benchmark assembles its relay without
 * Spring, and standing up the Admin API here would be far more machinery than the join key needs to
 * demonstrate.
 *
 * <p>Usage: {@code ./gradlew :tandem-benchmark:tracingDashboardDemo}. Add
 * {@code --args="--hold=60"} to close by itself instead of waiting on the keyboard. Needs Docker.
 */
public final class TracingDashboardDemo {

    private static final String SERVICE_NAME = "tandem-benchmark";
    private static final String INSTRUMENTATION_SCOPE = "com.codingful.tandem.benchmark";
    private static final String WRITE_SPAN_NAME = "tandem.benchmark.write";
    private static final String CONSUME_SPAN_NAME = "tandem.benchmark.consume";

    // Deliberately small: this demo shows one legible trace at a time on a waterfall, not throughput
    // (that is LoadTestRunner) or a busy dashboard (that is MetricsDashboardDemo).
    private static final double RATE = 1.5;
    private static final Duration RUN_WINDOW = Duration.ofSeconds(40);

    private static final TextMapGetter<Map<String, String>> HEADER_GETTER = new TextMapGetter<>() {

        @Override
        public Iterable<String> keys(Map<String, String> carrier) {
            return carrier.keySet();
        }

        @Override
        public String get(Map<String, String> carrier, String key) {
            return carrier == null ? null : carrier.get(key);
        }
    };

    private TracingDashboardDemo() {
    }

    // ConsumerSpanBridge below is opened purely for its side effect (the background poll thread it
    // starts in its constructor) — never referenced in the try body, which -Xlint:try flags.
    @SuppressWarnings("try")
    public static void main(String[] args) throws Exception {
        Duration hold = holdFrom(args);

        BenchmarkConfig cfg = BenchmarkConfig.builder()
                .bucketCount(256)
                .workers(1)
                .batchSize(2)
                .deliveryTimeoutMs(8_000)
                .rowLease(Duration.ofSeconds(20))
                .maxAttempts(4)
                .maxConnections(4)
                .payloadBytes(256)
                // Small on purpose: correlation-id is "biz-" + aggregateId (a whole simulated business
                // operation per aggregate), and a small universe means the demo revisits each one, so the
                // 1:n grouping actually shows up within the run window instead of every trace being a
                // singleton.
                .aggregateCardinality(4)
                .build();

        try (BenchmarkEnvironment env = new BenchmarkEnvironment(cfg).start();
             MetricsExporter exporter = new MetricsExporter("relay-1");
             ObservabilityStack observability = new ObservabilityStack(List.of(exporter), true).start()) {

            OpenTelemetrySdk openTelemetry = buildOpenTelemetry(observability.tempoOtlpEndpoint());
            try {
                TracePropagator tracePropagator =
                        TracePropagator.composite(new OtelTracePropagator(openTelemetry), TracePropagator.fromTandemContext());
                Tracer tracer = openTelemetry.getTracer(INSTRUMENTATION_SCOPE);

                RelayInstance relayInstance = env.newRelayInstance(env.relayConfigBuilder().build(),
                        exporter.metrics(), new OtelTandemSpanRecorder(openTelemetry));

                AggregateSelector selector = AggregateSelector.uniform("tracingdemo", cfg.aggregateCardinality());
                String highlighted = selector.universe().get(0);
                String highlightedCorrelationId = correlationIdFor(highlighted);

                try (LoadGenerator generator = new LoadGenerator(env.dataSource(), cfg.bucketCount(),
                        cfg.maxConnections(), selector, cfg.payloadBytes(), null, tracePropagator,
                        (aggregateId, work) -> runWithWriteSpan(tracer, aggregateId, work));
                     ConsumerSpanBridge consumerBridge =
                             new ConsumerSpanBridge(env.newConsumer("tracing-demo"), openTelemetry, tracer)) {

                    intro(observability, highlighted, highlightedCorrelationId);
                    relayInstance.pool().start();
                    generator.start(RATE);

                    Thread.sleep(RUN_WINDOW.toMillis());

                    generator.stop();
                    outro(observability, highlightedCorrelationId);
                    waitForOperator(hold);
                } finally {
                    relayInstance.pool().stop();
                }
            } finally {
                // Flushes the BatchSpanProcessor's remaining spans to Tempo before the container stops —
                // without this, whatever hasn't hit its 1s schedule yet is lost, including the very last
                // spans of the run the operator is most likely to go looking for.
                openTelemetry.getSdkTracerProvider().shutdown();
            }
        }
    }

    /** One unit of work's write span: current on this thread, so capture() below has something to read. */
    @SuppressWarnings("try")   // makeCurrent()'s Scope closes for its side effect, never read in the body
    private static void runWithWriteSpan(Tracer tracer, String aggregateId, Runnable work) {
        String correlationId = correlationIdFor(aggregateId);
        TandemContext.setCorrelationId(correlationId);
        Span span = tracer.spanBuilder(WRITE_SPAN_NAME)
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute(TandemSpanAttributes.AGGREGATE_ID, aggregateId)
                .setAttribute(TandemSpanAttributes.CORRELATION_ID, correlationId)
                .startSpan();
        try (Scope scope = span.makeCurrent()) {
            work.run();
        } finally {
            span.end();
            TandemContext.clear();
        }
    }

    /** One simulated business operation per aggregate — later events for the same aggregate share it. */
    private static String correlationIdFor(String aggregateId) {
        return "biz-" + aggregateId;
    }

    private static OpenTelemetrySdk buildOpenTelemetry(String tempoOtlpEndpoint) {
        Resource resource = Resource.getDefault().toBuilder()
                .put(AttributeKey.stringKey("service.name"), SERVICE_NAME)
                .build();
        SpanExporter exporter = OtlpHttpSpanExporter.builder()
                .setEndpoint(tempoOtlpEndpoint + "/v1/traces")
                .build();
        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .setResource(resource)
                // Short schedule delay: this is a live demo, not a production exporter — spans should
                // reach Tempo within a couple of seconds of being ended, not the 5s SDK default.
                .addSpanProcessor(BatchSpanProcessor.builder(exporter)
                        .setScheduleDelay(Duration.ofSeconds(1))
                        .build())
                .build();
        return OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                .build();
    }

    private static void intro(ObservabilityStack observability, String highlighted, String correlationId) {
        System.out.println();
        System.out.println("=".repeat(88));
        System.out.println("  Tandem relay tracing — live trace view");
        System.out.println("=".repeat(88));
        System.out.println();
        System.out.println("  Grafana (traces)  " + observability.tracesDashboardUrl());
        System.out.println("  Grafana (Tempo search UI also works directly at " + observability.grafanaBaseUrl() + ")");
        System.out.println();
        System.out.printf(Locale.ROOT, "  Running for %ds at ~%.1f events/s across %d aggregates.%n",
                RUN_WINDOW.toSeconds(), RATE, 4);
        System.out.println("  Each trace: write -> outbox dwell (no span - that gap is the point) ->");
        System.out.println("  tandem.relay.publish -> consume.");
        System.out.println();
        System.out.printf(Locale.ROOT, "  Aggregate %s carries correlation id %s on every one of its events —%n",
                highlighted, correlationId);
        System.out.println("  filter the traces panel on it, or once the run ends, run:");
        System.out.println();
        System.out.println("      tandem outbox search --correlation-id " + correlationId);
        System.out.println();
    }

    private static void outro(ObservabilityStack observability, String correlationId) {
        System.out.println();
        System.out.println("-".repeat(88));
        System.out.println("  Run complete. Traces already exported are still queryable.");
        System.out.println();
        System.out.println("  Grafana  " + observability.tracesDashboardUrl());
        System.out.println("  The operator's next step for aggregate biz operation " + correlationId + ":");
        System.out.println("      tandem outbox search --correlation-id " + correlationId);
    }

    private static void waitForOperator(Duration hold) throws Exception {
        if (hold != null) {
            System.out.printf(Locale.ROOT, "  Holding for %d seconds, then shutting the stack down.%n%n",
                    hold.toSeconds());
            Thread.sleep(hold.toMillis());
            return;
        }
        System.out.println("  Press Enter to shut the containers down.");
        System.out.println();
        // A closed stdin (the task is run without a console) reads -1 immediately rather than blocking
        // forever, which is the behaviour --hold exists for; falling through is the right answer either way.
        System.in.read();
    }

    private static Duration holdFrom(String[] args) {
        for (String arg : args) {
            if (arg.startsWith("--hold=")) {
                return Duration.ofSeconds(Long.parseLong(arg.substring("--hold=".length())));
            }
        }
        return null;
    }

    /**
     * Bridges the wire back into a span: extracts {@code traceparent}/{@code tracestate} from the Kafka
     * headers the relay already copied the row's captured context into (HLD-tracing.md §3 — no product
     * change needed for propagation to reach here) and opens a {@code CONSUMER} span parented from it. A
     * record carrying no valid trace context gets no span, the same orphan-avoidance rule the relay's
     * own {@link OtelTandemSpanRecorder} applies.
     */
    private static final class ConsumerSpanBridge implements AutoCloseable {

        private final KafkaConsumer<String, byte[]> consumer;
        private final TextMapPropagator propagator;
        private final Tracer tracer;
        private final Thread thread;
        private volatile boolean running = true;

        ConsumerSpanBridge(KafkaConsumer<String, byte[]> consumer, OpenTelemetry openTelemetry, Tracer tracer) {
            this.consumer = consumer;
            this.propagator = openTelemetry.getPropagators().getTextMapPropagator();
            this.tracer = tracer;
            this.thread = new Thread(this::pollLoop, "bench-tracing-consumer");
            thread.setDaemon(true);
            thread.start();
        }

        private void pollLoop() {
            while (running) {
                ConsumerRecords<String, byte[]> records;
                try {
                    records = consumer.poll(Duration.ofMillis(200));
                } catch (WakeupException e) {
                    continue;   // checked again against `running` at the top of the loop
                }
                for (ConsumerRecord<String, byte[]> record : records) {
                    consumeSpan(record);
                }
            }
        }

        private void consumeSpan(ConsumerRecord<String, byte[]> record) {
            Map<String, String> carrier = new HashMap<>();
            record.headers().forEach(h -> carrier.put(h.key(), new String(h.value(), StandardCharsets.UTF_8)));
            Context parent = propagator.extract(Context.root(), carrier, HEADER_GETTER);
            if (!Span.fromContext(parent).getSpanContext().isValid()) {
                return;
            }
            Span span = tracer.spanBuilder(CONSUME_SPAN_NAME)
                    .setParent(parent)
                    .setSpanKind(SpanKind.CONSUMER)
                    .setAttribute(TandemSpanAttributes.AGGREGATE_ID, record.key())
                    .startSpan();
            String correlationId = carrier.get(TandemHeaders.CORRELATION_ID);
            if (correlationId != null) {
                span.setAttribute(TandemSpanAttributes.CORRELATION_ID, correlationId);
            }
            span.end();
        }

        @Override
        public void close() {
            running = false;
            consumer.wakeup();
            try {
                thread.join(TimeUnit.SECONDS.toMillis(10));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            consumer.close();
        }
    }
}
