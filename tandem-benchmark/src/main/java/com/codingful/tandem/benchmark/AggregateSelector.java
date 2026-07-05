package com.codingful.tandem.benchmark;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Picks which synthetic aggregate the next generated event belongs to (LLD-benchmark §4.2). The full
 * id space ({@link #universe()}) is known upfront so {@link LoadGenerator} can seed every
 * {@code bench_aggregate} row before driving load.
 *
 * <p>{@code namespace} scopes the generated ids (e.g. to a scenario id like {@code "S1"}) so that two
 * selectors never collide on the same aggregate id when they share one {@code tandem_outbox} table —
 * as {@link com.codingful.tandem.benchmark.SmokeLoadTest} does across scenarios in one run. Without
 * this, S6's poisoned aggregate (always {@code universe().get(0)}) would deterministically collide
 * with another scenario's own use of the same default id space, permanently blocking it too (the
 * poison gate is structural and has no expiry — LLD-jdbc §3.4.2).
 */
public interface AggregateSelector {

    /** The next aggregate id to write to. */
    String nextAggregateId();

    /** Every aggregate id this selector can produce — the seed set for {@code bench_aggregate}. */
    List<String> universe();

    /** Spreads load evenly across {@code cardinality} aggregates, namespaced under {@code namespace} (S1, S2, S4, S5). */
    static AggregateSelector uniform(String namespace, int cardinality) {
        List<String> ids = idRange(namespace, cardinality);
        return new AggregateSelector() {
            @Override
            public String nextAggregateId() {
                return ids.get(ThreadLocalRandom.current().nextInt(ids.size()));
            }

            @Override
            public List<String> universe() {
                return ids;
            }
        };
    }

    /**
     * A single hot aggregate ({@link #universe()}{@code .get(0)}) receives {@code hotFraction} of the
     * traffic; the rest spreads uniformly over the remaining {@code cardinality - 1} aggregates,
     * namespaced under {@code namespace} (S3, S6's poison target).
     */
    static AggregateSelector skewed(String namespace, int cardinality, double hotFraction) {
        if (cardinality < 2) {
            throw new IllegalArgumentException("cardinality must be at least 2 for a skewed distribution");
        }
        if (hotFraction <= 0 || hotFraction >= 1) {
            throw new IllegalArgumentException("hotFraction must be in (0, 1)");
        }
        List<String> ids = idRange(namespace, cardinality);
        return new AggregateSelector() {
            @Override
            public String nextAggregateId() {
                ThreadLocalRandom random = ThreadLocalRandom.current();
                if (random.nextDouble() < hotFraction) {
                    return ids.get(0);
                }
                return ids.get(1 + random.nextInt(ids.size() - 1));
            }

            @Override
            public List<String> universe() {
                return ids;
            }
        };
    }

    private static List<String> idRange(String namespace, int cardinality) {
        List<String> ids = new ArrayList<>(cardinality);
        for (int i = 0; i < cardinality; i++) {
            ids.add(namespace + "-bench-agg-" + i);
        }
        return List.copyOf(ids);
    }
}
