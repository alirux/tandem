package com.codingful.tandem.jdbc;

import com.codingful.tandem.core.AggregateId;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The last {@code seq} one worker published per aggregate, and the verdict on the next one — the
 * in-process half of the write-side ordering detector (HLD §8).
 *
 * <p><b>Why in process, and why at publish time.</b> When two writers to one aggregate are not
 * serialised, the row inserted first can commit second, so the relay publishes {@code seq} 2 before
 * {@code seq} 1 — yet the table is left perfectly ordered, {@code id} ascending with {@code seq}. No
 * later scan of {@code tandem_outbox} can find the violation, because by then there is nothing wrong
 * with the data; only the relay, as it publishes, witnesses the sequence that actually went out.
 *
 * <p><b>Bounded, and deliberately lossy.</b> One entry per aggregate for the life of the process
 * would leak on a high-cardinality aggregate space, so this is an LRU capped at {@link #CAPACITY}
 * entries per worker — a fixed constant, not a knob. Evicting an aggregate degrades detection exactly
 * as a relay restart already does, and the design accepts that: this is an alerting signal for a
 * precondition that should never be violated, not an audit log.
 *
 * <p><b>Not thread-safe, by contract.</b> One instance belongs to one {@link RelayWorker} and is
 * touched only from that worker's own loop, never from the dispatcher's completion thread — which is
 * also what lets the regression path issue a database lookup at all.
 */
final class SeqWatermarks {

    /**
     * Entries kept per worker. Orders of magnitude above any realistic {@code batchSize}, and a few MB
     * of heap even across a wide worker pool.
     */
    static final int CAPACITY = 4096;

    /** What the next {@code seq} for an aggregate says about the order it was published in. */
    enum Verdict {
        /** Ahead of everything seen for this aggregate — the normal case, and a first sighting. */
        ADVANCED,
        /** Exactly the last one published: an at-least-once redelivery, not an ordering problem. */
        DUPLICATE,
        /** Behind the last one published — either a write-side ordering violation or an operator replay. */
        REGRESSED
    }

    private final int capacity;
    private final Map<AggregateId, Long> lastPublished;

    SeqWatermarks() {
        this(CAPACITY);
    }

    SeqWatermarks(int capacity) {
        this.capacity = capacity;
        this.lastPublished = new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<AggregateId, Long> eldest) {
                return size() > SeqWatermarks.this.capacity;
            }
        };
    }

    /**
     * Judge {@code seq} against the last one published for {@code aggregateId}, raising the watermark
     * only when it advances. A {@code REGRESSED} verdict leaves the watermark <b>untouched</b>
     * on purpose: lowering it to a replayed row's {@code seq} would make every later row look like an
     * advance, and the next genuine regression would go unseen (HLD §8).
     */
    Verdict record(AggregateId aggregateId, long seq) {
        Long last = lastPublished.get(aggregateId);
        if (last != null && seq < last) {
            return Verdict.REGRESSED;
        }
        if (last != null && seq == last) {
            return Verdict.DUPLICATE;
        }
        lastPublished.put(aggregateId, seq);
        return Verdict.ADVANCED;
    }

    /** Aggregates currently tracked — for tests asserting the LRU actually bounds itself. */
    int size() {
        return lastPublished.size();
    }
}
