package com.codingful.tandem.benchmark;

import java.time.Duration;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

/**
 * Thread-safe COMMIT→ack latency capture (HLD-load-testing.md §2.2), lossless via HdrHistogram.
 * {@link #snapshot()} follows {@link Recorder} semantics: it returns the interval since the previous
 * snapshot (or construction) and resets the accumulator.
 */
public final class LatencyRecorder {

    private static final int SIGNIFICANT_DIGITS = 3;

    private final Recorder recorder;
    private final long highestTrackableNanos;

    public LatencyRecorder(Duration highestTrackable) {
        this.highestTrackableNanos = highestTrackable.toNanos();
        this.recorder = new Recorder(highestTrackableNanos, SIGNIFICANT_DIGITS);
    }

    /** Records one COMMIT→ack latency sample, clamped to the configured ceiling. */
    public void record(Duration latency) {
        long nanos = latency.toNanos();
        if (nanos <= 0) {
            nanos = 1;
        }
        recorder.recordValue(Math.min(nanos, highestTrackableNanos));
    }

    /** Snapshot-and-reset the accumulated interval. */
    public LatencySnapshot snapshot() {
        Histogram histogram = recorder.getIntervalHistogram();
        return new LatencySnapshot(
                histogram.getTotalCount(),
                Duration.ofNanos(histogram.getValueAtPercentile(50.0)),
                Duration.ofNanos(histogram.getValueAtPercentile(95.0)),
                Duration.ofNanos(histogram.getValueAtPercentile(99.0)),
                Duration.ofNanos(histogram.getValueAtPercentile(99.9)));
    }
}
