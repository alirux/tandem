package com.codingful.tandem.benchmark;

import java.time.Duration;
import java.time.Instant;

/**
 * Adaptive lag-feedback rate search (HLD-load-testing.md §3.1, §4 S1; LLD-benchmark §7): additively
 * increases the offered rate while the backlog stays within a noise band, backs off multiplicatively
 * when it grows past that band, and reports the highest rate <b>held fixed</b> within the band for at
 * least {@code sustainWindow} — a burst peak is never reported as the sustainable max.
 *
 * <p>Two things that looked right on paper turned out not to converge on a real relay, both found by
 * actually running this against Docker containers rather than by inspection:
 *
 * <ul>
 *   <li><b>Hold the rate fixed while verifying it, don't keep raising it.</b> An earlier version kept
 *       increasing the rate on every flat observation <i>while</i> the sustain clock was running,
 *       which effectively tested whether the system could absorb a continuously-<i>increasing</i> rate
 *       for the whole window — a bar that is nearly impossible to clear.</li>
 *   <li><b>Compare against a fixed baseline with a tolerance band, not the immediately preceding
 *       sample with none.</b> The relay claims in batches (up to {@code batchSize} rows per poll
 *       cycle), so the pending count naturally saw-tooths by roughly that magnitude within a single
 *       poll cycle even at a genuinely sustainable steady state. A strict "no single sample may be
 *       higher than the last one" check trips on that normal wobble almost every window, so the
 *       sustain gate (also) never actually completed. {@code toleranceRows} — the claim-batch size is
 *       the natural unit for it — absorbs that wobble while still catching real, growing backlog.</li>
 * </ul>
 */
public final class RampController {

    private final LagProbe lagProbe;
    private final Duration observationWindow;
    private final Duration sustainWindow;
    private final double rampStepFraction;
    private final double backoffFraction;
    private final long toleranceRows;

    public RampController(LagProbe lagProbe, Duration observationWindow, Duration sustainWindow,
                           double rampStepFraction, double backoffFraction, long toleranceRows) {
        this.lagProbe = lagProbe;
        this.observationWindow = observationWindow;
        this.sustainWindow = sustainWindow;
        this.rampStepFraction = rampStepFraction;
        this.backoffFraction = backoffFraction;
        this.toleranceRows = toleranceRows;
    }

    public record RampResult(double sustainedRatePerSecond) {
    }

    /**
     * Starts {@code generator} at {@code initialRatePerSecond} and drives the search for up to
     * {@code budget}. Leaves the generator running at the end — the caller stops it.
     */
    public RampResult findSustainableMax(LoadGenerator generator, double initialRatePerSecond, Duration budget) {
        double rate = initialRatePerSecond;
        generator.start(rate);

        Instant deadline = Instant.now().plus(budget);
        double bestSustained = 0;
        Instant holdSince = Instant.now();
        long holdBaselinePending = lagProbe.overall().pending();

        while (Instant.now().isBefore(deadline)) {
            sleep(observationWindow);
            long pending = lagProbe.overall().pending();

            if (pending > holdBaselinePending + toleranceRows) {
                // Backlog grew past the noise band at this rate: back off and start a fresh hold.
                rate = Math.max(1, rate * (1 - backoffFraction));
                generator.setRate(rate);
                holdSince = Instant.now();
                holdBaselinePending = pending;
            } else if (Duration.between(holdSince, Instant.now()).compareTo(sustainWindow) >= 0) {
                // Stayed within the band for the whole window at a FIXED rate: confirmed sustained.
                bestSustained = rate;
                rate = rate * (1 + rampStepFraction);
                generator.setRate(rate);
                holdSince = Instant.now();
                holdBaselinePending = pending;
            }
            // else: still within the current hold — rate untouched, baseline untouched.
        }
        return new RampResult(bestSustained);
    }

    private static void sleep(Duration d) {
        try {
            Thread.sleep(d.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
