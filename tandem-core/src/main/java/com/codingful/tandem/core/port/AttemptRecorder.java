package com.codingful.tandem.core.port;

import com.codingful.tandem.core.AttemptOutcome;

/**
 * Optional attempt-archive port (LLD-core §2.5, §7.1, HLD-attempt-archive). The default is a no-op
 * and {@link #isEnabled()} is {@code false}, so the relay skips even <b>building</b> the {@link
 * AttemptOutcome} when off (HLD-attempt-archive §5). A real adapter ships in {@code tandem-jdbc}.
 */
public interface AttemptRecorder {

    /** A no-op recorder — the default when the archive is disabled. */
    AttemptRecorder NOOP = new AttemptRecorder() {
    };

    /** {@code true} once a real adapter is wired; the relay only builds {@link AttemptOutcome} when enabled. */
    default boolean isEnabled() {
        return false;
    }

    /** @param outcome the per-attempt record to archive */
    default void record(AttemptOutcome outcome) {
    }
}
