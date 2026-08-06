package com.codingful.tandem.jdbc;

/**
 * The relay's own read of the Admin API's desired control state (HLD-admin-api §4.1): whether it
 * should currently be paused, whole or per-bucket. Deliberately separate from {@link BucketSource}
 * (ownership) — pause is orthogonal to which buckets an instance owns, and unlike ownership it must
 * work under {@link Coordination#SINGLE} too, which has no lease table.
 *
 * <p>Cached by design: {@link #refresh()} is called on {@link WorkerPool}'s own maintenance cadence,
 * not on the claim hot path, so {@link #wholeRelayPaused()}/{@link #bucketPaused(int)} are always
 * free, in-memory reads. Every method has a default so {@link #NOOP} needs no implementation at all —
 * the seam {@link WorkerPool}'s basic-round convenience constructor uses, since it has no
 * {@code DataSource} to read control state from in the first place.
 */
public interface RelayControlSource {

    /** The default: never paused, records nothing. Used where no {@code DataSource} is available. */
    RelayControlSource NOOP = new RelayControlSource() {
    };

    /** Called once from {@link WorkerPool#start()}, before the first {@link #refresh()}. No-op by default. */
    default void onStart() {
    }

    /** Re-reads the desired state from the database. No-op by default. */
    default void refresh() {
    }

    /**
     * Marks this instance as still alive, for the Admin API's {@code RelayStatus.state == DOWN}
     * reading (HLD-admin-api §4.1) — a lightweight liveness signal, distinct from {@link #refresh}'s
     * read of desired state. Called on the same cadence as {@link #refresh}. No-op by default.
     */
    default void heartbeat() {
    }

    /** @return the whole-relay desired state as of the last {@link #refresh()}; {@code false} by default */
    default boolean wholeRelayPaused() {
        return false;
    }

    /** @return whether {@code bucket} is deliberately paused as of the last {@link #refresh()}; {@code false} by default */
    default boolean bucketPaused(int bucket) {
        return false;
    }
}
