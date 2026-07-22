package com.codingful.tandem.core;

import java.util.Objects;
import java.util.OptionalInt;

/**
 * Decides, purely from the <b>stored</b> bucket count and the <b>configured</b> bucket count, what a
 * startup guard should do about them (LLD-bucket-count-guard §3). This is the algorithm half of the
 * guard: a total function of two values with <b>no I/O</b>, so the decision is separable from acting
 * on it and unit-testable without a database. The orchestrator ({@code tandem-jdbc}'s
 * {@code BucketCountGuard}) performs the {@link Decision.Kind#SEED} write and throws on
 * {@link Decision.Kind#CONFLICT}; this type never touches storage.
 *
 * <p>Different policies are different pure functions over the same inputs, so this is a strategy: the
 * shipped default is {@link #seedOrValidate()}; alternatives (a strict "never auto-seed" policy, a
 * warn-only rollout policy) plug in without changing the port or the orchestrator. <b>Re-sharding</b>
 * — treating a mismatch as an intentional change of the bucket count — is deliberately <i>not</i> a
 * policy here: it requires re-bucketing stored rows and a coordinated cut-over, so it is a separate
 * future feature; this guard only ever detects a mismatch, never accepts one.
 */
public interface BucketCountReconciliation {

    /**
     * @param stored     the bucket count already established in the database, or empty if none is
     * @param configured the bucket count this process is configured with (must be positive)
     * @return what the guard should do
     */
    Decision decide(OptionalInt stored, int configured);

    /** The default policy: seed when absent, proceed when equal, fail fast when different (§3.1). */
    static BucketCountReconciliation seedOrValidate() {
        return (stored, configured) -> {
            if (configured <= 0) {
                throw new IllegalArgumentException("configured bucketCount must be positive: " + configured);
            }
            if (stored.isEmpty()) {
                return Decision.seed();
            }
            int existing = stored.getAsInt();
            if (existing == configured) {
                return Decision.proceed();
            }
            return Decision.conflict(
                    "Bucket count mismatch: this process is configured with bucketCount=" + configured
                            + " but the database was first initialised with bucketCount=" + existing
                            + ". The stored value is baked into every outbox row's bucket and cannot change"
                            + " after first deployment. The misconfigured side is the one reporting"
                            + " bucketCount=" + configured + " — align it to " + existing
                            + " (or, if this database is genuinely meant to be re-sharded, that is a"
                            + " separate migration, not a config change).");
        };
    }

    /** The outcome of {@link #decide}: seed the configured value, proceed, or fail with a message. */
    final class Decision {

        /** What the orchestrator should do next. */
        public enum Kind {
            /** No value stored yet — the orchestrator should seed the configured value, then re-decide. */
            SEED,
            /** Stored and configured agree — start normally. */
            PROCEED,
            /** Stored and configured differ — the orchestrator throws with {@link #message()}. */
            CONFLICT
        }

        private static final Decision SEED = new Decision(Kind.SEED, null);
        private static final Decision PROCEED = new Decision(Kind.PROCEED, null);

        private final Kind kind;
        private final String message;

        private Decision(Kind kind, String message) {
            this.kind = kind;
            this.message = message;
        }

        public static Decision seed() {
            return SEED;
        }

        public static Decision proceed() {
            return PROCEED;
        }

        public static Decision conflict(String message) {
            return new Decision(Kind.CONFLICT, Objects.requireNonNull(message, "message"));
        }

        public Kind kind() {
            return kind;
        }

        /** The failure message; set only when {@link #kind()} is {@link Kind#CONFLICT}, else {@code null}. */
        public String message() {
            return message;
        }
    }
}
