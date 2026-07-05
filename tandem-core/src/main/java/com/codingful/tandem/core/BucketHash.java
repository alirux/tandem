package com.codingful.tandem.core;

import java.nio.charset.StandardCharsets;

/**
 * Computes the <b>virtual bucket</b> for an aggregate id (HLD §4.3). The hash is computed in Java by
 * the core so {@code tandem-jdbc} (at insert) and {@code InMemoryOutbox} agree on the bucket, and so
 * the stored value is stable across DB engines and versions for the life of the data — there is no
 * DB-side hash function to keep in sync.
 */
public final class BucketHash {

    private static final long FNV1A_64_OFFSET_BASIS = 0xcbf29ce484222325L;
    private static final long FNV1A_64_PRIME = 0x100000001b3L;

    private BucketHash() {
    }

    /**
     * Virtual bucket for {@code aggregateId}: a 64-bit FNV-1a hash over its UTF-8 bytes, reduced with
     * {@link Math#floorMod(long, int)} into {@code [0, bucketCount)}.
     *
     * <p>{@code Math.floorMod} is used deliberately: it is always non-negative and overflow-free even
     * for {@link Long#MIN_VALUE} (unlike {@code abs(h) % n}, which overflows on the minimum value),
     * and it works for any {@code bucketCount}, not only powers of two.
     */
    public static int bucketFor(String aggregateId, int bucketCount) {
        if (bucketCount <= 0) {
            throw new IllegalArgumentException("bucketCount must be positive: " + bucketCount);
        }
        long h = FNV1A_64_OFFSET_BASIS;
        for (byte b : aggregateId.getBytes(StandardCharsets.UTF_8)) {
            h ^= (b & 0xff);
            h *= FNV1A_64_PRIME;
        }
        return Math.floorMod(h, bucketCount);
    }
}
