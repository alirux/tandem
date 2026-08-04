package com.codingful.tandem.admin.relay;

/** Thrown when a bucket number is outside {@code [0, bucketCount)} — no such {@code tandem_bucket_lease} row. */
final class RelayBucketNotFoundException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    RelayBucketNotFoundException(int bucket) {
        super("No such bucket: " + bucket);
    }
}
