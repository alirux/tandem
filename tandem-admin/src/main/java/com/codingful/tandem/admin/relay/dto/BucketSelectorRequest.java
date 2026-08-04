package com.codingful.tandem.admin.relay.dto;

/** Wire representation of the OpenAPI {@code BucketSelector} schema. {@code null} — or no body at all — means the whole relay. */
public record BucketSelectorRequest(Integer bucket) {
}
