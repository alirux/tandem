package com.codingful.tandem.core.port;

import com.codingful.tandem.core.OutboxRecord;
import java.util.Objects;

/**
 * Maps a record to its destination topic (LLD-core §2.4). The routing source field is
 * {@code aggregate_type} (not the finer CloudEvents {@code type}; LLD-kafka §5).
 */
public interface TopicRouter {

    /**
     * @param record the row about to be published
     * @return the destination Kafka topic name
     */
    String topicFor(OutboxRecord record);

    /**
     * The default router (Q18, LLD-kafka §5): {@code kebab-case(aggregateType) + suffix}, no
     * pluralization — e.g. {@code Order} → {@code order-topic}, {@code OrderLine} → {@code order-line-topic}.
     * The kebab transform is pure, so it lives in the dependency-free core; {@code tandem-kafka} wires
     * it as the default with the configured suffix.
     */
    static TopicRouter kebabWithSuffix(String suffix) {
        Objects.requireNonNull(suffix, "suffix");
        return record -> kebabCase(record.aggregateType()) + suffix;
    }

    /** {@code OrderLine} → {@code order-line}; inserts a hyphen at camel-case boundaries and lower-cases. */
    static String kebabCase(String value) {
        StringBuilder out = new StringBuilder(value.length() + 4);
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (Character.isUpperCase(c)) {
                boolean prevIsLower = i > 0 && !Character.isUpperCase(value.charAt(i - 1));
                boolean nextIsLower = i + 1 < value.length() && !Character.isUpperCase(value.charAt(i + 1));
                if (i > 0 && (prevIsLower || nextIsLower)) {
                    out.append('-');
                }
                out.append(Character.toLowerCase(c));
            } else {
                out.append(c);
            }
        }
        return out.toString();
    }
}
