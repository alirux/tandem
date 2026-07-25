package com.codingful.tandem.sample.spring;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Component;

/**
 * [DEMO-ONLY] An in-memory stand-in for the application's own domain repository (a JPA repository, say).
 * It stores only each order's current version, so every {@link #load} returns a fresh {@link Order}
 * whose pending list starts empty — keeping the sample's seq numbering easy to follow.
 */
@Component
class OrderStore {

    private final Map<String, Long> versionsById = new ConcurrentHashMap<>();

    Order load(String id) {
        return new Order(id, versionsById.getOrDefault(id, 0L));
    }

    void save(Order order) {
        versionsById.put(order.id(), order.version());
    }
}
