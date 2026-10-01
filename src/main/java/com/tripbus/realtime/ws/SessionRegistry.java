package com.tripbus.realtime.ws;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Conexiones y suscripciones activas de ESTA réplica. */
@Component
public class SessionRegistry {

    private final Map<String, ClientSession> sessions = new ConcurrentHashMap<>();
    private final Map<String, Set<ClientSession>> byTopic = new ConcurrentHashMap<>();

    public SessionRegistry(MeterRegistry meterRegistry) {
        Gauge.builder("tripbus.realtime.connections", sessions, Map::size).register(meterRegistry);
        Gauge.builder("tripbus.realtime.topics", byTopic, Map::size).register(meterRegistry);
    }

    public void add(ClientSession session) {
        sessions.put(session.id(), session);
    }

    public ClientSession get(String sessionId) {
        return sessions.get(sessionId);
    }

    public void remove(String sessionId) {
        ClientSession session = sessions.remove(sessionId);
        if (session != null) {
            for (String topic : session.topics()) {
                unsubscribe(session, topic);
            }
        }
    }

    public void subscribe(ClientSession session, String topic) {
        session.topics().add(topic);
        byTopic.computeIfAbsent(topic, t -> ConcurrentHashMap.newKeySet()).add(session);
    }

    public void unsubscribe(ClientSession session, String topic) {
        session.topics().remove(topic);
        byTopic.computeIfPresent(topic, (t, set) -> {
            set.remove(session);
            return set.isEmpty() ? null : set;
        });
    }

    public Set<ClientSession> subscribers(String topic) {
        return byTopic.getOrDefault(topic, Set.of());
    }

    public Collection<ClientSession> all() {
        return sessions.values();
    }
}
