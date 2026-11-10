package com.tripbus.realtime.ws;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/** Conexiones y suscripciones activas de ESTA réplica. */
@Component
public class SessionRegistry {

    private final Map<String, ClientSession> sessions = new ConcurrentHashMap<>();
    private final Map<String, Set<ClientSession>> byTopic = new ConcurrentHashMap<>();
    private final List<Consumer<ClientSession>> removalListeners = new CopyOnWriteArrayList<>();

    public SessionRegistry(MeterRegistry meterRegistry) {
        Gauge.builder("tripbus.realtime.connections", sessions, Map::size).register(meterRegistry);
        Gauge.builder("tripbus.realtime.topics", byTopic, Map::size).register(meterRegistry);
    }

    /** {@code listener} se llama una vez por cada sesión que sale del registro (p. ej. para descontarla en Redis). */
    public void onRemove(Consumer<ClientSession> listener) {
        removalListeners.add(listener);
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
            removalListeners.forEach(listener -> listener.accept(session));
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

    /** Tópicos con al menos un suscriptor en esta réplica. */
    public Set<String> topics() {
        return byTopic.keySet();
    }

    /** Suscripciones de esta réplica a tópicos de {@code kind} (métrica tripbus_ws_suscripciones). */
    public int subscriptions(Topics.Kind kind) {
        int total = 0;
        for (Map.Entry<String, Set<ClientSession>> entry : byTopic.entrySet()) {
            Topics.Parsed parsed = Topics.parse(entry.getKey());
            if (parsed != null && parsed.kind() == kind) {
                total += entry.getValue().size();
            }
        }
        return total;
    }

    public Collection<ClientSession> all() {
        return sessions.values();
    }
}
