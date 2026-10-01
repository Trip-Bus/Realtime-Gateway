package com.tripbus.realtime.events;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.tripbus.realtime.config.RealtimeProperties;
import com.tripbus.realtime.ws.ClientSession;
import com.tripbus.realtime.ws.SessionRegistry;
import com.tripbus.realtime.ws.Topics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Lee TODOS los eventos del bus (sin consumer group: cada réplica recibe todo, como un
 * Pub/Sub) y los difunde a las conexiones suscritas al tópico correspondiente (US-021,
 * US-047, US-056). Nunca reenvía el userId de otros pasajeros.
 */
@Component
public class EventFanout implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(EventFanout.class);
    private static final Set<String> PRIVATE_FIELDS = Set.of("userId");

    private final RedisConnectionFactory connectionFactory;
    private final StringRedisTemplate redis;
    private final SessionRegistry registry;
    private final ObjectMapper objectMapper;
    private final RealtimeProperties properties;
    private final Counter delivered;
    private StreamMessageListenerContainer<String, MapRecord<String, String, String>> container;
    private volatile boolean running;

    public EventFanout(RedisConnectionFactory connectionFactory, StringRedisTemplate redis, SessionRegistry registry,
                       ObjectMapper objectMapper, RealtimeProperties properties, MeterRegistry meterRegistry) {
        this.connectionFactory = connectionFactory;
        this.redis = redis;
        this.registry = registry;
        this.objectMapper = objectMapper;
        this.properties = properties;
        this.delivered = Counter.builder("tripbus.realtime.messages.delivered").register(meterRegistry);
    }

    @Override
    public void start() {
        var options = StreamMessageListenerContainer.StreamMessageListenerContainerOptions.builder()
                .pollTimeout(Duration.ofSeconds(1))
                .batchSize(200)
                .build();
        container = StreamMessageListenerContainer.create(connectionFactory, options);
        // Se parte del último id existente y luego el contenedor avanza id por id. (Con
        // ReadOffset.latest() cada lectura usaría "$" y se perderían eventos que llegan
        // casi al mismo tiempo, p. ej. SeatStateChanged + SeatHeld de una misma retención.)
        var request = StreamMessageListenerContainer.StreamReadRequest
                .builder(StreamOffset.create(properties.eventsStream(), ReadOffset.from(currentLastId())))
                .cancelOnError(t -> false)
                .errorHandler(t -> log.warn("Error leyendo eventos: {}", t.getMessage()))
                .build();
        container.register(request, this::onEvent);
        container.start();
        running = true;
        log.info("Difundiendo eventos de {}", properties.eventsStream());
    }

    private String currentLastId() {
        try {
            var last = redis.opsForStream().reverseRange(properties.eventsStream(),
                    org.springframework.data.domain.Range.unbounded(),
                    org.springframework.data.redis.connection.Limit.limit().count(1));
            if (last != null && !last.isEmpty()) {
                return last.get(0).getId().getValue();
            }
        } catch (RuntimeException e) {
            log.warn("No se pudo leer el último id del stream: {}", e.getMessage());
        }
        return "0-0";
    }

    void onEvent(MapRecord<String, String, String> record) {
        Map<String, String> fields = record.getValue();
        String type = fields.get("type");
        if (type == null) {
            return;
        }
        try {
            JsonNode payload = objectMapper.readTree(fields.getOrDefault("payload", "{}"));
            for (String topic : Topics.forEvent(type, payload)) {
                Set<ClientSession> subscribers = registry.subscribers(topic);
                if (subscribers.isEmpty()) {
                    continue;
                }
                ObjectNode out = objectMapper.createObjectNode();
                out.put("type", "event");
                out.put("topic", topic);
                out.put("event", type);
                out.put("eventId", fields.get("eventId"));
                out.put("occurredAt", fields.get("occurredAt"));
                out.set("data", sanitize(payload));
                String json = out.toString();
                for (ClientSession session : subscribers) {
                    session.send(json);
                    delivered.increment();
                }
            }
        } catch (Exception e) {
            log.warn("Evento {} no difundido: {}", type, e.getMessage());
        }
    }

    private static JsonNode sanitize(JsonNode payload) {
        if (payload instanceof ObjectNode object) {
            ObjectNode copy = object.deepCopy();
            Iterator<String> names = copy.fieldNames();
            while (names.hasNext()) {
                if (PRIVATE_FIELDS.contains(names.next())) {
                    names.remove();
                }
            }
            return copy;
        }
        return payload;
    }

    @Override
    public void stop() {
        if (container != null) {
            container.stop();
        }
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}
