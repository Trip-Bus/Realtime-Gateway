package com.tripbus.realtime.events;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.tripbus.realtime.config.RealtimeProperties;
import com.tripbus.realtime.ws.ClientSession;
import com.tripbus.realtime.ws.RealtimeHandler;
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
import org.springframework.web.socket.CloseStatus;

import java.time.Duration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Lee TODOS los eventos del bus (sin consumer group: cada réplica recibe todo, como un
 * Pub/Sub) y los difunde a las conexiones suscritas al tópico correspondiente (US-021,
 * US-047, US-056). Los tópicos públicos (US-039: los siguen también sesiones anónimas) nunca
 * reciben datos de una persona: ni el userId, email, documento o nombre de un pasajero, ni los ids de
 * su retención o reserva, ni el conductorId, ni el texto libre de un SOS. El aviso privado
 * de tiquete (US-033) lleva solo bookingId, ticketId, itineraryId y seatNumbers: nunca qrCode ni userId.
 * Los avisos del pasajero (US-183) llevan solo identificadores y datos de la decisión. El canal del
 * conductor y el de alertas de la empresa sí llevan conductorId, pero nunca datos de pasajeros.
 *
 * <p>Orden: los eventos se envían en el orden del stream, también un TripAlertRaised CRITICA (SOS,
 * ACCIDENTE). No hay una cola propia que reordenar: cada envío va directo al buffer del
 * ConcurrentWebSocketSessionDecorator, así que adelantarlo exigiría reescribir el envío.
 */
@Component
public class EventFanout implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(EventFanout.class);
    /** Datos de pasajeros: no salen en ningún tópico salvo el propio aviso del pasajero (y ahí solo ids). */
    private static final Set<String> PERSONAL_FIELDS = Set.of("userId", "holdId", "email", "numeroDocumento", "nombre",
            "qrCode");
    /** Lo que se quita de los tópicos públicos, a cualquier profundidad. */
    private static final Set<String> PRIVATE_FIELDS = union(PERSONAL_FIELDS, Set.of("bookingId", "conductorId", "mensaje"));
    /** Las paradas (EtaUpdated, posición) tienen nombre de lugar, no de persona: ahí se conserva "nombre". */
    private static final String STOPS = "paradas";
    private static final String NAME = "nombre";
    /** Texto libre de una alerta SOS: se quita si el objeto es un SOS (tipo = SOS). */
    private static final String SOS_REASON = "motivo";
    private static final List<String> TICKET_NOTICE_FIELDS = List.of("bookingId", "ticketId", "itineraryId", "seatNumbers");
    private static final List<String> USER_NOTICE_FIELDS = List.of("bookingId", "bookingIdAnterior", "bookingIdNuevo",
            "ticketId", "ticketIdAnterior", "ticketIdNuevo", "itineraryId", "itineraryIdAnterior", "itineraryIdNuevo",
            "refundId", "seatNumbers", "cambio", "causa", "plazoDecision", "opciones");
    private static final Set<String> REVOCATIONS = Set.of("SessionRevoked", "UserSuspended");

    private final RedisConnectionFactory connectionFactory;
    private final StringRedisTemplate redis;
    private final SessionRegistry registry;
    private final ObjectMapper objectMapper;
    private final RealtimeProperties properties;
    private final Counter delivered;
    private StreamMessageListenerContainer<String, MapRecord<String, String, String>> container;
    private volatile boolean running;
    private volatile String lastSeq = "0-0";

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
        lastSeq = currentLastId();
        var request = StreamMessageListenerContainer.StreamReadRequest
                .builder(StreamOffset.create(properties.eventsStream(), ReadOffset.from(lastSeq)))
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

    /**
     * Id de la última entrada del stream ya difundida (US-006): el snapshot lo lleva como asOfSeq.
     * "0-0" antes de arrancar o con el stream vacío.
     */
    public String lastSeq() {
        return lastSeq;
    }

    void onEvent(MapRecord<String, String, String> record) {
        try {
            fanOut(record);
        } finally {
            lastSeq = record.getId().getValue();
        }
    }

    private void fanOut(MapRecord<String, String, String> record) {
        Map<String, String> fields = record.getValue();
        String type = fields.get("type");
        if (type == null) {
            return;
        }
        try {
            JsonNode payload = objectMapper.readTree(fields.getOrDefault("payload", "{}"));
            if (REVOCATIONS.contains(type)) {
                revoke(type, payload);
                return;
            }
            List<String> topics = Topics.forEvent(type, payload);
            if (topics.isEmpty() && Topics.isUserNotice(type)) {
                // Booking debe mandar el userId del dueño: sin él no hay a quién avisar.
                log.warn("Evento {} {} sin userId: no se difunde", type, fields.get("eventId"));
            }
            for (String topic : topics) {
                Set<ClientSession> subscribers = registry.subscribers(topic);
                // Un id que no es uuid (p. ej. userId mal formado) da un tópico que nadie puede seguir.
                Topics.Parsed parsed = Topics.parse(topic);
                if (subscribers.isEmpty() || parsed == null) {
                    continue;
                }
                ObjectNode out = objectMapper.createObjectNode();
                out.put("type", "event");
                out.put("topic", topic);
                out.put("event", type);
                out.put("seq", record.getId().getValue());
                out.put("eventId", fields.get("eventId"));
                out.put("occurredAt", fields.get("occurredAt"));
                out.set("data", data(parsed.kind(), payload));
                String json = out.toString();
                for (ClientSession session : subscribers) {
                    deliver(session, json);
                }
            }
        } catch (Exception e) {
            log.warn("Evento {} no difundido: {}", type, e.getMessage());
        }
    }

    /**
     * Logout o suspensión publicados por identity (US-006): cierra con 4401 las sesiones de ese token
     * (SessionRevoked, por jti) o de ese usuario (UserSuspended, por userId). No se reenvían a ningún
     * tema y las sesiones anónimas no se tocan.
     */
    private void revoke(String type, JsonNode payload) {
        boolean byToken = "SessionRevoked".equals(type);
        String value = payload.path(byToken ? "jti" : "userId").asText(null);
        if (value == null || value.isBlank()) {
            return;
        }
        for (ClientSession session : registry.all()) {
            String current = byToken ? session.jti() : session.userId();
            if (session.authenticated() && value.equals(current)) {
                session.send(objectMapper.createObjectNode().put("type", "error").put("code", "UNAUTHENTICATED")
                        .put("message", "La sesión fue cerrada; inicie sesión de nuevo").toString());
                session.close(RealtimeHandler.UNAUTHORIZED);
                registry.remove(session.id());
            }
        }
    }

    /** Un fallo con un suscriptor nunca impide enviar al siguiente (US-031). */
    private void deliver(ClientSession session, String json) {
        try {
            if (session.send(json)) {
                delivered.increment();
                return;
            }
        } catch (RuntimeException e) {
            log.warn("Fallo inesperado enviando a la sesión {} ({})", session.id(), e.getClass().getSimpleName());
            session.close(CloseStatus.SESSION_NOT_RELIABLE);
        }
        // La sesión fallida o ya cerrada deja de recibir eventos aunque el cierre tarde.
        registry.remove(session.id());
    }

    /**
     * Lo que ve el cliente según el tópico: los avisos al pasajero se arman campo a campo; el canal del
     * conductor y el de la empresa, sin datos de pasajeros; los públicos, sin ningún campo privado.
     */
    static JsonNode data(Topics.Kind kind, JsonNode payload) {
        return switch (kind) {
            case TIQUETES -> pick(payload, TICKET_NOTICE_FIELDS);
            case AVISOS -> strip(pick(payload, USER_NOTICE_FIELDS), PERSONAL_FIELDS, false);
            // El operador sí ve el motivo de un SOS.
            case VIAJE_CONDUCTOR, ALERTAS -> strip(payload.deepCopy(), PERSONAL_FIELDS, false);
            default -> sanitize(payload);
        };
    }

    private static ObjectNode pick(JsonNode payload, List<String> allowed) {
        ObjectNode notice = JsonNodeFactory.instance.objectNode();
        for (String field : allowed) {
            if (payload.has(field)) {
                notice.set(field, payload.get(field).deepCopy());
            }
        }
        return notice;
    }

    /** Copia sin campos privados a cualquier profundidad (p. ej. el detalle libre de TripEventReported). */
    static JsonNode sanitize(JsonNode payload) {
        return strip(payload.deepCopy(), PRIVATE_FIELDS, true);
    }

    /** Quita {@code remove} a cualquier profundidad; con {@code hideSosReason}, también el motivo de un SOS. */
    private static JsonNode strip(JsonNode node, Set<String> remove, boolean hideSosReason) {
        if (node instanceof ObjectNode object) {
            boolean sos = hideSosReason && "SOS".equals(object.path("tipo").asText());
            Iterator<Map.Entry<String, JsonNode>> fields = object.properties().iterator();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                String key = field.getKey();
                if (remove.contains(key) || (sos && SOS_REASON.equals(key))) {
                    fields.remove();
                } else {
                    strip(field.getValue(), STOPS.equals(key) ? without(remove, NAME) : remove, hideSosReason);
                }
            }
        } else if (node instanceof ArrayNode array) {
            array.forEach(item -> strip(item, remove, hideSosReason));
        }
        return node;
    }

    private static Set<String> without(Set<String> fields, String field) {
        Set<String> copy = new HashSet<>(fields);
        copy.remove(field);
        return copy;
    }

    private static Set<String> union(Set<String> a, Set<String> b) {
        Set<String> all = new HashSet<>(a);
        all.addAll(b);
        return Set.copyOf(all);
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
