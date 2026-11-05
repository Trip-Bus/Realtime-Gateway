package com.tripbus.realtime.events;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.tripbus.realtime.config.RealtimeProperties;
import com.tripbus.realtime.config.TraceContexts;
import com.tripbus.realtime.ws.ClientSession;
import com.tripbus.realtime.ws.RealtimeHandler;
import com.tripbus.realtime.ws.SessionRegistry;
import com.tripbus.realtime.ws.Topics;
import com.tripbus.realtime.ws.WsMetrics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.SmartLifecycle;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;

import java.time.Clock;
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
 * Los avisos del pasajero (US-183, US-140, US-153, US-161) llevan solo identificadores y datos de la
 * decisión u oferta: nunca la retención ni los userIds de otros turnos de la lista de espera. El canal
 * del conductor y los de la empresa (alertas y flota) sí llevan conductorId, pero nunca datos de pasajeros.
 *
 * <p>Frecuencia: la vitrina ({@code ofertas:activas}) recibe como máximo un OfferQuotaUpdated por
 * segundo por oferta y la flota ({@code empresa:{id}:flota}) una PositionUpdated cada 3 s por viaje; lo
 * demás se descarta (oferta:{id}:cupo y viaje:{id}:posicion siguen recibiéndolo todo).
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
    private static final String ITINERARY_ID = "itineraryId";
    private static final String OFFER_ID = "offerId";
    /** Lo que se quita de los tópicos públicos, a cualquier profundidad. */
    private static final Set<String> PRIVATE_FIELDS = union(PERSONAL_FIELDS, Set.of("bookingId", "conductorId", "mensaje"));
    /** Las paradas (EtaUpdated, posición) tienen nombre de lugar, no de persona: ahí se conserva "nombre". */
    private static final String STOPS = "paradas";
    private static final String NAME = "nombre";
    /** Texto libre de una alerta SOS: se quita si el objeto es un SOS (tipo = SOS). */
    private static final String SOS_REASON = "motivo";
    private static final List<String> TICKET_NOTICE_FIELDS = List.of("bookingId", "ticketId", ITINERARY_ID, "seatNumbers");
    private static final List<String> USER_NOTICE_FIELDS = List.of("bookingId", "bookingIdAnterior", "bookingIdNuevo",
            "ticketId", "ticketIdAnterior", "ticketIdNuevo", ITINERARY_ID, "itineraryIdAnterior", "itineraryIdNuevo",
            "refundId", "seatNumbers", "cambio", "causa", "plazoDecision", "opciones",
            // Lista de espera (US-140): hasta cuándo se le guardan los asientos ofrecidos.
            "expiraEn");
    /** Alerta de oferta (US-161): la oferta que coincide con la ruta que sigue el pasajero. */
    private static final String OFFER_ALERT = "OfferAlert";
    private static final List<String> OFFER_ALERT_FIELDS = List.of(OFFER_ID, ITINERARY_ID, "origen", "destino",
            "precioOferta", "precioBase", "inicio", "fin", "salida");
    /** Sala de espera (US-162): el pasajero ya puede comprar en la oferta; hasta cuándo y en qué turno. */
    private static final String OFFER_QUEUE_ADMITTED = "OfferQueueAdmitted";
    private static final List<String> OFFER_QUEUE_ADMITTED_FIELDS = List.of(OFFER_ID, ITINERARY_ID, "turno",
            "admitidoEn", "expiraEn");
    private static final Set<String> REVOCATIONS = Set.of("SessionRevoked", "UserSuspended");
    private static final String QUOTA_UPDATED = "OfferQuotaUpdated";
    private static final String POSITION_UPDATED = "PositionUpdated";

    private final RedisConnectionFactory connectionFactory;
    private final StringRedisTemplate redis;
    private final SessionRegistry registry;
    private final ObjectMapper objectMapper;
    private final RealtimeProperties properties;
    private final Counter delivered;
    private final ReplayBuffer replay;
    private final WsMetrics metrics;
    private final TraceContexts tracing;
    private final ObjectProvider<DomainEventListener> listeners;
    private Clock clock = Clock.systemUTC();
    private final Throttle showcaseQuota = new Throttle(Duration.ofSeconds(1));
    private final Throttle fleetPositions = new Throttle(Duration.ofSeconds(3));
    private StreamMessageListenerContainer<String, MapRecord<String, String, String>> container;
    private volatile boolean running;
    private volatile String lastSeq = "0-0";

    /**
     * @param replay    búfer de reconexión (US-184): cada mensaje difundido se guarda, haya o no suscriptores
     *                  en esta réplica (el cliente puede reconectar a otra)
     * @param listeners los que reaccionan a cada evento además de la difusión (p. ej. el Web Push, US-185)
     */
    @Autowired
    public EventFanout(RedisConnectionFactory connectionFactory, StringRedisTemplate redis, SessionRegistry registry,
                       ObjectMapper objectMapper, RealtimeProperties properties, MeterRegistry meterRegistry,
                       ReplayBuffer replay, WsMetrics metrics, TraceContexts tracing,
                       ObjectProvider<DomainEventListener> listeners) {
        this.connectionFactory = connectionFactory;
        this.redis = redis;
        this.registry = registry;
        this.objectMapper = objectMapper;
        this.properties = properties;
        this.delivered = Counter.builder("tripbus.realtime.messages.delivered").register(meterRegistry);
        this.replay = replay;
        this.metrics = metrics;
        this.tracing = tracing;
        this.listeners = listeners;
    }

    /** Reloj de los límites de frecuencia (pruebas). */
    void useClock(Clock clock) {
        this.clock = clock;
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
            // HT-10: la difusión continúa la traza del servicio que publicó el evento (campo traceparent).
            tracing.run("tripbus.ws.difundir", record.getValue().get(TraceContexts.TRACEPARENT), () -> fanOut(record));
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
        String seq = record.getId().getValue();
        try {
            JsonNode payload = objectMapper.readTree(fields.getOrDefault("payload", "{}"));
            if (REVOCATIONS.contains(type)) {
                revoke(type, payload);
                return;
            }
            List<String> topics = Topics.forEvent(type, payload);
            if (topics.isEmpty() && Topics.isUserNotice(type)) {
                // El servicio dueño debe mandar el userId (o los userIds): sin él no hay a quién avisar.
                log.warn("Evento {} {} sin userId: no se difunde", type, fields.get("eventId"));
            }
            for (String topic : topics) {
                // Un id que no es uuid (p. ej. userId mal formado) da un tópico que nadie puede seguir.
                Topics.Parsed parsed = Topics.parse(topic);
                if (parsed == null || throttled(parsed.kind(), type, topic, payload)) {
                    continue;
                }
                ObjectNode out = objectMapper.createObjectNode();
                out.put("type", "event");
                out.put("topic", topic);
                out.put("event", type);
                out.put("seq", seq);
                out.put("eventId", fields.get("eventId"));
                out.put("occurredAt", fields.get("occurredAt"));
                out.set("data", data(parsed.kind(), type, payload));
                String json = out.toString();
                replay.append(topic, seq, json);
                for (ClientSession session : registry.subscribers(topic)) {
                    deliver(session, topic, parsed.kind(), seq, json);
                }
            }
            notifyListeners(type, seq, fields.get("eventId"), payload);
        } catch (Exception e) {
            log.warn("Evento {} no difundido: {}", type, e.getMessage());
        }
    }

    /** Un listener que falla no afecta la difusión ni a los demás listeners. */
    private void notifyListeners(String type, String seq, String eventId, JsonNode payload) {
        listeners.orderedStream().forEach(listener -> {
            try {
                listener.onEvent(type, seq, eventId, payload);
            } catch (RuntimeException e) {
                log.warn("Listener {} falló con el evento {} ({})", listener.getClass().getSimpleName(), type,
                        e.getClass().getSimpleName());
            }
        });
    }

    /**
     * true si el evento se descarta en este tópico por el límite de frecuencia: OfferQuotaUpdated en la
     * vitrina (uno por segundo por oferta) y PositionUpdated en la flota (uno cada 3 s por viaje).
     */
    private boolean throttled(Topics.Kind kind, String type, String topic, JsonNode payload) {
        if (kind == Topics.Kind.OFERTAS_ACTIVAS && QUOTA_UPDATED.equals(type)) {
            return !showcaseQuota.allow(topic + "|" + payload.path(OFFER_ID).asText(), clock.instant());
        }
        if (kind == Topics.Kind.FLOTA && POSITION_UPDATED.equals(type)) {
            return !fleetPositions.allow(topic + "|" + payload.path(ITINERARY_ID).asText(), clock.instant());
        }
        return false;
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
    private void deliver(ClientSession session, String topic, Topics.Kind kind, String seq, String json) {
        try {
            if (session.sendEvent(topic, seq, json)) {
                delivered.increment();
                metrics.sent(kind);
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
    static JsonNode data(Topics.Kind kind, String type, JsonNode payload) {
        return switch (kind) {
            case TIQUETES -> pick(payload, TICKET_NOTICE_FIELDS);
            case AVISOS -> strip(pick(payload, noticeFields(type)), PERSONAL_FIELDS, false);
            // El operador sí ve el motivo de un SOS.
            case VIAJE_CONDUCTOR, ALERTAS, FLOTA -> strip(payload.deepCopy(), PERSONAL_FIELDS, false);
            default -> sanitize(payload);
        };
    }

    private static List<String> noticeFields(String type) {
        if (OFFER_ALERT.equals(type)) {
            return OFFER_ALERT_FIELDS;
        }
        return OFFER_QUEUE_ADMITTED.equals(type) ? OFFER_QUEUE_ADMITTED_FIELDS : USER_NOTICE_FIELDS;
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
