package com.tripbus.realtime.ws;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.tripbus.realtime.config.RealtimeProperties;
import com.tripbus.realtime.snapshot.SnapshotService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtException;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.time.Clock;
import java.time.Instant;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Protocolo (JSON por mensaje):
 * <pre>
 *  cliente → {"type":"connect","token":"JWT"}        (si no envió ?token= en la URL)
 *  cliente → {"type":"connect"}                      (sin cuenta: solo temas públicos, US-039)
 *  cliente → {"type":"subscribe","topic":"itinerario:{id}:asientos"}
 *  cliente → {"type":"unsubscribe","topic":"..."}
 *  cliente → {"type":"ping"} | {"type":"pong"}
 *  servidor → connected | subscribed | snapshot | event | unsubscribed | ping | pong | error
 * </pre>
 * No tiene lógica de negocio: solo autentica, gestiona suscripciones y transporta.
 */
@Component
public class RealtimeHandler extends TextWebSocketHandler {

    public static final CloseStatus UNAUTHORIZED = new CloseStatus(4401, "No autenticado");
    public static final CloseStatus HEARTBEAT_TIMEOUT = new CloseStatus(4408, "Sin heartbeat");

    private static final Logger log = LoggerFactory.getLogger(RealtimeHandler.class);

    private final SessionRegistry registry;
    private final SnapshotService snapshots;
    private final JwtDecoder jwtDecoder;
    private final ObjectMapper objectMapper;
    private final RealtimeProperties properties;
    private final Clock clock = Clock.systemUTC();
    private final ExecutorService snapshotExecutor = Executors.newVirtualThreadPerTaskExecutor();

    public RealtimeHandler(SessionRegistry registry, SnapshotService snapshots, JwtDecoder jwtDecoder,
                           ObjectMapper objectMapper, RealtimeProperties properties) {
        this.registry = registry;
        this.snapshots = snapshots;
        this.jwtDecoder = jwtDecoder;
        this.objectMapper = objectMapper;
        this.properties = properties;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession raw) {
        ClientSession session = new ClientSession(raw, clock.instant());
        registry.add(session);
        Object jwt = raw.getAttributes().get(HandshakeAuthInterceptor.JWT_ATTRIBUTE);
        if (jwt instanceof Jwt token) {
            authenticate(session, token);
        }
    }

    @Override
    protected void handleTextMessage(WebSocketSession raw, TextMessage message) {
        ClientSession session = registry.get(raw.getId());
        if (session == null) {
            return;
        }
        session.touch(clock.instant());
        JsonNode msg;
        try {
            msg = objectMapper.readTree(message.getPayload());
        } catch (Exception e) {
            sendError(session, "INVALID_MESSAGE", "El mensaje debe ser JSON");
            return;
        }
        String type = msg.path("type").asText("").toLowerCase(Locale.ROOT);
        switch (type) {
            case "connect" -> connect(session, msg.path("token").asText(null));
            case "subscribe" -> subscribe(session, msg.path("topic").asText(null));
            case "unsubscribe" -> unsubscribe(session, msg.path("topic").asText(null));
            case "ping" -> session.send("{\"type\":\"pong\"}");
            case "pong" -> {
                // heartbeat respondido (touch ya registrado)
            }
            default -> sendError(session, "UNKNOWN_TYPE", "Tipo de mensaje no soportado: " + type);
        }
    }

    private void connect(ClientSession session, String token) {
        if (token == null || token.isBlank()) {
            connectAnonymously(session);
            return;
        }
        try {
            authenticate(session, jwtDecoder.decode(token));
        } catch (JwtException e) {
            sendError(session, "INVALID_TOKEN", "Token inválido o expirado");
            session.close(UNAUTHORIZED);
        }
    }

    private void authenticate(ClientSession session, Jwt jwt) {
        session.authenticate(jwt.getSubject(), jwt.getClaimAsString("role"), jwt.getExpiresAt());
        ObjectNode out = objectMapper.createObjectNode();
        out.put("type", "connected");
        out.put("userId", jwt.getSubject());
        out.put("role", jwt.getClaimAsString("role"));
        out.put("heartbeatSeconds", properties.heartbeatInterval().toSeconds());
        if (jwt.getExpiresAt() != null) {
            out.put("tokenExpiresAt", jwt.getExpiresAt().toString());
        }
        session.send(out.toString());
    }

    /**
     * Seguimiento público (US-039): connect sin token deja la sesión anónima. Solo puede seguir
     * temas públicos y no tiene token que venza; una sesión ya autenticada no se degrada.
     */
    private void connectAnonymously(ClientSession session) {
        if (session.authenticated()) {
            sendError(session, "ALREADY_AUTHENTICATED", "La conexión ya está autenticada");
            return;
        }
        session.connectAnonymously();
        ObjectNode out = objectMapper.createObjectNode();
        out.put("type", "connected");
        out.put("anonymous", true);
        out.put("heartbeatSeconds", properties.heartbeatInterval().toSeconds());
        out.put("maxSubscriptions", properties.maxAnonymousSubscriptions());
        session.send(out.toString());
    }

    private void subscribe(ClientSession session, String rawTopic) {
        if (!session.identified()) {
            sendError(session, "UNAUTHENTICATED",
                    "Primero envíe {\"type\":\"connect\"} (con \"token\" para una sesión autenticada)");
            return;
        }
        String topic = rawTopic == null ? null : rawTopic.trim().toLowerCase(Locale.ROOT);
        Topics.Parsed parsed = Topics.parse(topic);
        if (parsed == null) {
            sendError(session, "INVALID_TOPIC", "Tópico inválido: " + rawTopic);
            return;
        }
        if (session.anonymous() && !Topics.isPublic(topic)) {
            sendError(session, "FORBIDDEN_TOPIC", "Tópico no disponible sin cuenta: " + topic);
            return;
        }
        // Tema privado (US-033): solo su dueño, sin excepción por rol.
        if (parsed.kind() == Topics.Kind.TIQUETES && !Topics.isOwnedBy(parsed, session.userId())) {
            sendError(session, "FORBIDDEN_TOPIC", "Tópico de otro usuario: " + topic);
            return;
        }
        int max = session.anonymous() ? properties.maxAnonymousSubscriptions() : properties.maxSubscriptions();
        if (!session.topics().contains(topic) && session.topics().size() >= max) {
            sendError(session, "TOO_MANY_SUBSCRIPTIONS", "Máximo " + max + " suscripciones");
            return;
        }
        // Se suscribe ANTES de pedir el snapshot para no perder eventos intermedios.
        registry.subscribe(session, topic);
        session.send(objectMapper.createObjectNode().put("type", "subscribed").put("topic", topic).toString());
        if (!parsed.kind().hasSnapshot()) {
            return;
        }
        snapshotExecutor.submit(() -> {
            ObjectNode out = objectMapper.createObjectNode();
            out.put("type", "snapshot");
            out.put("topic", topic);
            snapshots.snapshot(parsed).ifPresentOrElse(
                    data -> out.set("data", data),
                    () -> {
                        out.putNull("data");
                        out.put("error", "SNAPSHOT_UNAVAILABLE");
                    });
            session.send(out.toString());
        });
    }

    private void unsubscribe(ClientSession session, String rawTopic) {
        String topic = rawTopic == null ? "" : rawTopic.trim().toLowerCase(Locale.ROOT);
        registry.unsubscribe(session, topic);
        session.send(objectMapper.createObjectNode().put("type", "unsubscribed").put("topic", topic).toString());
    }

    private void sendError(ClientSession session, String code, String message) {
        session.send(objectMapper.createObjectNode().put("type", "error").put("code", code)
                .put("message", message).toString());
    }

    @Override
    public void afterConnectionClosed(WebSocketSession raw, CloseStatus status) {
        registry.remove(raw.getId());
    }

    @Override
    public void handleTransportError(WebSocketSession raw, Throwable exception) {
        log.debug("Error de transporte en {}: {}", raw.getId(), exception.getMessage());
        registry.remove(raw.getId());
    }

    // ------------------------------------------------------------------ heartbeat

    /**
     * Ping cada 20 s; si no hay respuesta en 45 s se cierra. Token vencido → reconectar. Las
     * sesiones anónimas no tienen token: solo las cierra la falta de heartbeat.
     */
    @org.springframework.scheduling.annotation.Scheduled(fixedDelayString = "${tripbus.realtime.heartbeat-interval-ms:20000}")
    public void heartbeat() {
        Instant now = clock.instant();
        for (ClientSession session : registry.all()) {
            if (!session.identified()) {
                if (session.connectedAt().plus(properties.authTimeout()).isBefore(now)) {
                    sendError(session, "AUTH_TIMEOUT", "No se recibió el token a tiempo");
                    session.close(UNAUTHORIZED);
                    registry.remove(session.id());
                }
                continue;
            }
            if (session.lastSeen().plus(properties.heartbeatTimeout()).isBefore(now)) {
                session.close(HEARTBEAT_TIMEOUT);
                registry.remove(session.id());
                continue;
            }
            if (session.tokenExpiresAt() != null && session.tokenExpiresAt().isBefore(now)) {
                sendError(session, "TOKEN_EXPIRED", "El token expiró; reconecte con uno nuevo");
                session.close(UNAUTHORIZED);
                registry.remove(session.id());
                continue;
            }
            session.send("{\"type\":\"ping\"}");
        }
    }
}
