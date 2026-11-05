package com.tripbus.realtime.ws;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.tripbus.realtime.config.RealtimeLimits;
import com.tripbus.realtime.config.RealtimeProperties;
import com.tripbus.realtime.config.TraceContexts;
import com.tripbus.realtime.events.EventFanout;
import com.tripbus.realtime.events.ReplayBuffer;
import com.tripbus.realtime.snapshot.SnapshotService;
import com.tripbus.realtime.ws.ClientSession.ReplayEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtException;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.net.InetSocketAddress;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;

/**
 * Protocolo (JSON por mensaje):
 * <pre>
 *  cliente → {"type":"connect","token":"JWT"}        (si no envió ?token= en la URL)
 *  cliente → {"type":"connect"}                      (sin cuenta: solo temas públicos, US-039)
 *  cliente → {"type":"subscribe","topic":"itinerario:{id}:asientos"}
 *  cliente → {"type":"resume","topic":"...","lastSeq":"<ms>-<n>"}   (reconexión, US-184)
 *  cliente → {"type":"unsubscribe","topic":"..."}
 *  cliente → {"type":"ping"} | {"type":"pong"}
 *  servidor → connected | subscribed | snapshot | event | resumed | unsubscribed | ping | pong | error
 * </pre>
 * Cada event lleva seq (id de la entrada del stream) y cada snapshot asOfSeq: ver README.
 * No tiene lógica de negocio: solo autentica, gestiona suscripciones y transporta.
 */
@Component
public class RealtimeHandler extends TextWebSocketHandler {

    public static final CloseStatus UNAUTHORIZED = new CloseStatus(4401, "No autenticado");
    public static final CloseStatus HEARTBEAT_TIMEOUT = new CloseStatus(4408, "Sin heartbeat");
    /** HT-11: la IP o el usuario ya tienen el máximo de conexiones abiertas. */
    public static final CloseStatus TOO_MANY_CONNECTIONS = new CloseStatus(4429, "Demasiadas conexiones");

    private static final Logger log = LoggerFactory.getLogger(RealtimeHandler.class);
    private static final String UNAUTHENTICATED = "UNAUTHENTICATED";
    private static final String SESSION_CLOSED = "La sesión fue cerrada; inicie sesión de nuevo";
    private static final String TOPIC = "topic";
    private static final String TRACEPARENT = "traceparent";
    /** Una IP (v4 o v6) del header del proxy; cualquier otra cosa se ignora y se usa la de la conexión. */
    private static final Pattern IP = Pattern.compile("[0-9A-Fa-f:.]{2,45}");

    private final SessionRegistry registry;
    private final SnapshotService snapshots;
    private final JwtDecoder jwtDecoder;
    private final ObjectMapper objectMapper;
    private final RealtimeProperties properties;
    private final TokenRevocations revocations;
    private final EventFanout fanout;
    private final RealtimeLimits limits;
    private final ConnectionPresence presence;
    private final ReplayBuffer replay;
    private final WsMetrics metrics;
    private final TraceContexts tracing;
    private final Clock clock = Clock.systemUTC();
    private final ExecutorService snapshotExecutor = Executors.newVirtualThreadPerTaskExecutor();

    /** Tópico ya validado y autorizado para la sesión. */
    private record Target(String topic, Topics.Parsed parsed) {
    }

    @Autowired
    public RealtimeHandler(SessionRegistry registry, SnapshotService snapshots, JwtDecoder jwtDecoder,
                           ObjectMapper objectMapper, RealtimeProperties properties, TokenRevocations revocations,
                           EventFanout fanout, RealtimeLimits limits, ConnectionPresence presence,
                           ReplayBuffer replay, WsMetrics metrics, TraceContexts tracing) {
        this.registry = registry;
        this.snapshots = snapshots;
        this.jwtDecoder = jwtDecoder;
        this.objectMapper = objectMapper;
        this.properties = properties;
        this.revocations = revocations;
        this.fanout = fanout;
        this.limits = limits;
        this.presence = presence;
        this.replay = replay;
        this.metrics = metrics;
        this.tracing = tracing;
        // Toda sesión que sale del registro (cierre, rechazo, revocación) deja de contar en Redis.
        registry.onRemove(presence::release);
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession raw) {
        ClientSession session = new ClientSession(raw, clock.instant(), clientIp(raw));
        registry.add(session);
        if (!presence.acquireIp(session)) {
            tooManyConnections(session);
            return;
        }
        Object jwt = raw.getAttributes().get(HandshakeAuthInterceptor.JWT_ATTRIBUTE);
        if (jwt instanceof Jwt token) {
            authenticate(session, token);
        }
    }

    /**
     * IP del cliente: el último valor de {@code client-ip-header} (lo agrega el proxy de confianza; lo anterior
     * lo pudo escribir el cliente) o, sin header configurado o con un valor que no es una IP, la de la conexión.
     */
    private String clientIp(WebSocketSession raw) {
        HttpHeaders headers = raw.getHandshakeHeaders();
        if (!limits.clientIpHeader().isEmpty() && headers != null) {
            List<String> values = headers.get(limits.clientIpHeader());
            if (values != null && !values.isEmpty()) {
                String[] hops = values.get(values.size() - 1).split(",");
                String ip = hops[hops.length - 1].trim();
                if (IP.matcher(ip).matches()) {
                    return ip;
                }
            }
        }
        InetSocketAddress remote = raw.getRemoteAddress();
        return remote == null ? null : remote.getHostString();
    }

    @Override
    protected void handleTextMessage(WebSocketSession raw, TextMessage message) {
        ClientSession session = registry.get(raw.getId());
        if (session == null) {
            return;
        }
        Instant now = clock.instant();
        session.touch(now);
        if (!session.allowMessage(now, limits.maxClientMessages(), limits.clientMessageWindow())) {
            sendError(session, "RATE_LIMITED", "Máximo " + limits.maxClientMessages() + " mensajes cada "
                    + limits.clientMessageWindow().toSeconds() + " s; este se descartó");
            return;
        }
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
            case "subscribe" -> subscribe(session, msg.path(TOPIC).asText(null), msg.path(TRACEPARENT).asText(null));
            case "resume" -> resume(session, msg);
            case "unsubscribe" -> unsubscribe(session, msg.path(TOPIC).asText(null));
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

    /**
     * Token revocado por logout (US-006) ⇒ UNAUTHENTICATED y cierre 4401. La sesión guarda el jti
     * ANTES de consultar la revocación: un SessionRevoked que llegue entre la consulta y el registro
     * igual la encuentra (identity guarda la clave antes de publicar el evento). Cuenta suspendida
     * (US-104, {@code tripbus:suspended:{sub}}) ⇒ ACCOUNT_SUSPENDED y el mismo cierre 4401. Token
     * emitido antes de un cambio de contraseña (US-129, {@code tripbus:pwd-changed:{sub}}) ⇒ igual que
     * uno revocado. El usuario ya tiene {@code max-connections-per-user} abiertas (HT-11) ⇒
     * TOO_MANY_CONNECTIONS y cierre 4429.
     */
    private void authenticate(ClientSession session, Jwt jwt) {
        session.authenticate(jwt.getSubject(), jwt.getId(), jwt.getClaimAsString("role"),
                jwt.getClaimAsString("companyId"), jwt.getIssuedAt(), jwt.getExpiresAt());
        if (revocations.isRevoked(jwt.getId())) {
            reject(session, UNAUTHENTICATED, SESSION_CLOSED);
            return;
        }
        if (revocations.isSuspended(jwt.getSubject())) {
            reject(session, "ACCOUNT_SUSPENDED", "La cuenta está suspendida");
            return;
        }
        if (revocations.isPasswordChangedSince(jwt.getSubject(), jwt.getIssuedAt())) {
            reject(session, UNAUTHENTICATED, SESSION_CLOSED);
            return;
        }
        if (!presence.acquireUser(session, jwt.getSubject())) {
            tooManyConnections(session);
            return;
        }
        ObjectNode out = objectMapper.createObjectNode();
        out.put("type", "connected");
        out.put("userId", jwt.getSubject());
        out.put("role", jwt.getClaimAsString("role"));
        out.put("heartbeatSeconds", properties.heartbeatInterval().toSeconds());
        out.put("maxSubscriptions", properties.maxSubscriptionsFor(session.role()));
        if (jwt.getExpiresAt() != null) {
            out.put("tokenExpiresAt", jwt.getExpiresAt().toString());
        }
        session.send(out.toString());
    }

    private void reject(ClientSession session, String code, String message) {
        sendError(session, code, message);
        session.close(UNAUTHORIZED);
        registry.remove(session.id());
    }

    private void tooManyConnections(ClientSession session) {
        sendError(session, "TOO_MANY_CONNECTIONS", "Hay demasiadas conexiones abiertas; cierre alguna e intente de nuevo");
        session.close(TOO_MANY_CONNECTIONS);
        registry.remove(session.id());
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

    /**
     * Valida el tópico y la autorización de la sesión (los mismos para subscribe y resume); si no pasa,
     * responde el error y devuelve null.
     */
    private Target authorize(ClientSession session, String rawTopic) {
        if (!session.identified()) {
            sendError(session, UNAUTHENTICATED,
                    "Primero envíe {\"type\":\"connect\"} (con \"token\" para una sesión autenticada)");
            return null;
        }
        String topic = rawTopic == null ? null : rawTopic.trim().toLowerCase(Locale.ROOT);
        Topics.Parsed parsed = Topics.parse(topic);
        if (parsed == null) {
            sendError(session, "INVALID_TOPIC", "Tópico inválido: " + rawTopic);
            return null;
        }
        if (session.anonymous() && !Topics.isPublic(topic)) {
            sendError(session, "FORBIDDEN_TOPIC", "Tópico no disponible sin cuenta: " + topic);
            return null;
        }
        // Temas privados (US-033, US-183, US-186): dueño, rol y empresa según Topics.canSubscribe.
        if (!Topics.canSubscribe(parsed, session.userId(), session.role(), session.companyId())) {
            sendError(session, "FORBIDDEN_TOPIC", "Tópico no autorizado para esta sesión: " + topic);
            return null;
        }
        int max = session.anonymous() ? properties.maxAnonymousSubscriptions()
                : properties.maxSubscriptionsFor(session.role());
        if (!session.topics().contains(topic) && session.topics().size() >= max) {
            sendError(session, "TOO_MANY_SUBSCRIPTIONS", "Máximo " + max + " suscripciones");
            return null;
        }
        return new Target(topic, parsed);
    }

    private void subscribe(ClientSession session, String rawTopic, String traceparent) {
        Target target = authorize(session, rawTopic);
        if (target == null) {
            return;
        }
        // Se suscribe ANTES de pedir el snapshot para no perder eventos intermedios.
        registry.subscribe(session, target.topic());
        sendSubscribed(session, target.topic());
        if (target.parsed().kind().hasSnapshot()) {
            sendSnapshot(session, target, traceparent);
        }
    }

    /**
     * US-184: con la misma autorización que subscribe, reenvía en orden los eventos del tópico con seq mayor
     * que lastSeq que sigan en el búfer y termina con resumed. Si ya no están todos, manda el snapshot como un
     * subscribe (en los tópicos sin snapshot, con data null: la app vuelve a pedir su GET). Los eventos en vivo
     * que llegan mientras tanto se retienen y van después de los del búfer, sin repetir ni desordenar seq.
     */
    private void resume(ClientSession session, JsonNode msg) {
        Target target = authorize(session, msg.path(TOPIC).asText(null));
        if (target == null) {
            metrics.replay(WsMetrics.REPLAY_REJECTED);
            return;
        }
        String lastSeq = msg.path("lastSeq").asText(null);
        if (!Seq.isValid(lastSeq)) {
            sendError(session, "INVALID_SEQ", "lastSeq debe ser el seq del último evento recibido (\"<ms>-<n>\")");
            metrics.replay(WsMetrics.REPLAY_REJECTED);
            return;
        }
        String topic = target.topic();
        session.beginReplay(topic);
        registry.subscribe(session, topic);
        sendSubscribed(session, topic);
        Optional<List<ReplayEvent>> missed = replay.since(topic, lastSeq);
        if (missed.isEmpty()) {
            session.endReplay(topic, List.of(), null, null);
            metrics.replay(WsMetrics.REPLAY_SNAPSHOT);
            sendSnapshot(session, target, msg.path(TRACEPARENT).asText(null));
            return;
        }
        session.endReplay(topic, missed.get(), lastSeq, last -> objectMapper.createObjectNode()
                .put("type", "resumed").put(TOPIC, topic).put("lastSeq", last).toString());
        metrics.sent(target.parsed().kind(), missed.get().size());
        metrics.replay(WsMetrics.REPLAY_COMPLETE);
    }

    private void sendSubscribed(ClientSession session, String topic) {
        session.send(objectMapper.createObjectNode().put("type", "subscribed").put(TOPIC, topic).toString());
    }

    /**
     * Snapshot en un hilo virtual. Lo difundido hasta aquí ya está en el snapshot: el cliente ignora los event
     * con seq <= asOfSeq. La llamada al servicio dueño continúa la traza del traceparent del cliente (HT-10).
     */
    private void sendSnapshot(ClientSession session, Target target, String traceparent) {
        String asOfSeq = fanout.lastSeq();
        snapshotExecutor.submit(() -> tracing.run("tripbus.ws.snapshot", traceparent, () -> {
            ObjectNode out = objectMapper.createObjectNode();
            out.put("type", "snapshot");
            out.put(TOPIC, target.topic());
            out.put("asOfSeq", asOfSeq);
            if (!target.parsed().kind().hasSnapshot()) {
                out.putNull("data");
            } else {
                snapshots.snapshot(target.parsed()).ifPresentOrElse(
                        data -> out.set("data", data),
                        () -> {
                            out.putNull("data");
                            out.put("error", "SNAPSHOT_UNAVAILABLE");
                        });
            }
            session.send(out.toString());
        }));
    }

    private void unsubscribe(ClientSession session, String rawTopic) {
        String topic = rawTopic == null ? "" : rawTopic.trim().toLowerCase(Locale.ROOT);
        registry.unsubscribe(session, topic);
        session.send(objectMapper.createObjectNode().put("type", "unsubscribed").put(TOPIC, topic).toString());
    }

    private void sendError(ClientSession session, String code, String message) {
        session.send(objectMapper.createObjectNode().put("type", "error").put("code", code)
                .put("message", message).toString());
    }

    @Override
    public void afterConnectionClosed(WebSocketSession raw, CloseStatus status) {
        metrics.closed(status.getCode());
        registry.remove(raw.getId());
    }

    @Override
    public void handleTransportError(WebSocketSession raw, Throwable exception) {
        log.debug("Error de transporte en {}: {}", raw.getId(), exception.getMessage());
        registry.remove(raw.getId());
    }

    // ------------------------------------------------------------------ heartbeat

    /**
     * Ping cada 20 s; si no hay respuesta en 45 s se cierra. Token vencido → reconectar. Contraseña
     * cambiada después de emitido el token (US-129) → UNAUTHENTICATED y 4401. Las sesiones anónimas no
     * tienen token: solo las cierra la falta de heartbeat. Al final renueva en Redis la marca de las
     * conexiones que siguen abiertas (HT-11).
     */
    @Scheduled(fixedDelayString = "${tripbus.realtime.heartbeat-interval-ms:20000}")
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
            if (stillValid(session, now)) {
                session.send("{\"type\":\"ping\"}");
            }
        }
        presence.refresh(registry.all());
    }

    /** Cierra la sesión identificada si ya no vale (sin heartbeat, token vencido o viejo); true si sigue. */
    private boolean stillValid(ClientSession session, Instant now) {
        if (session.lastSeen().plus(properties.heartbeatTimeout()).isBefore(now)) {
            session.close(HEARTBEAT_TIMEOUT);
            registry.remove(session.id());
            return false;
        }
        if (session.tokenExpiresAt() != null && session.tokenExpiresAt().isBefore(now)) {
            reject(session, "TOKEN_EXPIRED", "El token expiró; reconecte con uno nuevo");
            return false;
        }
        // US-129: una sola consulta a Redis por sesión autenticada y heartbeat.
        if (session.authenticated()
                && revocations.isPasswordChangedSince(session.userId(), session.tokenIssuedAt())) {
            reject(session, UNAUTHENTICATED, SESSION_CLOSED);
            return false;
        }
        return true;
    }
}
