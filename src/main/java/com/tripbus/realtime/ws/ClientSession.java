package com.tripbus.realtime.ws;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.UnaryOperator;

/** Estado en memoria de una conexión (el gateway no tiene base de datos). */
public class ClientSession {

    private static final Logger log = LoggerFactory.getLogger(ClientSession.class);

    private final WebSocketSession session;
    private final Instant connectedAt;
    private final Set<String> topics = ConcurrentHashMap.newKeySet();
    private volatile String userId;
    private volatile String jti;
    private volatile boolean anonymous;
    private volatile String role;
    private volatile String companyId;
    private volatile Instant tokenIssuedAt;
    private volatile Instant tokenExpiresAt;
    private volatile Instant lastSeen;
    private final String ip;
    private volatile String countedIp;
    private volatile String countedUserId;
    private Instant messageWindowStart;
    private int messagesInWindow;
    /** Eventos en vivo retenidos por tópico mientras se reenvía el búfer de reconexión (US-184). */
    private final Map<String, List<ReplayEvent>> replaying = new HashMap<>();
    private final ReentrantLock deliveryLock = new ReentrantLock();

    public ClientSession(WebSocketSession raw, Instant now) {
        this(raw, now, null);
    }

    /** ip: la del cliente (HT-11), o null si no se conoce. */
    public ClientSession(WebSocketSession raw, Instant now, String ip) {
        // Envíos concurrentes seguros (difusión + respuestas) con límite de tiempo y buffer.
        this.session = new ConcurrentWebSocketSessionDecorator(raw, 5_000, 512 * 1024);
        this.connectedAt = now;
        this.lastSeen = now;
        this.ip = ip;
    }

    public String id() {
        return session.getId();
    }

    public boolean authenticated() {
        return userId != null;
    }

    /** Sesión sin cuenta (seguimiento público, US-039): solo temas públicos y sin token que venza. */
    public boolean anonymous() {
        return anonymous;
    }

    /** Ya se identificó, con token o como anónima: el plazo de autenticación no la cierra. */
    public boolean identified() {
        return authenticated() || anonymous;
    }

    public void connectAnonymously() {
        this.anonymous = true;
    }

    public void authenticate(String userId, String jti, String role, Instant expiresAt) {
        authenticate(userId, jti, role, null, expiresAt);
    }

    /** companyId: claim del JWT de un OPERADOR (empresa:{uuid}:alertas|flota); null en los demás roles. */
    public void authenticate(String userId, String jti, String role, String companyId, Instant expiresAt) {
        authenticate(userId, jti, role, companyId, null, expiresAt);
    }

    /** issuedAt: claim iat, para cerrar la sesión si después se cambia la contraseña (US-129). */
    public void authenticate(String userId, String jti, String role, String companyId, Instant issuedAt,
                             Instant expiresAt) {
        this.anonymous = false;
        this.userId = userId;
        this.jti = jti;
        this.role = role;
        this.companyId = companyId;
        this.tokenIssuedAt = issuedAt;
        this.tokenExpiresAt = expiresAt;
    }

    public void touch(Instant now) {
        this.lastSeen = now;
    }

    /**
     * Envía un mensaje a esta conexión. Nunca lanza: si el cliente es lento (el decorador
     * excede el tiempo o el buffer y lanza SessionLimitExceededException) o la conexión
     * falla, se cierra SOLO esta sesión y se devuelve false, para no cortar la difusión a
     * los demás suscriptores (US-031).
     *
     * @return true si el mensaje se entregó al transporte
     */
    public boolean send(String json) {
        if (!session.isOpen()) {
            return false;
        }
        try {
            session.sendMessage(new TextMessage(json));
            return true;
        } catch (IOException | RuntimeException e) {
            // Sin el contenido del mensaje: puede llevar datos del evento.
            log.warn("Envío fallido a la sesión {} ({}); se cierra", id(), e.getClass().getSimpleName());
            close(CloseStatus.SESSION_NOT_RELIABLE);
            return false;
        }
    }

    /**
     * HT-11: true si el mensaje del cliente cabe en el límite ({@code max} por {@code window}, ventana fija que
     * empieza con el primer mensaje). Lo llama el hilo que atiende los mensajes de esta sesión.
     */
    public synchronized boolean allowMessage(Instant now, int max, Duration window) {
        if (messageWindowStart == null || !now.isBefore(messageWindowStart.plus(window))) {
            messageWindowStart = now;
            messagesInWindow = 0;
        }
        messagesInWindow++;
        return messagesInWindow <= max;
    }

    /**
     * US-184: desde aquí y hasta {@link #endReplay} los eventos en vivo de {@code topic} no se envían: se
     * guardan para mandarlos después de los del búfer, sin desordenar los seq.
     */
    public void beginReplay(String topic) {
        deliveryLock.lock();
        try {
            replaying.put(topic, new ArrayList<>());
        } finally {
            deliveryLock.unlock();
        }
    }

    /** Envía un evento en vivo del tópico, o lo retiene si ese tópico se está reenviando. */
    public boolean sendEvent(String topic, String seq, String json) {
        deliveryLock.lock();
        try {
            List<ReplayEvent> held = replaying.get(topic);
            if (held != null) {
                held.add(new ReplayEvent(seq, json));
                return true;
            }
            return send(json);
        } finally {
            deliveryLock.unlock();
        }
    }

    /**
     * Termina el reenvío de {@code topic}: manda {@code replayed} (en orden, todos con seq mayor que
     * {@code after}), luego los eventos en vivo retenidos que sean posteriores al último enviado y por último
     * {@code tail.apply(último seq)}, sin que se cuele otro evento del tópico. Con {@code after} null los
     * retenidos se mandan todos (el cliente se guía por el asOfSeq del snapshot) y {@code tail} puede ser null.
     *
     * @return cuántos eventos se enviaron
     */
    public int endReplay(String topic, List<ReplayEvent> replayed, String after, UnaryOperator<String> tail) {
        deliveryLock.lock();
        try {
            List<ReplayEvent> held = replaying.remove(topic);
            String last = after;
            int sent = 0;
            for (ReplayEvent event : replayed) {
                send(event.json());
                last = event.seq();
                sent++;
            }
            for (ReplayEvent event : held == null ? List.<ReplayEvent>of() : held) {
                if (last == null || Seq.compare(event.seq(), last) > 0) {
                    send(event.json());
                    last = after == null ? null : event.seq();
                    sent++;
                }
            }
            if (tail != null) {
                send(tail.apply(last));
            }
            return sent;
        } finally {
            deliveryLock.unlock();
        }
    }

    /** Un evento del búfer de reconexión. */
    public record ReplayEvent(String seq, String json) {
    }

    /** Idempotente: no hace nada si la sesión ya está cerrada o se está cerrando. */
    public void close(CloseStatus status) {
        try {
            if (session.isOpen()) {
                session.close(status);
            }
        } catch (IOException | RuntimeException ignored) {
            // ya cerrada o cerrándose
        }
    }

    public Set<String> topics() {
        return topics;
    }

    public String userId() {
        return userId;
    }

    /** Id del token con el que se autenticó (claim jti); null si es anónima o el token no lo trae. */
    public String jti() {
        return jti;
    }

    public String role() {
        return role;
    }

    public String companyId() {
        return companyId;
    }

    public Instant tokenIssuedAt() {
        return tokenIssuedAt;
    }

    public Instant tokenExpiresAt() {
        return tokenExpiresAt;
    }

    public Instant lastSeen() {
        return lastSeen;
    }

    public Instant connectedAt() {
        return connectedAt;
    }

    /** IP del cliente (HT-11); null si no se conoce. */
    public String ip() {
        return ip;
    }

    /** IP con la que la conexión está contada en Redis (ConnectionPresence); null si no lo está. */
    String countedIp() {
        return countedIp;
    }

    void countedIp(String ip) {
        this.countedIp = ip;
    }

    /** Usuario con el que la conexión está contada en Redis (ConnectionPresence); null si no lo está. */
    String countedUserId() {
        return countedUserId;
    }

    void countedUserId(String userId) {
        this.countedUserId = userId;
    }
}
