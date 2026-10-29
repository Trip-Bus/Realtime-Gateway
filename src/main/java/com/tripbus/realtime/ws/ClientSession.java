package com.tripbus.realtime.ws;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;

import java.io.IOException;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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
    private volatile Instant tokenExpiresAt;
    private volatile Instant lastSeen;

    public ClientSession(WebSocketSession raw, Instant now) {
        // Envíos concurrentes seguros (difusión + respuestas) con límite de tiempo y buffer.
        this.session = new ConcurrentWebSocketSessionDecorator(raw, 5_000, 512 * 1024);
        this.connectedAt = now;
        this.lastSeen = now;
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

    /** companyId: claim del JWT de un OPERADOR (empresa:{uuid}:alertas); null en los demás roles. */
    public void authenticate(String userId, String jti, String role, String companyId, Instant expiresAt) {
        this.anonymous = false;
        this.userId = userId;
        this.jti = jti;
        this.role = role;
        this.companyId = companyId;
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

    public Instant tokenExpiresAt() {
        return tokenExpiresAt;
    }

    public Instant lastSeen() {
        return lastSeen;
    }

    public Instant connectedAt() {
        return connectedAt;
    }
}
