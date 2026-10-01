package com.tripbus.realtime.ws;

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

    private final WebSocketSession session;
    private final Instant connectedAt;
    private final Set<String> topics = ConcurrentHashMap.newKeySet();
    private volatile String userId;
    private volatile String role;
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

    public void authenticate(String userId, String role, Instant expiresAt) {
        this.userId = userId;
        this.role = role;
        this.tokenExpiresAt = expiresAt;
    }

    public void touch(Instant now) {
        this.lastSeen = now;
    }

    public void send(String json) {
        if (!session.isOpen()) {
            return;
        }
        try {
            session.sendMessage(new TextMessage(json));
        } catch (IOException | IllegalStateException e) {
            close(CloseStatus.SESSION_NOT_RELIABLE);
        }
    }

    public void close(CloseStatus status) {
        try {
            if (session.isOpen()) {
                session.close(status);
            }
        } catch (IOException ignored) {
            // ya cerrada
        }
    }

    public Set<String> topics() {
        return topics;
    }

    public String userId() {
        return userId;
    }

    public String role() {
        return role;
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
