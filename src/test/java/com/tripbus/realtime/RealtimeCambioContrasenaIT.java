package com.tripbus.realtime;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tripbus.realtime.snapshot.SnapshotService;
import com.tripbus.realtime.ws.RealtimeHandler;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

/**
 * US-129 contra Redis 7 real: identity deja {@code tripbus:pwd-changed:{sub}} = epoch en segundos del
 * cambio. Un token con iat anterior se rechaza al conectar y una sesión ya abierta se cierra en el
 * siguiente heartbeat (aquí cada 200 ms), ambos con UNAUTHENTICATED y 4401.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "tripbus.realtime.heartbeat-interval-ms=200")
@ActiveProfiles("it")
@Testcontainers
class RealtimeCambioContrasenaIT {

    @Container
    @ServiceConnection(name = "redis")
    static GenericContainer<?> redis = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

    @LocalServerPort
    private int port;
    @Autowired
    private StringRedisTemplate redisTemplate;
    @Autowired
    private ObjectMapper mapper;

    @MockitoBean
    private JwtDecoder jwtDecoder;
    @MockitoBean
    private SnapshotService snapshotService;

    /** Una conexión de prueba con sus mensajes y su cierre. */
    private final class Client {
        final BlockingQueue<JsonNode> received = new LinkedBlockingQueue<>();
        final CompletableFuture<CloseStatus> closed = new CompletableFuture<>();
        final WebSocketSession ws;

        Client(String token) throws Exception {
            ws = new StandardWebSocketClient().execute(new TextWebSocketHandler() {
                @Override
                protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
                    received.add(mapper.readTree(message.getPayload()));
                }

                @Override
                public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
                    closed.complete(status);
                }
            }, null, URI.create("ws://localhost:" + port + "/ws")).get(5, TimeUnit.SECONDS);
            ws.sendMessage(new TextMessage("{\"type\":\"connect\",\"token\":\"" + token + "\"}"));
        }

        JsonNode next(String type) throws InterruptedException {
            while (true) {
                JsonNode msg = received.poll(5, TimeUnit.SECONDS);
                assertThat(msg).as("esperando mensaje " + type).isNotNull();
                if (type.equals(msg.path("type").asText())) {
                    return msg;
                }
            }
        }

        /** Responde los ping durante {@code millis} para que la falta de heartbeat no la cierre. */
        void stayAlive(long millis) throws Exception {
            long deadline = System.currentTimeMillis() + millis;
            while (System.currentTimeMillis() < deadline && ws.isOpen()) {
                JsonNode msg = received.poll(100, TimeUnit.MILLISECONDS);
                if (msg != null && "ping".equals(msg.path("type").asText())) {
                    ws.sendMessage(new TextMessage("{\"type\":\"pong\"}"));
                }
            }
        }
    }

    private String token(String userId, Instant issuedAt) {
        String value = "token-" + UUID.randomUUID();
        when(jwtDecoder.decode(value)).thenReturn(Jwt.withTokenValue(value).header("alg", "RS256").subject(userId)
                .jti(UUID.randomUUID().toString()).claim("role", "PASAJERO").issuedAt(issuedAt)
                .expiresAt(Instant.now().plusSeconds(600)).build());
        return value;
    }

    private void passwordChanged(String userId, Instant at) {
        redisTemplate.opsForValue().set("tripbus:pwd-changed:" + userId, String.valueOf(at.getEpochSecond()),
                Duration.ofMinutes(5));
    }

    @Test
    void tokenEmitidoAntesDelCambioSeRechazaAlConectarYElNuevoNo() throws Exception {
        String userId = UUID.randomUUID().toString();
        Instant changedAt = Instant.now();
        // Se simulan los tokens antes de conectar: stubbing y decode a la vez rompen a Mockito.
        String viejo = token(userId, changedAt.minusSeconds(60));
        String nuevo = token(userId, changedAt);
        passwordChanged(userId, changedAt);

        Client rechazado = new Client(viejo);
        assertThat(rechazado.next("error").path("code").asText()).isEqualTo("UNAUTHENTICATED");
        assertThat(rechazado.closed.get(5, TimeUnit.SECONDS).getCode()).isEqualTo(RealtimeHandler.UNAUTHORIZED.getCode());
        assertThat(rechazado.received.stream().map(m -> m.path("type").asText())).doesNotContain("connected");

        // El token que identity entrega con el cambio (iat en ese mismo segundo) sigue valiendo.
        Client aceptado = new Client(nuevo);
        assertThat(aceptado.next("connected").path("userId").asText()).isEqualTo(userId);
        aceptado.stayAlive(600);
        assertThat(aceptado.ws.isOpen()).isTrue();
        aceptado.ws.close();
    }

    @Test
    void laSesionAbiertaSeCierraEnElHeartbeatTrasElCambioYLasDemasSiguen() throws Exception {
        String userId = UUID.randomUUID().toString();
        String otroId = UUID.randomUUID().toString();
        Instant emitido = Instant.now().minusSeconds(30);
        String suyo = token(userId, emitido);
        String ajeno = token(otroId, emitido);
        Client abierta = new Client(suyo);
        Client otra = new Client(ajeno);
        abierta.next("connected");
        otra.next("connected");
        abierta.stayAlive(500);
        assertThat(abierta.ws.isOpen()).as("sin cambio de contraseña la sesión sigue").isTrue();

        passwordChanged(userId, Instant.now());

        assertThat(abierta.next("error").path("code").asText()).isEqualTo("UNAUTHENTICATED");
        assertThat(abierta.closed.get(5, TimeUnit.SECONDS).getCode()).isEqualTo(RealtimeHandler.UNAUTHORIZED.getCode());
        otra.stayAlive(500);
        assertThat(otra.ws.isOpen()).isTrue();
        otra.ws.close();
    }
}
