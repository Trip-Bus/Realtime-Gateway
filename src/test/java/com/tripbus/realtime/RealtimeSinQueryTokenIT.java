package com.tripbus.realtime;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tripbus.realtime.snapshot.SnapshotService;
import com.tripbus.realtime.ws.RealtimeHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
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
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * US-030: con tripbus.realtime.allow-query-token=false (como en PROD) el ?token de la URL se
 * ignora y la única forma de autenticarse es el mensaje connect. Plazo de autenticación y
 * heartbeat cortos para no esperar los 10 s reales.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "tripbus.realtime.allow-query-token=false",
        "tripbus.realtime.auth-timeout=1s",
        "tripbus.realtime.heartbeat-interval-ms=200"})
@ActiveProfiles("it")
@Testcontainers
class RealtimeSinQueryTokenIT {

    @Container
    @ServiceConnection(name = "redis")
    static GenericContainer<?> redis = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

    @LocalServerPort
    private int port;
    @Autowired
    private ObjectMapper mapper;

    @MockitoBean
    private JwtDecoder jwtDecoder;
    @MockitoBean
    private SnapshotService snapshotService;

    private final BlockingQueue<JsonNode> received = new LinkedBlockingQueue<>();
    private final CompletableFuture<CloseStatus> closed = new CompletableFuture<>();

    @BeforeEach
    void tokenValido() {
        Jwt jwt = Jwt.withTokenValue("token-valido").header("alg", "RS256").subject(UUID.randomUUID().toString())
                .claim("role", "PASAJERO").expiresAt(Instant.now().plusSeconds(600)).issuedAt(Instant.now()).build();
        when(jwtDecoder.decode("token-valido")).thenReturn(jwt);
    }

    private WebSocketSession connect(String query) throws Exception {
        return new StandardWebSocketClient().execute(new TextWebSocketHandler() {
            @Override
            protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
                received.add(mapper.readTree(message.getPayload()));
            }

            @Override
            public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
                closed.complete(status);
            }
        }, null, URI.create("ws://localhost:" + port + "/ws" + query)).get(5, TimeUnit.SECONDS);
    }

    @Test
    void tokenEnLaUrlSinMensajeConnectSeCierraAlVencerElPlazo() throws Exception {
        connect("?token=token-valido");

        assertThat(closed.get(10, TimeUnit.SECONDS).getCode()).isEqualTo(RealtimeHandler.UNAUTHORIZED.getCode());
        List<String> recibidos = received.stream()
                .map(msg -> msg.path("type").asText() + ":" + msg.path("code").asText()).toList();
        assertThat(recibidos).doesNotContain("connected:").contains("error:AUTH_TIMEOUT");
        verify(jwtDecoder, never()).decode(anyString());
    }

    @Test
    void sinTokenEnLaUrlYConMensajeConnectQuedaAutenticado() throws Exception {
        WebSocketSession ws = connect("");
        ws.sendMessage(new TextMessage("{\"type\":\"connect\",\"token\":\"token-valido\"}"));

        JsonNode msg = received.poll(5, TimeUnit.SECONDS);
        assertThat(msg).isNotNull();
        assertThat(msg.path("type").asText()).isEqualTo("connected");
        assertThat(msg.path("role").asText()).isEqualTo("PASAJERO");

        // Ya autenticado, el plazo de autenticación (1 s) no lo cierra.
        assertThat(closed.completeOnTimeout(null, 1_500, TimeUnit.MILLISECONDS).get()).isNull();
        assertThat(ws.isOpen()).isTrue();
        ws.close();
    }
}
