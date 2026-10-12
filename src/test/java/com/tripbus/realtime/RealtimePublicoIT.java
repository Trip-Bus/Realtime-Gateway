package com.tripbus.realtime;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.tripbus.realtime.snapshot.SnapshotService;
import com.tripbus.realtime.ws.RealtimeHandler;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtException;
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
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * US-039: seguimiento público del bus sin cuenta. Un {"type":"connect"} sin token deja la sesión
 * anónima: recibe snapshot y eventos de los temas públicos, no la cierra el plazo de autenticación
 * (10 s reales) y tiene un máximo de suscripciones propio. Heartbeat cada 200 ms para que el
 * chequeo del plazo corra muchas veces durante la espera.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "tripbus.realtime.heartbeat-interval-ms=200",
        "tripbus.realtime.max-anonymous-subscriptions=5"})
@ActiveProfiles("it")
@Testcontainers
class RealtimePublicoIT {

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

    private final BlockingQueue<JsonNode> received = new LinkedBlockingQueue<>();
    private final CompletableFuture<CloseStatus> closed = new CompletableFuture<>();

    private WebSocketSession connect() throws Exception {
        return new StandardWebSocketClient().execute(new TextWebSocketHandler() {
            @Override
            protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
                received.add(mapper.readTree(message.getPayload()));
            }

            @Override
            public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
                closed.complete(status);
            }
        }, null, URI.create("ws://localhost:" + port + "/ws")).get(5, TimeUnit.SECONDS);
    }

    private JsonNode next(String type) throws InterruptedException {
        while (true) {
            JsonNode msg = received.poll(5, TimeUnit.SECONDS);
            assertThat(msg).as("esperando mensaje " + type).isNotNull();
            if (type.equals(msg.path("type").asText())) {
                return msg;
            }
        }
    }

    private static void send(WebSocketSession ws, String json) throws Exception {
        ws.sendMessage(new TextMessage(json));
    }

    private static String subscribe(String topic) {
        return "{\"type\":\"subscribe\",\"topic\":\"" + topic + "\"}";
    }

    @Test
    void anonimoSigueLaPosicionDelBusConSnapshotYEventosSinDatosPersonales() throws Exception {
        UUID busId = UUID.randomUUID();
        when(snapshotService.snapshot(any())).thenReturn(Optional.of(
                JsonNodeFactory.instance.objectNode().put("busId", busId.toString()).put("lat", 4.6)));

        WebSocketSession ws = connect();
        send(ws, "{\"type\":\"connect\"}");
        JsonNode connected = next("connected");
        assertThat(connected.path("anonymous").asBoolean()).isTrue();
        assertThat(connected.has("userId")).isFalse();
        assertThat(connected.path("maxSubscriptions").asInt()).isEqualTo(5);

        String topic = "bus:" + busId + ":posicion";
        send(ws, subscribe(topic));
        next("subscribed");
        assertThat(next("snapshot").path("data").path("busId").asText()).isEqualTo(busId.toString());

        redisTemplate.opsForStream().add(StreamRecords.newRecord().in("tripbus:events:realtime-test").ofMap(Map.of(
                "eventId", UUID.randomUUID().toString(), "type", "PositionUpdated", "source", "test",
                "occurredAt", Instant.now().toString(),
                "payload", "{\"busId\":\"" + busId + "\",\"lat\":4.61,\"lng\":-74.08,\"userId\":\"conductor\"}")));
        JsonNode event = next("event");
        assertThat(event.path("topic").asText()).isEqualTo(topic);
        assertThat(event.path("data").path("lat").asDouble()).isEqualTo(4.61);
        assertThat(event.path("data").has("userId")).isFalse();
        ws.close();
    }

    @Test
    void anonimoNoSeCierraPorElPlazoDeAutenticacion() throws Exception {
        WebSocketSession ws = connect();
        send(ws, "{\"type\":\"connect\",\"token\":\"\"}");
        assertThat(next("connected").path("anonymous").asBoolean()).isTrue();

        // auth-timeout real (10 s): una sesión sin identificar ya se habría cerrado con 4401.
        assertThat(closed.completeOnTimeout(null, 12, TimeUnit.SECONDS).get()).isNull();
        assertThat(ws.isOpen()).isTrue();
        assertThat(received).noneMatch(msg -> "AUTH_TIMEOUT".equals(msg.path("code").asText()));
        ws.close();
    }

    @Test
    void anonimoTieneUnMaximoDeCincoSuscripciones() throws Exception {
        when(snapshotService.snapshot(any())).thenReturn(Optional.empty());
        WebSocketSession ws = connect();
        send(ws, "{\"type\":\"connect\"}");
        next("connected");

        String[] patterns = {"bus:%s:posicion", "viaje:%s:eventos", "oferta:%s:cupo", "itinerario:%s:asientos",
                "bus:%s:posicion"};
        String primero = null;
        for (String pattern : patterns) {
            String topic = pattern.formatted(UUID.randomUUID());
            primero = primero == null ? topic : primero;
            send(ws, subscribe(topic));
            next("subscribed");
        }
        // Repetir uno ya suscrito no cuenta como nueva suscripción.
        send(ws, subscribe(primero));
        assertThat(next("subscribed").path("topic").asText()).isEqualTo(primero);

        send(ws, subscribe("bus:" + UUID.randomUUID() + ":posicion"));
        JsonNode error = next("error");
        assertThat(error.path("code").asText()).isEqualTo("TOO_MANY_SUBSCRIPTIONS");
        assertThat(error.path("message").asText()).contains("5");
        assertThat(ws.isOpen()).isTrue();
        ws.close();
    }

    @Test
    void connectConTokenInvalidoSigueCerrandoCon4401() throws Exception {
        when(jwtDecoder.decode("malo")).thenThrow(new JwtException("firma inválida"));
        WebSocketSession ws = connect();
        send(ws, "{\"type\":\"connect\",\"token\":\"malo\"}");

        assertThat(next("error").path("code").asText()).isEqualTo("INVALID_TOKEN");
        assertThat(closed.get(5, TimeUnit.SECONDS).getCode()).isEqualTo(RealtimeHandler.UNAUTHORIZED.getCode());
    }

    @Test
    void anonimoPuedeAutenticarseDespuesYUnAutenticadoNoSeDegrada() throws Exception {
        Jwt jwt = Jwt.withTokenValue("token-valido").header("alg", "RS256").subject(UUID.randomUUID().toString())
                .claim("role", "PASAJERO").expiresAt(Instant.now().plusSeconds(600)).issuedAt(Instant.now()).build();
        when(jwtDecoder.decode("token-valido")).thenReturn(jwt);

        WebSocketSession ws = connect();
        send(ws, "{\"type\":\"connect\"}");
        assertThat(next("connected").path("anonymous").asBoolean()).isTrue();

        send(ws, "{\"type\":\"connect\",\"token\":\"token-valido\"}");
        JsonNode autenticado = next("connected");
        assertThat(autenticado.path("role").asText()).isEqualTo("PASAJERO");
        assertThat(autenticado.has("anonymous")).isFalse();

        send(ws, "{\"type\":\"connect\"}");
        assertThat(next("error").path("code").asText()).isEqualTo("ALREADY_AUTHENTICATED");
        assertThat(ws.isOpen()).isTrue();
        ws.close();
    }
}
