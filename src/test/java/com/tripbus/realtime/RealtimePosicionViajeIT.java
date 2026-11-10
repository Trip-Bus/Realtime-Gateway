package com.tripbus.realtime;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import com.tripbus.realtime.ws.RealtimeHandler;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

/**
 * US-173/US-174: posición por viaje ({@code viaje:{id}:posicion}) con el snapshot real de fleet (servidor
 * HTTP local que responde GET /itineraries/{id}/bus-position) y eventos desde Redis 7 real. US-104: cuenta
 * suspendida ({@code tripbus:suspended:{sub}}) rechazada al conectarse, por connect y por ?token= (QA).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "tripbus.realtime.allow-query-token=true")
@ActiveProfiles("it")
@Testcontainers
class RealtimePosicionViajeIT {

    private static final String STREAM = "tripbus:events:realtime-test";

    @Container
    @ServiceConnection(name = "redis")
    static GenericContainer<?> redis = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

    /** fleet simulado: responde la posición del bus de cualquier itinerario. */
    static final HttpServer fleet = startFleet();

    private static HttpServer startFleet() {
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
            server.createContext("/itineraries/", exchange -> {
                String id = exchange.getRequestURI().getPath().split("/")[2];
                byte[] body = ("{\"itineraryId\":\"" + id + "\",\"posicion\":{\"lat\":4.6,\"lng\":-74.08},"
                        + "\"estado\":\"EN_RUTA\",\"paradas\":[],\"estadoRastreo\":\"ACTIVO\","
                        + "\"ultimaPosicionEn\":\"2026-10-22T12:00:00Z\"}").getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, body.length);
                exchange.getResponseBody().write(body);
                exchange.close();
            });
            server.start();
            return server;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @DynamicPropertySource
    static void fleetUrl(DynamicPropertyRegistry registry) {
        registry.add("tripbus.realtime.fleet-url", () -> "http://localhost:" + fleet.getAddress().getPort());
    }

    @AfterAll
    static void stopFleet() {
        fleet.stop(0);
    }

    @LocalServerPort
    private int port;
    @Autowired
    private StringRedisTemplate redisTemplate;
    @Autowired
    private ObjectMapper mapper;

    @MockitoBean
    private JwtDecoder jwtDecoder;

    /** Una conexión de prueba con sus mensajes y su cierre. */
    private final class Client {
        final BlockingQueue<JsonNode> received = new LinkedBlockingQueue<>();
        final CompletableFuture<CloseStatus> closed = new CompletableFuture<>();
        final WebSocketSession ws;

        Client(String query) throws Exception {
            ws = new StandardWebSocketClient().execute(new TextWebSocketHandler() {
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

        void send(String json) throws Exception {
            ws.sendMessage(new TextMessage(json));
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
    }

    private String token(String userId) {
        String value = "token-" + UUID.randomUUID();
        when(jwtDecoder.decode(value)).thenReturn(Jwt.withTokenValue(value).header("alg", "RS256").subject(userId)
                .jti(UUID.randomUUID().toString()).claim("role", "PASAJERO")
                .expiresAt(Instant.now().plusSeconds(600)).issuedAt(Instant.now()).build());
        return value;
    }

    private void publish(String type, String payload) {
        redisTemplate.opsForStream().add(StreamRecords.newRecord().in(STREAM).ofMap(Map.of(
                "eventId", UUID.randomUUID().toString(), "type", type, "source", "test",
                "occurredAt", Instant.now().toString(), "payload", payload)));
    }

    @Test
    void anonimoSigueLaPosicionDelViajeConSnapshotDeFleetYEventosDePosicionYRastreo() throws Exception {
        UUID itineraryId = UUID.randomUUID();
        UUID busId = UUID.randomUUID();
        Client client = new Client("");
        client.send("{\"type\":\"connect\"}");
        assertThat(client.next("connected").path("anonymous").asBoolean()).isTrue();

        String topic = "viaje:" + itineraryId + ":posicion";
        client.send("{\"type\":\"subscribe\",\"topic\":\"" + topic + "\"}");
        client.next("subscribed");
        JsonNode snapshot = client.next("snapshot");
        assertThat(snapshot.path("topic").asText()).isEqualTo(topic);
        assertThat(snapshot.has("error")).isFalse();
        assertThat(snapshot.path("data").path("itineraryId").asText()).isEqualTo(itineraryId.toString());
        assertThat(snapshot.path("data").path("estadoRastreo").asText()).isEqualTo("ACTIVO");
        assertThat(snapshot.path("data").has("ultimaPosicionEn")).isTrue();

        publish("PositionUpdated", "{\"busId\":\"" + busId + "\",\"itineraryId\":\"" + itineraryId
                + "\",\"lat\":4.61,\"lng\":-74.07,\"conductorId\":\"" + UUID.randomUUID() + "\"}");
        JsonNode position = client.next("event");
        assertThat(position.path("topic").asText()).isEqualTo(topic);
        assertThat(position.path("event").asText()).isEqualTo("PositionUpdated");
        assertThat(position.path("data").path("lat").asDouble()).isEqualTo(4.61);
        assertThat(position.path("data").has("conductorId")).isFalse();

        publish("TrackingStatusChanged", "{\"itineraryId\":\"" + itineraryId + "\",\"estadoRastreo\":\"SIN_SENAL\"}");
        JsonNode status = client.next("event");
        assertThat(status.path("topic").asText()).isEqualTo(topic);
        assertThat(status.path("event").asText()).isEqualTo("TrackingStatusChanged");
        assertThat(status.path("data").path("estadoRastreo").asText()).isEqualTo("SIN_SENAL");
        client.ws.close();
    }

    @Test
    void positionUpdatedSigueLlegandoAlTopicoDeprecadoDelBus() throws Exception {
        UUID busId = UUID.randomUUID();
        Client client = new Client("");
        client.send("{\"type\":\"connect\"}");
        client.next("connected");
        String topic = "bus:" + busId + ":posicion";
        client.send("{\"type\":\"subscribe\",\"topic\":\"" + topic + "\"}");
        client.next("subscribed");

        publish("PositionUpdated", "{\"busId\":\"" + busId + "\",\"itineraryId\":\"" + UUID.randomUUID()
                + "\",\"lat\":4.62}");

        assertThat(client.next("event").path("topic").asText()).isEqualTo(topic);
        client.ws.close();
    }

    @Test
    void cuentaSuspendidaSeRechazaAlConectarseConConnectYConQueryToken() throws Exception {
        String userId = UUID.randomUUID().toString();
        // Se simulan los tokens antes de conectar: stubbing y decode a la vez rompen a Mockito.
        String porConnect = token(userId);
        String porQuery = token(userId);
        redisTemplate.opsForValue().set("tripbus:suspended:" + userId, "1", Duration.ofMinutes(5));

        Client connect = new Client("");
        connect.send("{\"type\":\"connect\",\"token\":\"" + porConnect + "\"}");
        assertThat(connect.next("error").path("code").asText()).isEqualTo("ACCOUNT_SUSPENDED");
        assertThat(connect.closed.get(5, TimeUnit.SECONDS).getCode()).isEqualTo(RealtimeHandler.UNAUTHORIZED.getCode());
        assertThat(connect.received.stream().map(m -> m.path("type").asText())).doesNotContain("connected");

        Client query = new Client("?token=" + porQuery);
        assertThat(query.next("error").path("code").asText()).isEqualTo("ACCOUNT_SUSPENDED");
        assertThat(query.closed.get(5, TimeUnit.SECONDS).getCode()).isEqualTo(RealtimeHandler.UNAUTHORIZED.getCode());
        assertThat(query.received.stream().map(m -> m.path("type").asText())).doesNotContain("connected");
    }

    @Test
    void cuentaNoSuspendidaSeConectaNormalmente() throws Exception {
        String userId = UUID.randomUUID().toString();
        String value = token(userId);
        redisTemplate.opsForValue().set("tripbus:suspended:" + UUID.randomUUID(), "1", Duration.ofMinutes(5));

        Client client = new Client("");
        client.send("{\"type\":\"connect\",\"token\":\"" + value + "\"}");

        assertThat(client.next("connected").path("userId").asText()).isEqualTo(userId);
        client.ws.close();
    }
}
