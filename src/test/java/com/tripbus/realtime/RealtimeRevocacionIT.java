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
 * US-006: tokens revocados por logout (clave tripbus:revoked:{jti} y evento SessionRevoked de
 * identity) y numeración de los eventos (seq) frente al snapshot (asOfSeq), contra Redis 7 real.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "tripbus.realtime.allow-query-token=false")
@ActiveProfiles("it")
@Testcontainers
class RealtimeRevocacionIT {

    private static final String STREAM = "tripbus:events:realtime-test";

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
            send("{\"type\":\"connect\",\"token\":\"" + token + "\"}");
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

    private String token(String userId, String jti) {
        String value = "token-" + jti;
        when(jwtDecoder.decode(value)).thenReturn(Jwt.withTokenValue(value).header("alg", "RS256").subject(userId)
                .jti(jti).claim("role", "PASAJERO").expiresAt(Instant.now().plusSeconds(600))
                .issuedAt(Instant.now()).build());
        return value;
    }

    private String publish(String type, String payload) {
        return redisTemplate.opsForStream().add(StreamRecords.newRecord().in(STREAM).ofMap(Map.of(
                "eventId", UUID.randomUUID().toString(), "type", type, "source", "test",
                "occurredAt", Instant.now().toString(), "payload", payload))).getValue();
    }

    private static int compareSeq(String a, String b) {
        String[] x = a.split("-");
        String[] y = b.split("-");
        int ms = Long.compare(Long.parseLong(x[0]), Long.parseLong(y[0]));
        return ms != 0 ? ms : Long.compare(Long.parseLong(x[1]), Long.parseLong(y[1]));
    }

    @Test
    void tokenConClaveDeRevocacionSeRechazaAlConectarCon4401() throws Exception {
        String jti = UUID.randomUUID().toString();
        String token = token(UUID.randomUUID().toString(), jti);
        redisTemplate.opsForValue().set("tripbus:revoked:" + jti, "1", Duration.ofMinutes(5));

        Client client = new Client(token);

        assertThat(client.next("error").path("code").asText()).isEqualTo("UNAUTHENTICATED");
        assertThat(client.closed.get(5, TimeUnit.SECONDS).getCode()).isEqualTo(RealtimeHandler.UNAUTHORIZED.getCode());
        assertThat(client.received.stream().map(m -> m.path("type").asText())).doesNotContain("connected");
    }

    @Test
    void sessionRevokedCierraSoloLaSesionDeEseJtiYNoSeReenvia() throws Exception {
        String userId = UUID.randomUUID().toString();
        String revokedJti = UUID.randomUUID().toString();
        // Se simulan todos los tokens antes de conectar: stubbing y decode a la vez rompen a Mockito.
        String revokedToken = token(userId, revokedJti);
        String otherTokenValue = token(userId, UUID.randomUUID().toString());
        Client revoked = new Client(revokedToken);
        Client otherToken = new Client(otherTokenValue);
        Client anonymous = new Client("");
        revoked.next("connected");
        otherToken.next("connected");
        anonymous.next("connected");
        String topic = "usuario:" + userId + ":tiquetes";
        otherToken.send("{\"type\":\"subscribe\",\"topic\":\"" + topic + "\"}");
        otherToken.next("subscribed");

        publish("SessionRevoked", "{\"jti\":\"" + revokedJti + "\",\"userId\":\"" + userId + "\"}");

        assertThat(revoked.next("error").path("code").asText()).isEqualTo("UNAUTHENTICATED");
        assertThat(revoked.closed.get(5, TimeUnit.SECONDS).getCode()).isEqualTo(RealtimeHandler.UNAUTHORIZED.getCode());
        // Las demás siguen abiertas y no reciben el SessionRevoked como evento.
        otherToken.send("{\"type\":\"ping\"}");
        otherToken.next("pong");
        anonymous.send("{\"type\":\"ping\"}");
        anonymous.next("pong");
        assertThat(otherToken.received.stream().map(m -> m.path("type").asText())).doesNotContain("event");
        assertThat(otherToken.ws.isOpen()).isTrue();
        assertThat(anonymous.ws.isOpen()).isTrue();
        otherToken.ws.close();
        anonymous.ws.close();
    }

    @Test
    void userSuspendedCierraTodasLasSesionesDelUsuario() throws Exception {
        String userId = UUID.randomUUID().toString();
        String firstToken = token(userId, UUID.randomUUID().toString());
        String secondToken = token(userId, UUID.randomUUID().toString());
        String otherToken = token(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        Client first = new Client(firstToken);
        Client second = new Client(secondToken);
        Client other = new Client(otherToken);
        first.next("connected");
        second.next("connected");
        other.next("connected");

        publish("UserSuspended", "{\"userId\":\"" + userId + "\"}");

        assertThat(first.closed.get(5, TimeUnit.SECONDS).getCode()).isEqualTo(RealtimeHandler.UNAUTHORIZED.getCode());
        assertThat(second.closed.get(5, TimeUnit.SECONDS).getCode()).isEqualTo(RealtimeHandler.UNAUTHORIZED.getCode());
        other.send("{\"type\":\"ping\"}");
        other.next("pong");
        other.ws.close();
    }

    @Test
    void losEventosLlevanSeqCrecienteYElSnapshotAsOfSeq() throws Exception {
        UUID itineraryId = UUID.randomUUID();
        when(snapshotService.snapshot(any())).thenReturn(Optional.of(JsonNodeFactory.instance.objectNode()));
        // Un evento ya difundido antes de suscribirse: el snapshot debe quedar en ese id o después.
        String before = publish("SeatHeld", "{\"itineraryId\":\"" + UUID.randomUUID() + "\"}");
        Client client = new Client(token(UUID.randomUUID().toString(), UUID.randomUUID().toString()));
        client.next("connected");
        String topic = "itinerario:" + itineraryId + ":asientos";
        long deadline = System.currentTimeMillis() + 5_000;
        JsonNode snapshot;
        do {
            client.send("{\"type\":\"subscribe\",\"topic\":\"" + topic + "\"}");
            snapshot = client.next("snapshot");
        } while (compareSeq(snapshot.path("asOfSeq").asText(), before) < 0 && System.currentTimeMillis() < deadline);
        String asOfSeq = snapshot.path("asOfSeq").asText();
        assertThat(compareSeq(asOfSeq, before)).isGreaterThanOrEqualTo(0);

        String payload = "{\"itineraryId\":\"" + itineraryId + "\",\"seatNumbers\":[\"1\"]}";
        String id1 = publish("SeatHeld", payload);
        String id2 = publish("SeatReleased", payload);

        String seq1 = client.next("event").path("seq").asText();
        String seq2 = client.next("event").path("seq").asText();
        assertThat(seq1).isEqualTo(id1);
        assertThat(seq2).isEqualTo(id2);
        assertThat(compareSeq(seq2, seq1)).isPositive();
        assertThat(compareSeq(seq1, asOfSeq)).isPositive();
        client.ws.close();
    }
}
