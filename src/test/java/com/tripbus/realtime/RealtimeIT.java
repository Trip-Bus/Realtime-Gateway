package com.tripbus.realtime;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.tripbus.realtime.snapshot.SnapshotService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtException;
import org.springframework.test.context.TestPropertySource;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.net.URI;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/** US-020/US-021: conexión autenticada, snapshot inicial y difusión en vivo (Redis real). */
@EnabledIfEnvironmentVariable(named = "TRIPBUS_IT", matches = "true")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestPropertySource(properties = "EVENTS_STREAM=tripbus:events:realtime-test")
class RealtimeIT {

    @LocalServerPort
    private int port;
    @Autowired
    private StringRedisTemplate redis;
    @Autowired
    private ObjectMapper mapper;

    @MockBean
    private JwtDecoder jwtDecoder;
    @MockBean
    private SnapshotService snapshotService;

    private final BlockingQueue<JsonNode> received = new LinkedBlockingQueue<>();

    private WebSocketSession connect(String query) throws Exception {
        return new StandardWebSocketClient().execute(new TextWebSocketHandler() {
            @Override
            protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
                received.add(mapper.readTree(message.getPayload()));
            }
        }, null, URI.create("ws://localhost:" + port + "/ws" + query)).get(5, TimeUnit.SECONDS);
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

    @Test
    void autenticaEnviaSnapshotYDifundeCambiosDeAsientosSinExponerUsuarios() throws Exception {
        Jwt jwt = Jwt.withTokenValue("token-valido").header("alg", "RS256").subject(UUID.randomUUID().toString())
                .claim("role", "PASAJERO").expiresAt(Instant.now().plusSeconds(600)).issuedAt(Instant.now()).build();
        when(jwtDecoder.decode("token-valido")).thenReturn(jwt);
        UUID itineraryId = UUID.randomUUID();
        when(snapshotService.snapshot(any())).thenReturn(Optional.of(
                JsonNodeFactory.instance.objectNode().put("itineraryId", itineraryId.toString())));

        WebSocketSession ws = connect("?token=token-valido");
        assertThat(next("connected").path("role").asText()).isEqualTo("PASAJERO");

        String topic = "itinerario:" + itineraryId + ":asientos";
        ws.sendMessage(new TextMessage("{\"type\":\"subscribe\",\"topic\":\"" + topic + "\"}"));
        next("subscribed");
        assertThat(next("snapshot").path("data").path("itineraryId").asText()).isEqualTo(itineraryId.toString());

        redis.opsForStream().add(StreamRecords.newRecord().in("tripbus:events:realtime-test").ofMap(Map.of(
                "eventId", UUID.randomUUID().toString(), "type", "SeatHeld", "source", "test",
                "occurredAt", Instant.now().toString(),
                "payload", "{\"itineraryId\":\"" + itineraryId + "\",\"seatNumbers\":[\"7\"],\"userId\":\"secreto\"}")));
        JsonNode event = next("event");
        assertThat(event.path("event").asText()).isEqualTo("SeatHeld");
        assertThat(event.path("topic").asText()).isEqualTo(topic);
        assertThat(event.path("data").path("seatNumbers").get(0).asText()).isEqualTo("7");
        assertThat(event.path("data").has("userId")).isFalse();

        ws.sendMessage(new TextMessage("{\"type\":\"ping\"}"));
        next("pong");
        ws.close();
    }

    @Test
    void rechazaTokensInvalidosEnElHandshakeYTopicosInvalidos() throws Exception {
        when(jwtDecoder.decode("malo")).thenThrow(new JwtException("firma inválida"));
        assertThatThrownBy(() -> connect("?token=malo")).hasRootCauseInstanceOf(Exception.class);

        WebSocketSession ws = connect("");
        ws.sendMessage(new TextMessage("{\"type\":\"subscribe\",\"topic\":\"bus:x:posicion\"}"));
        assertThat(next("error").path("code").asText()).isEqualTo("UNAUTHENTICATED");
        ws.close();
    }
}
