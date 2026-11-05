package com.tripbus.realtime;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import com.tripbus.realtime.snapshot.SnapshotService;
import com.tripbus.realtime.ws.ClientSession;
import com.tripbus.realtime.ws.OfferViewers;
import com.tripbus.realtime.ws.SessionRegistry;
import com.tripbus.realtime.ws.WsMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
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
import org.springframework.web.socket.WebSocketSession;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Sala de espera (US-162): OfferQueueUpdated llega a oferta:{id}:cupo y OfferQueueAdmitted solo al pasajero
 * admitido. US-163: OfferViewers con las personas mirando la oferta en este gateway y en otra réplica simulada.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "tripbus.realtime.viewers-interval-ms=200"})
@ActiveProfiles("it")
@Testcontainers
class RealtimeSalaEsperaIT {

    @Container
    @ServiceConnection(name = "redis")
    static GenericContainer<?> redis = RedisTestSupport.container();

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

    @BeforeEach
    void snapshots() {
        when(snapshotService.snapshot(any())).thenReturn(Optional.of(NullNode.getInstance()));
    }

    private void publish(String type, String payload) {
        redisTemplate.opsForStream().add(StreamRecords.newRecord().in("tripbus:events:realtime-test").ofMap(Map.of(
                "eventId", UUID.randomUUID().toString(), "type", type, "occurredAt", Instant.now().toString(),
                "payload", payload)));
    }

    private WsTestClient connected(UUID userId) throws Exception {
        WsTestClient client = new WsTestClient(port, mapper);
        if (userId == null) {
            client.send("{\"type\":\"connect\"}");
        } else {
            String token = "token-" + UUID.randomUUID();
            when(jwtDecoder.decode(token)).thenReturn(Jwt.withTokenValue(token).header("alg", "RS256")
                    .subject(userId.toString()).claim("role", "PASAJERO").issuedAt(Instant.now())
                    .expiresAt(Instant.now().plusSeconds(600)).build());
            client.send("{\"type\":\"connect\",\"token\":\"" + token + "\"}");
        }
        client.next("connected");
        return client;
    }

    /** El siguiente event que no sea OfferViewers. */
    private static JsonNode nextEvent(WsTestClient client) throws InterruptedException {
        while (true) {
            JsonNode event = client.next("event");
            if (!"OfferViewers".equals(event.path("event").asText())) {
                return event;
            }
        }
    }

    @Test
    void losEventosDeLaSalaDeEsperaVanAlCupoYAlPasajeroAdmitido() throws Exception {
        UUID offer = UUID.randomUUID();
        UUID admitido = UUID.randomUUID();
        WsTestClient publico = connected(null);
        publico.send("{\"type\":\"subscribe\",\"topic\":\"oferta:" + offer + ":cupo\"}");
        publico.next("snapshot");
        WsTestClient pasajero = connected(admitido);
        pasajero.send("{\"type\":\"subscribe\",\"topic\":\"usuario:" + admitido + ":avisos\"}");
        pasajero.next("subscribed");
        WsTestClient otro = connected(UUID.randomUUID());

        publish("OfferQueueUpdated", "{\"offerId\":\"" + offer + "\",\"enFila\":42,\"admitidos\":10,"
                + "\"userId\":\"" + admitido + "\"}");
        JsonNode cola = nextEvent(publico);
        assertThat(cola.path("event").asText()).isEqualTo("OfferQueueUpdated");
        assertThat(cola.path("topic").asText()).isEqualTo("oferta:" + offer + ":cupo");
        assertThat(cola.path("data").path("enFila").asInt()).isEqualTo(42);
        assertThat(cola.path("data").path("admitidos").asInt()).isEqualTo(10);
        assertThat(cola.path("data").has("userId")).isFalse();

        publish("OfferQueueAdmitted", "{\"userId\":\"" + admitido + "\",\"offerId\":\"" + offer
                + "\",\"turno\":7,\"expiraEn\":\"2026-11-04T12:10:00Z\",\"email\":\"a@b.co\"}");
        JsonNode aviso = pasajero.next("event");
        assertThat(aviso.path("event").asText()).isEqualTo("OfferQueueAdmitted");
        assertThat(aviso.path("topic").asText()).isEqualTo("usuario:" + admitido + ":avisos");
        assertThat(aviso.path("data").path("offerId").asText()).isEqualTo(offer.toString());
        assertThat(aviso.path("data").path("expiraEn").asText()).isEqualTo("2026-11-04T12:10:00Z");
        assertThat(aviso.path("data").path("turno").asInt()).isEqualTo(7);
        assertThat(aviso.path("data").has("userId")).isFalse();
        assertThat(aviso.path("data").has("email")).isFalse();
        Thread.sleep(300);
        assertThat(otro.received).noneMatch(m -> "event".equals(m.path("type").asText()));
        assertThat(publico.received).noneMatch(m -> "OfferQueueAdmitted".equals(m.path("event").asText()));
    }

    @Test
    void personasMirandoSumaEsteGatewayYOtraReplica() throws Exception {
        UUID offer = UUID.randomUUID();
        String topic = "oferta:" + offer + ":cupo";
        WsTestClient client = connected(null);
        client.send("{\"type\":\"subscribe\",\"topic\":\"" + topic + "\"}");

        JsonNode solo = client.next("event");
        assertThat(solo.path("event").asText()).isEqualTo("OfferViewers");
        assertThat(solo.path("topic").asText()).isEqualTo(topic);
        assertThat(solo.path("data").path("offerId").asText()).isEqualTo(offer.toString());
        assertThat(solo.path("data").path("personas").asLong()).isEqualTo(1);
        assertThat(solo.has("seq")).isFalse();

        // Otra réplica con dos personas mirando la misma oferta.
        SessionRegistry otraRegistry = new SessionRegistry(new SimpleMeterRegistry());
        for (int i = 0; i < 2; i++) {
            WebSocketSession raw = mock(WebSocketSession.class);
            when(raw.getId()).thenReturn("otra-" + i);
            when(raw.isOpen()).thenReturn(true);
            ClientSession session = new ClientSession(raw, Instant.now());
            otraRegistry.add(session);
            otraRegistry.subscribe(session, topic);
        }
        new OfferViewers(otraRegistry, redisTemplate, mapper, new WsMetrics(new SimpleMeterRegistry(), otraRegistry),
                5000).tick();

        JsonNode tres = client.next("event");
        assertThat(tres.path("event").asText()).isEqualTo("OfferViewers");
        assertThat(tres.path("data").path("personas").asLong()).isEqualTo(3);
        client.ws.close();
    }
}
