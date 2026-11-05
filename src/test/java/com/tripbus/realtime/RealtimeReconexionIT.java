package com.tripbus.realtime;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.tripbus.realtime.snapshot.SnapshotService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.web.client.RestClient;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * US-184: al reconectar, {@code resume} reenvía en orden lo que siga en el búfer de Redis y termina con
 * {@code resumed}; si ya no está (búfer de 3 eventos en esta prueba) manda el snapshot con asOfSeq, como un
 * subscribe. Con la misma autorización que subscribe. Contra Redis 7 real.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "tripbus.realtime.replay.size=3"})
@AutoConfigureObservability
@ActiveProfiles("it")
@Testcontainers
class RealtimeReconexionIT {

    private static final String STREAM = "tripbus:events:realtime-test";

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
    void snapshot() {
        when(snapshotService.snapshot(any())).thenReturn(Optional.of(
                JsonNodeFactory.instance.objectNode().put("estado", "actual")));
    }

    private WsTestClient anonymous() throws Exception {
        WsTestClient client = new WsTestClient(port, mapper);
        client.send("{\"type\":\"connect\"}");
        client.next("connected");
        return client;
    }

    private WsTestClient authenticated(UUID userId) throws Exception {
        String token = "token-" + UUID.randomUUID();
        when(jwtDecoder.decode(token)).thenReturn(Jwt.withTokenValue(token).header("alg", "RS256")
                .subject(userId.toString()).claim("role", "PASAJERO").issuedAt(Instant.now())
                .expiresAt(Instant.now().plusSeconds(600)).build());
        WsTestClient client = new WsTestClient(port, mapper);
        client.send("{\"type\":\"connect\",\"token\":\"" + token + "\"}");
        client.next("connected");
        return client;
    }

    private RecordId seatHeld(UUID itinerary, String seat) {
        return publish("SeatHeld", "{\"itineraryId\":\"" + itinerary + "\",\"seatNumbers\":[\"" + seat + "\"]}");
    }

    private RecordId publish(String type, String payload) {
        return redisTemplate.opsForStream().add(StreamRecords.newRecord().in(STREAM).ofMap(Map.of(
                "eventId", UUID.randomUUID().toString(), "type", type, "source", "test",
                "occurredAt", Instant.now().toString(), "payload", payload)));
    }

    /** Espera a que el gateway haya guardado el evento {@code id} en el búfer del tópico. */
    private void awaitBuffered(String topic, RecordId id) throws InterruptedException {
        for (int i = 0; i < 100; i++) {
            var last = redisTemplate.opsForStream().reverseRange("tripbus:replay:" + topic, Range.unbounded(),
                    org.springframework.data.redis.connection.Limit.limit().count(1));
            if (last != null && !last.isEmpty() && last.get(0).getId().equals(id)) {
                return;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("el evento " + id + " no llegó al búfer de " + topic);
    }

    private static List<String> types(List<JsonNode> messages) {
        return messages.stream().map(m -> m.path("type").asText() + (m.has("seq") ? ":" + m.path("seq").asText() : ""))
                .toList();
    }

    private String prometheus() {
        return RestClient.create("http://localhost:" + port).get().uri("/actuator/prometheus").retrieve()
                .body(String.class);
    }

    @Test
    void resumeReenviaEnOrdenLoPerdidoMientrasEstabaDesconectadoYTerminaConResumed() throws Exception {
        UUID itinerary = UUID.randomUUID();
        String topic = "itinerario:" + itinerary + ":asientos";
        WsTestClient antes = anonymous();
        antes.send("{\"type\":\"subscribe\",\"topic\":\"" + topic + "\"}");
        antes.next("snapshot");
        seatHeld(itinerary, "1");
        String ultimoRecibido = antes.next("event").path("seq").asText();
        antes.ws.close();
        antes.awaitClose();

        // Mientras estuvo desconectado.
        RecordId e2 = seatHeld(itinerary, "2");
        RecordId e3 = seatHeld(itinerary, "3");
        awaitBuffered(topic, e3);

        // Reconecta (podría ser otra réplica: el búfer está en Redis).
        WsTestClient despues = anonymous();
        despues.send("{\"type\":\"resume\",\"topic\":\"" + topic + "\",\"lastSeq\":\"" + ultimoRecibido + "\"}");
        List<JsonNode> messages = despues.until("resumed");

        assertThat(types(messages)).containsExactly("subscribed", "event:" + e2.getValue(), "event:" + e3.getValue(),
                "resumed");
        assertThat(messages.get(1).path("data").path("seatNumbers").get(0).asText()).isEqualTo("2");
        assertThat(messages.get(3).path("lastSeq").asText()).isEqualTo(e3.getValue());
        assertThat(messages.get(3).path("topic").asText()).isEqualTo(topic);

        // Y sigue suscrito en vivo.
        RecordId e4 = seatHeld(itinerary, "4");
        assertThat(despues.next("event").path("seq").asText()).isEqualTo(e4.getValue());
        assertThat(prometheus()).contains("tripbus_ws_replay_total{application=\"realtime-gateway\",resultado=\"completo\"}");
        despues.ws.close();
    }

    @Test
    void siLoPerdidoYaNoEstaEnElBuferMandaElSnapshotConAsOfSeq() throws Exception {
        UUID itinerary = UUID.randomUUID();
        String topic = "itinerario:" + itinerary + ":asientos";
        RecordId primero = seatHeld(itinerary, "1");
        RecordId ultimo = null;
        for (int i = 2; i <= 5; i++) {
            ultimo = seatHeld(itinerary, String.valueOf(i));
        }
        awaitBuffered(topic, ultimo);

        // El búfer solo guarda 3: después de "primero" faltaría el 2.
        WsTestClient client = anonymous();
        client.send("{\"type\":\"resume\",\"topic\":\"" + topic + "\",\"lastSeq\":\"" + primero.getValue() + "\"}");
        List<JsonNode> messages = client.until("snapshot");

        assertThat(types(messages)).containsExactly("subscribed", "snapshot");
        JsonNode snapshot = messages.get(1);
        assertThat(snapshot.path("data").path("estado").asText()).isEqualTo("actual");
        assertThat(snapshot.path("asOfSeq").asText()).isNotBlank();
        assertThat(snapshot.path("topic").asText()).isEqualTo(topic);

        // Un tópico sin eventos en el búfer también cae al snapshot.
        String otro = "oferta:" + UUID.randomUUID() + ":cupo";
        client.send("{\"type\":\"resume\",\"topic\":\"" + otro + "\",\"lastSeq\":\"1-0\"}");
        assertThat(client.next("snapshot").path("topic").asText()).isEqualTo(otro);
        assertThat(prometheus()).contains("resultado=\"snapshot\"");
        client.ws.close();
    }

    @Test
    void resumeExigeLaMismaAutorizacionQueSubscribe() throws Exception {
        UUID owner = UUID.randomUUID();
        String avisos = "usuario:" + owner + ":avisos";

        WsTestClient anonimo = anonymous();
        anonimo.send("{\"type\":\"resume\",\"topic\":\"" + avisos + "\",\"lastSeq\":\"1-0\"}");
        assertThat(anonimo.next("error").path("code").asText()).isEqualTo("FORBIDDEN_TOPIC");

        WsTestClient otro = authenticated(UUID.randomUUID());
        otro.send("{\"type\":\"resume\",\"topic\":\"" + avisos + "\",\"lastSeq\":\"1-0\"}");
        assertThat(otro.next("error").path("code").asText()).isEqualTo("FORBIDDEN_TOPIC");

        // Sin connect, y con un lastSeq que no es un seq.
        WsTestClient sinConnect = new WsTestClient(port, mapper);
        sinConnect.send("{\"type\":\"resume\",\"topic\":\"" + avisos + "\",\"lastSeq\":\"1-0\"}");
        assertThat(sinConnect.next("error").path("code").asText()).isEqualTo("UNAUTHENTICATED");
        WsTestClient dueno = authenticated(owner);
        dueno.send("{\"type\":\"resume\",\"topic\":\"" + avisos + "\",\"lastSeq\":\"ayer\"}");
        assertThat(dueno.next("error").path("code").asText()).isEqualTo("INVALID_SEQ");

        // Ninguno quedó suscrito: el aviso no les llega; el dueño, al reanudar bien, sí lo recibe.
        dueno.send("{\"type\":\"resume\",\"topic\":\"" + avisos + "\",\"lastSeq\":\"1-0\"}");
        JsonNode snapshot = dueno.next("snapshot");
        assertThat(snapshot.path("data").isNull()).as("tópico sin snapshot: data null").isTrue();
        publish("BookingAffected", "{\"userId\":\"" + owner + "\",\"bookingId\":\"b-1\",\"causa\":\"CANCELACION\"}");
        assertThat(dueno.next("event").path("data").path("bookingId").asText()).isEqualTo("b-1");
        Thread.sleep(300);
        assertThat(anonimo.received).noneMatch(m -> "event".equals(m.path("type").asText()));
        assertThat(otro.received).noneMatch(m -> "event".equals(m.path("type").asText()));
        assertThat(anonimo.ws.isOpen()).isTrue();
        assertThat(prometheus()).contains("resultado=\"rechazado\"");
        for (WsTestClient client : List.of(anonimo, otro, sinConnect, dueno)) {
            client.ws.close();
        }
    }
}
