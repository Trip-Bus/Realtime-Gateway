package com.tripbus.realtime;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tripbus.realtime.snapshot.SnapshotService;
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
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * US-033: aviso privado cuando se emite el tiquete. Solo el dueño sigue usuario:{uuid}:tiquetes
 * (sin snapshot) y recibe TicketIssued sin qrCode ni userId; otro usuario, un ADMIN y un anónimo
 * reciben FORBIDDEN_TOPIC sin que se cierre la conexión.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("it")
@Testcontainers
class RealtimeTiquetesIT {

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

    /** Una conexión con su propia cola de mensajes. */
    private final class Client {
        private final BlockingQueue<JsonNode> received = new LinkedBlockingQueue<>();
        private final WebSocketSession ws;

        Client() throws Exception {
            ws = new StandardWebSocketClient().execute(new TextWebSocketHandler() {
                @Override
                protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
                    received.add(mapper.readTree(message.getPayload()));
                }
            }, null, URI.create("ws://localhost:" + port + "/ws")).get(5, TimeUnit.SECONDS);
        }

        void send(String json) throws Exception {
            ws.sendMessage(new TextMessage(json));
        }

        void subscribe(String topic) throws Exception {
            send("{\"type\":\"subscribe\",\"topic\":\"" + topic + "\"}");
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

    private Client authenticated(UUID userId, String role) throws Exception {
        String token = "token-" + UUID.randomUUID();
        Jwt jwt = Jwt.withTokenValue(token).header("alg", "RS256").subject(userId.toString())
                .claim("role", role).expiresAt(Instant.now().plusSeconds(600)).issuedAt(Instant.now()).build();
        when(jwtDecoder.decode(token)).thenReturn(jwt);
        Client client = new Client();
        client.send("{\"type\":\"connect\",\"token\":\"" + token + "\"}");
        client.next("connected");
        return client;
    }

    private void publishTicketIssued(UUID userId, UUID bookingId, UUID ticketId, UUID itineraryId) {
        redisTemplate.opsForStream().add(StreamRecords.newRecord().in("tripbus:events:realtime-test").ofMap(Map.of(
                "eventId", UUID.randomUUID().toString(), "type", "TicketIssued", "source", "booking",
                "occurredAt", Instant.now().toString(),
                "payload", """
                        {"bookingId":"%s","ticketId":"%s","qrCode":"QR-SECRETO","itineraryId":"%s",
                         "seatNumbers":["5","6"],"userId":"%s","offerId":"%s"}"""
                        .formatted(bookingId, ticketId, itineraryId, userId, UUID.randomUUID()))));
    }

    @Test
    void elDuenoRecibeElAvisoSinQrNiUserIdYSinSnapshot() throws Exception {
        UUID userId = UUID.randomUUID();
        Client owner = authenticated(userId, "PASAJERO");
        String topic = "usuario:" + userId + ":tiquetes";
        // El uuid del tema se compara sin importar mayúsculas.
        owner.subscribe("usuario:" + userId.toString().toUpperCase() + ":tiquetes");
        assertThat(owner.next("subscribed").path("topic").asText()).isEqualTo(topic);

        UUID bookingId = UUID.randomUUID();
        UUID ticketId = UUID.randomUUID();
        UUID itineraryId = UUID.randomUUID();
        publishTicketIssued(userId, bookingId, ticketId, itineraryId);

        JsonNode event = owner.next("event");
        assertThat(event.path("topic").asText()).isEqualTo(topic);
        assertThat(event.path("event").asText()).isEqualTo("TicketIssued");
        JsonNode data = event.path("data");
        assertThat(data.path("bookingId").asText()).isEqualTo(bookingId.toString());
        assertThat(data.path("ticketId").asText()).isEqualTo(ticketId.toString());
        assertThat(data.path("itineraryId").asText()).isEqualTo(itineraryId.toString());
        assertThat(data.path("seatNumbers")).hasSize(2);
        assertThat(data.has("qrCode")).isFalse();
        assertThat(data.has("userId")).isFalse();
        assertThat(data.has("offerId")).isFalse();
        assertThat(event.toString()).doesNotContain("QR-SECRETO");

        // El tema no tiene snapshot: nunca se pide ni llega uno.
        assertThat(owner.received).noneMatch(msg -> "snapshot".equals(msg.path("type").asText()));
        verify(snapshotService, never()).snapshot(any());
        owner.ws.close();
    }

    @Test
    void otroUsuarioYUnAdminNoPuedenSeguirTemasAjenos() throws Exception {
        String ajeno = "usuario:" + UUID.randomUUID() + ":tiquetes";
        for (String role : new String[] {"PASAJERO", "ADMIN"}) {
            Client other = authenticated(UUID.randomUUID(), role);
            other.subscribe(ajeno);
            JsonNode error = other.next("error");
            assertThat(error.path("code").asText()).as(role).isEqualTo("FORBIDDEN_TOPIC");
            assertThat(other.ws.isOpen()).isTrue();
            other.send("{\"type\":\"ping\"}");
            other.next("pong");
            other.ws.close();
        }
    }

    @Test
    void unAnonimoNoPuedeSeguirTemasDeTiquetes() throws Exception {
        Client anonymous = new Client();
        anonymous.send("{\"type\":\"connect\"}");
        assertThat(anonymous.next("connected").path("anonymous").asBoolean()).isTrue();

        anonymous.subscribe("usuario:" + UUID.randomUUID() + ":tiquetes");
        assertThat(anonymous.next("error").path("code").asText()).isEqualTo("FORBIDDEN_TOPIC");
        assertThat(anonymous.ws.isOpen()).isTrue();
        anonymous.ws.close();
    }
}
