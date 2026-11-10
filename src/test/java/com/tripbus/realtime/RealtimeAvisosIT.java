package com.tripbus.realtime;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import com.tripbus.realtime.snapshot.SnapshotService;
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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * US-183, US-186 y alertas de empresa: usuario:{uuid}:avisos, conductor:{uuid}:viaje y
 * empresa:{uuid}:alertas son privados (dueño, rol y empresa), sin snapshot y con FORBIDDEN_TOPIC sin
 * cerrar la conexión. Los públicos del viaje reciben EtaUpdated e ItineraryRescheduled sin datos
 * privados, y TripAlertRaised nunca llega a ellos. OPERADOR y ADMIN tienen su propio límite.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "tripbus.realtime.max-subscriptions=5",
        "tripbus.realtime.max-staff-subscriptions=8"})
@ActiveProfiles("it")
@Testcontainers
class RealtimeAvisosIT {

    private static final String FORBIDDEN = "FORBIDDEN_TOPIC";

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

    @BeforeEach
    void snapshotsVacios() {
        when(snapshotService.snapshot(any())).thenReturn(Optional.of(NullNode.getInstance()));
    }

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

        /** Se suscribe y espera el subscribed (o el error, que devuelve). */
        JsonNode subscribeAndWait(String topic) throws Exception {
            subscribe(topic);
            while (true) {
                JsonNode msg = received.poll(5, TimeUnit.SECONDS);
                assertThat(msg).as("respuesta a subscribe " + topic).isNotNull();
                String type = msg.path("type").asText();
                if ("subscribed".equals(type) || "error".equals(type)) {
                    return msg;
                }
            }
        }

        /** FORBIDDEN_TOPIC y la conexión sigue abierta y respondiendo. */
        void assertForbidden(String topic) throws Exception {
            JsonNode error = subscribeAndWait(topic);
            assertThat(error.path("type").asText()).as(topic).isEqualTo("error");
            assertThat(error.path("code").asText()).as(topic).isEqualTo(FORBIDDEN);
            assertThat(ws.isOpen()).isTrue();
            send("{\"type\":\"ping\"}");
            next("pong");
        }
    }

    private Client authenticated(UUID userId, String role, UUID companyId) throws Exception {
        String token = "token-" + UUID.randomUUID();
        Jwt.Builder builder = Jwt.withTokenValue(token).header("alg", "RS256").subject(userId.toString())
                .claim("role", role).expiresAt(Instant.now().plusSeconds(600)).issuedAt(Instant.now());
        if (companyId != null) {
            builder.claim("companyId", companyId.toString());
        }
        when(jwtDecoder.decode(token)).thenReturn(builder.build());
        Client client = new Client();
        client.send("{\"type\":\"connect\",\"token\":\"" + token + "\"}");
        client.next("connected");
        return client;
    }

    private Client anonymous() throws Exception {
        Client client = new Client();
        client.send("{\"type\":\"connect\"}");
        assertThat(client.next("connected").path("anonymous").asBoolean()).isTrue();
        return client;
    }

    private void publish(String type, String payload) {
        redisTemplate.opsForStream().add(StreamRecords.newRecord().in("tripbus:events:realtime-test").ofMap(Map.of(
                "eventId", UUID.randomUUID().toString(), "type", type, "source", "test",
                "occurredAt", Instant.now().toString(), "payload", payload)));
    }

    @Test
    void elPasajeroRecibeSusAvisosSoloConIdsYDatosDeLaDecision() throws Exception {
        UUID userId = UUID.randomUUID();
        Client owner = authenticated(userId, "PASAJERO", null);
        String topic = "usuario:" + userId + ":avisos";
        assertThat(owner.subscribeAndWait(topic).path("type").asText()).isEqualTo("subscribed");

        UUID bookingId = UUID.randomUUID();
        publish("BookingAffected", """
                {"userId":"%s","bookingId":"%s","ticketId":"t1","itineraryId":"i1","causa":"CANCELACION",
                 "plazoDecision":"2026-11-01T12:00:00Z","opciones":["REUBICAR","REEMBOLSO"],
                 "email":"ana@correo.co","numeroDocumento":"10203040","nombre":"Ana","qrCode":"QR-SECRETO"}"""
                .formatted(userId, bookingId));

        JsonNode event = owner.next("event");
        assertThat(event.path("topic").asText()).isEqualTo(topic);
        assertThat(event.path("event").asText()).isEqualTo("BookingAffected");
        JsonNode data = event.path("data");
        assertThat(data.path("bookingId").asText()).isEqualTo(bookingId.toString());
        assertThat(data.path("causa").asText()).isEqualTo("CANCELACION");
        assertThat(data.path("opciones")).hasSize(2);
        assertThat(event.toString()).doesNotContain("ana@correo.co", "10203040", "Ana", "QR-SECRETO", "userId");

        // TicketIssued llega también aquí; sin userId un aviso no se difunde a nadie.
        publish("RefundProcessed", "{\"bookingId\":\"%s\",\"refundId\":\"r0\"}".formatted(bookingId));
        publish("TicketIssued", "{\"userId\":\"%s\",\"ticketId\":\"t9\",\"qrCode\":\"QR\"}".formatted(userId));
        JsonNode issued = owner.next("event");
        assertThat(issued.path("event").asText()).isEqualTo("TicketIssued");
        assertThat(issued.path("data").toString()).isEqualTo("{\"ticketId\":\"t9\"}");
        assertThat(owner.received).noneMatch(msg -> "snapshot".equals(msg.path("type").asText()));
        owner.ws.close();
    }

    @Test
    void otroUsuarioYUnAdminNoSiguenLosAvisosAjenos() throws Exception {
        String ajeno = "usuario:" + UUID.randomUUID() + ":avisos";
        for (String role : new String[] {"PASAJERO", "ADMIN"}) {
            Client other = authenticated(UUID.randomUUID(), role, null);
            other.assertForbidden(ajeno);
            other.ws.close();
        }
    }

    @Test
    void elConductorRecibeSusAsignacionesYNadieMasLasSigue() throws Exception {
        UUID conductorId = UUID.randomUUID();
        String topic = "conductor:" + conductorId + ":viaje";
        Client conductor = authenticated(conductorId, "CONDUCTOR", null);
        assertThat(conductor.subscribeAndWait(topic).path("type").asText()).isEqualTo("subscribed");

        publish("DriverAssigned", "{\"itineraryId\":\"i1\",\"companyId\":\"c1\",\"conductorId\":\"%s\",\"rol\":\"PRINCIPAL\"}"
                .formatted(conductorId));
        JsonNode event = conductor.next("event");
        assertThat(event.path("event").asText()).isEqualTo("DriverAssigned");
        assertThat(event.path("data").path("conductorId").asText()).isEqualTo(conductorId.toString());
        assertThat(conductor.received).noneMatch(msg -> "snapshot".equals(msg.path("type").asText()));

        // El mismo uuid sin rol CONDUCTOR, otro conductor y un ADMIN: FORBIDDEN_TOPIC.
        Client sinRol = authenticated(conductorId, "PASAJERO", null);
        sinRol.assertForbidden(topic);
        Client otro = authenticated(UUID.randomUUID(), "CONDUCTOR", null);
        otro.assertForbidden(topic);
        Client admin = authenticated(UUID.randomUUID(), "ADMIN", null);
        admin.assertForbidden(topic);
        conductor.ws.close();
        sinRol.ws.close();
        otro.ws.close();
        admin.ws.close();
    }

    @Test
    void elOperadorDeLaEmpresaYElAdminRecibenLasAlertasYOtroOperadorNo() throws Exception {
        UUID companyId = UUID.randomUUID();
        String topic = "empresa:" + companyId + ":alertas";
        Client operador = authenticated(UUID.randomUUID(), "OPERADOR", companyId);
        Client admin = authenticated(UUID.randomUUID(), "ADMIN", null);
        assertThat(operador.subscribeAndWait(topic).path("type").asText()).isEqualTo("subscribed");
        assertThat(admin.subscribeAndWait(topic).path("type").asText()).isEqualTo("subscribed");

        UUID conductorId = UUID.randomUUID();
        publish("TripAlertRaised", """
                {"alertId":"a1","itineraryId":"%s","companyId":"%s","tipo":"SOS","gravedad":"CRITICA",
                 "mensaje":"Asalto en curso"}""".formatted(UUID.randomUUID(), companyId));
        publish("DriverAssignmentConflict", "{\"itineraryId\":\"i1\",\"companyId\":\"%s\",\"conductorId\":\"%s\"}"
                .formatted(companyId, conductorId));
        publish("TrackingStatusChanged", "{\"itineraryId\":\"i1\",\"companyId\":\"%s\",\"estado\":\"ACTIVO\"}"
                .formatted(companyId));
        publish("TrackingStatusChanged", "{\"itineraryId\":\"i1\",\"companyId\":\"%s\",\"estado\":\"SIN_SENAL\"}"
                .formatted(companyId));

        for (Client client : new Client[] {operador, admin}) {
            JsonNode alerta = client.next("event");
            assertThat(alerta.path("event").asText()).isEqualTo("TripAlertRaised");
            assertThat(alerta.path("data").path("mensaje").asText()).isEqualTo("Asalto en curso");
            JsonNode conflicto = client.next("event");
            assertThat(conflicto.path("event").asText()).isEqualTo("DriverAssignmentConflict");
            assertThat(conflicto.path("data").path("conductorId").asText()).isEqualTo(conductorId.toString());
            // ACTIVO no llega al canal de alertas: el siguiente es el SIN_SENAL.
            JsonNode sinSenal = client.next("event");
            assertThat(sinSenal.path("event").asText()).isEqualTo("TrackingStatusChanged");
            assertThat(sinSenal.path("data").path("estado").asText()).isEqualTo("SIN_SENAL");
            assertThat(client.received).noneMatch(msg -> "snapshot".equals(msg.path("type").asText()));
        }

        Client otraEmpresa = authenticated(UUID.randomUUID(), "OPERADOR", UUID.randomUUID());
        otraEmpresa.assertForbidden(topic);
        Client pasajero = authenticated(UUID.randomUUID(), "PASAJERO", companyId);
        pasajero.assertForbidden(topic);
        operador.ws.close();
        admin.ws.close();
        otraEmpresa.ws.close();
        pasajero.ws.close();
    }

    @Test
    void unAnonimoNoSigueNingunTemaPrivado() throws Exception {
        Client anonymous = anonymous();
        UUID id = UUID.randomUUID();
        anonymous.assertForbidden("usuario:" + id + ":avisos");
        anonymous.assertForbidden("conductor:" + id + ":viaje");
        anonymous.assertForbidden("empresa:" + id + ":alertas");
        anonymous.ws.close();
    }

    @Test
    void losPublicosDelViajeRecibenEtaYReprogramacionSinDatosPrivadosNiAlertas() throws Exception {
        UUID itinerary = UUID.randomUUID();
        String posicion = "viaje:" + itinerary + ":posicion";
        String eventos = "viaje:" + itinerary + ":eventos";
        Client anonymous = anonymous();
        assertThat(anonymous.subscribeAndWait(posicion).path("type").asText()).isEqualTo("subscribed");
        assertThat(anonymous.subscribeAndWait(eventos).path("type").asText()).isEqualTo("subscribed");

        // TripAlertRaised lleva itineraryId, pero nunca se difunde a un tópico público.
        publish("TripAlertRaised", "{\"itineraryId\":\"%s\",\"companyId\":\"%s\",\"tipo\":\"SOS\",\"mensaje\":\"SOS-SECRETO\"}"
                .formatted(itinerary, UUID.randomUUID()));
        publish("EtaUpdated", """
                {"itineraryId":"%s","retrasoMinutos":7,"conductorId":"cond-secreto",
                 "paradas":[{"orden":1,"nombre":"Terminal Norte","estimada":"2026-10-28T10:00:00Z"}]}"""
                .formatted(itinerary));
        publish("ItineraryRescheduled", """
                {"itineraryId":"%s","companyId":"c1","tipo":"RETRASO","minutosDiferencia":40,"motivo":"Vía en obras",
                 "email":"ops@empresa.co","numeroDocumento":"99","nombre":"Operador"}""".formatted(itinerary));
        publish("TripEventReported", """
                {"tripEventId":"e1","itineraryId":"%s","tipo":"ACCIDENTE","descripcion":"Choque leve",
                 "payload":{"conductorId":"cond-secreto"}}""".formatted(itinerary));

        JsonNode eta = anonymous.next("event");
        assertThat(eta.path("event").asText()).isEqualTo("EtaUpdated");
        assertThat(eta.path("topic").asText()).isEqualTo(posicion);
        assertThat(eta.path("data").path("retrasoMinutos").asInt()).isEqualTo(7);
        assertThat(eta.path("data").path("paradas").get(0).path("nombre").asText()).isEqualTo("Terminal Norte");

        JsonNode reprogramado = anonymous.next("event");
        assertThat(reprogramado.path("event").asText()).isEqualTo("ItineraryRescheduled");
        assertThat(reprogramado.path("topic").asText()).isEqualTo(eventos);
        assertThat(reprogramado.path("data").path("motivo").asText()).isEqualTo("Vía en obras");

        JsonNode novedad = anonymous.next("event");
        assertThat(novedad.path("event").asText()).isEqualTo("TripEventReported");
        assertThat(novedad.path("data").path("tipo").asText()).isEqualTo("ACCIDENTE");

        String recibido = anonymous.received.toString() + eta + reprogramado + novedad;
        assertThat(recibido).doesNotContain("SOS-SECRETO", "TripAlertRaised", "cond-secreto", "conductorId",
                "ops@empresa.co", "numeroDocumento", "Operador");
        anonymous.ws.close();
    }

    @Test
    void operadorYAdminTienenMasSuscripcionesQueLosDemas() throws Exception {
        Client operador = authenticated(UUID.randomUUID(), "OPERADOR", UUID.randomUUID());
        Client pasajero = authenticated(UUID.randomUUID(), "PASAJERO", null);
        for (int i = 0; i < 8; i++) {
            String topic = "viaje:" + UUID.randomUUID() + ":posicion";
            assertThat(operador.subscribeAndWait(topic).path("type").asText()).as("operador " + i).isEqualTo("subscribed");
            if (i < 5) {
                assertThat(pasajero.subscribeAndWait(topic).path("type").asText()).as("pasajero " + i)
                        .isEqualTo("subscribed");
            }
        }
        JsonNode noveno = operador.subscribeAndWait("viaje:" + UUID.randomUUID() + ":posicion");
        assertThat(noveno.path("code").asText()).isEqualTo("TOO_MANY_SUBSCRIPTIONS");
        JsonNode sexto = pasajero.subscribeAndWait("viaje:" + UUID.randomUUID() + ":posicion");
        assertThat(sexto.path("code").asText()).isEqualTo("TOO_MANY_SUBSCRIPTIONS");
        operador.ws.close();
        pasajero.ws.close();
    }
}
