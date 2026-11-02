package com.tripbus.realtime;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import com.tripbus.realtime.snapshot.SnapshotService;
import com.tripbus.realtime.ws.Topics;
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
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.when;

/**
 * Sprint 4 contra Redis 7 real: la vitrina ofertas:activas (US-160, pública y con snapshot), el límite
 * de OfferQuotaUpdated en ella, los avisos nuevos del pasajero (US-140, US-153, US-161), WaitlistClosed
 * repartido a cada turno y la flota de la empresa (US-178) con su límite de posiciones.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("it")
@Testcontainers
class RealtimeOfertasFlotaIT {

    private static final String VITRINA = "ofertas:activas";
    private static final String EVENT = "event";

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
    void snapshots() throws Exception {
        when(snapshotService.snapshot(any())).thenReturn(Optional.of(NullNode.getInstance()));
        when(snapshotService.snapshot(argThat(t -> t != null && t.kind() == Topics.Kind.OFERTAS_ACTIVAS)))
                .thenReturn(Optional.of(mapper.readTree("{\"content\":[{\"offerId\":\"o-vitrina\"}],\"page\":0}")));
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

        JsonNode next(String type) throws InterruptedException {
            while (true) {
                JsonNode msg = received.poll(5, TimeUnit.SECONDS);
                assertThat(msg).as("esperando mensaje " + type).isNotNull();
                if (type.equals(msg.path("type").asText())) {
                    return msg;
                }
            }
        }

        /** Se suscribe y devuelve el subscribed o el error. */
        JsonNode subscribe(String topic) throws Exception {
            send("{\"type\":\"subscribe\",\"topic\":\"" + topic + "\"}");
            while (true) {
                JsonNode msg = received.poll(5, TimeUnit.SECONDS);
                assertThat(msg).as("respuesta a subscribe " + topic).isNotNull();
                String type = msg.path("type").asText();
                if ("subscribed".equals(type) || "error".equals(type)) {
                    return msg;
                }
            }
        }

        void subscribed(String topic) throws Exception {
            assertThat(subscribe(topic).path("type").asText()).as(topic).isEqualTo("subscribed");
        }

        /** El siguiente event que llega, sin esperar más de {@code millis}; null si no llega ninguno. */
        JsonNode eventWithin(long millis) throws InterruptedException {
            long deadline = System.currentTimeMillis() + millis;
            long left;
            while ((left = deadline - System.currentTimeMillis()) > 0) {
                JsonNode msg = received.poll(left, TimeUnit.MILLISECONDS);
                if (msg != null && EVENT.equals(msg.path("type").asText())) {
                    return msg;
                }
            }
            return null;
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
    void unAnonimoSigueLaVitrinaConSnapshotYRecibeLosEventosDeOfertas() throws Exception {
        Client anonimo = anonymous();
        anonimo.subscribed(VITRINA);
        JsonNode snapshot = anonimo.next("snapshot");
        assertThat(snapshot.path("topic").asText()).isEqualTo(VITRINA);
        assertThat(snapshot.path("asOfSeq").asText()).isNotBlank();
        assertThat(snapshot.path("data").path("content").get(0).path("offerId").asText()).isEqualTo("o-vitrina");

        UUID offer = UUID.randomUUID();
        String conRuta = "{\"offerId\":\"%s\",\"origen\":\"Bogotá\",\"destino\":\"Tunja\",\"precioOferta\":30000}"
                .formatted(offer);
        for (String type : new String[] {"OfferCreated", "OfferActivated", "OfferUpdated", "OfferSoldOut", "OfferClosed"}) {
            publish(type, conRuta);
        }
        for (String type : new String[] {"OfferCreated", "OfferActivated", "OfferUpdated", "OfferSoldOut", "OfferClosed"}) {
            JsonNode event = anonimo.next(EVENT);
            assertThat(event.path("event").asText()).isEqualTo(type);
            assertThat(event.path("topic").asText()).isEqualTo(VITRINA);
            assertThat(event.path("data").path("origen").asText()).isEqualTo("Bogotá");
            assertThat(event.path("data").path("destino").asText()).isEqualTo("Tunja");
        }
        anonimo.ws.close();
    }

    @Test
    void laVitrinaRecibeComoMaximoUnCupoPorSegundoPorOfertaYElCupoTodos() throws Exception {
        UUID offer = UUID.randomUUID();
        Client vitrina = anonymous();
        Client cupo = anonymous();
        vitrina.subscribed(VITRINA);
        vitrina.next("snapshot");
        cupo.subscribed("oferta:" + offer + ":cupo");

        for (int restante = 10; restante > 5; restante--) {
            publish("OfferQuotaUpdated", "{\"offerId\":\"%s\",\"cupoRestante\":%d}".formatted(offer, restante));
        }
        for (int restante = 10; restante > 5; restante--) {
            assertThat(cupo.next(EVENT).path("data").path("cupoRestante").asInt()).isEqualTo(restante);
        }
        assertThat(vitrina.next(EVENT).path("data").path("cupoRestante").asInt()).isEqualTo(10);
        assertThat(vitrina.eventWithin(300)).as("la ráfaga cabe en un segundo: solo pasa el primero").isNull();

        // Pasado el segundo vuelve a pasar uno: se publica cada 200 ms hasta que llegue.
        JsonNode siguiente = null;
        for (int restante = 0; restante < 15 && siguiente == null; restante++) {
            publish("OfferQuotaUpdated", "{\"offerId\":\"%s\",\"cupoRestante\":%d}".formatted(offer, restante));
            siguiente = vitrina.eventWithin(200);
        }
        assertThat(siguiente).isNotNull();
        assertThat(siguiente.path("data").path("cupoRestante").asInt()).isLessThan(10);
        vitrina.ws.close();
        cupo.ws.close();
    }

    @Test
    void cadaAvisoNuevoLlegaSoloASuDueno() throws Exception {
        UUID duenoId = UUID.randomUUID();
        UUID otroId = UUID.randomUUID();
        Client dueno = authenticated(duenoId, "PASAJERO", null);
        Client otro = authenticated(otroId, "PASAJERO", null);
        dueno.subscribed("usuario:" + duenoId + ":avisos");
        otro.subscribed("usuario:" + otroId + ":avisos");
        assertThat(otro.subscribe("usuario:" + duenoId + ":avisos").path("code").asText()).isEqualTo("FORBIDDEN_TOPIC");

        publish("WaitlistSeatOffered", """
                {"userId":"%s","itineraryId":"i1","holdId":"h-secreto","seatNumbers":["7"],"expiraEn":"2026-11-02T10:15:00Z",
                 "email":"ana@correo.co"}""".formatted(duenoId));
        publish("OfferAlert", """
                {"userId":"%s","offerId":"o1","origen":"Bogotá","destino":"Tunja","precioOferta":30000,"nombre":"Ana"}"""
                .formatted(duenoId));
        publish("TicketNoShow", "{\"userId\":\"%s\",\"bookingId\":\"b1\",\"ticketId\":\"t1\",\"itineraryId\":\"i1\"}"
                .formatted(duenoId));
        // Marca para el otro usuario: si le hubiera llegado algo del dueño, saldría antes.
        publish("TicketNoShow", "{\"userId\":\"%s\",\"ticketId\":\"marca\"}".formatted(otroId));

        JsonNode ofrecido = dueno.next(EVENT);
        assertThat(ofrecido.path("event").asText()).isEqualTo("WaitlistSeatOffered");
        assertThat(ofrecido.path("data").path("expiraEn").asText()).isEqualTo("2026-11-02T10:15:00Z");
        assertThat(ofrecido.path("data").path("seatNumbers").get(0).asText()).isEqualTo("7");
        JsonNode alerta = dueno.next(EVENT);
        assertThat(alerta.path("event").asText()).isEqualTo("OfferAlert");
        assertThat(alerta.path("data").path("offerId").asText()).isEqualTo("o1");
        assertThat(alerta.path("data").path("origen").asText()).isEqualTo("Bogotá");
        JsonNode noShow = dueno.next(EVENT);
        assertThat(noShow.path("event").asText()).isEqualTo("TicketNoShow");
        assertThat(noShow.path("data").path("ticketId").asText()).isEqualTo("t1");
        assertThat(ofrecido.toString() + alerta + noShow)
                .doesNotContain("h-secreto", "holdId", "ana@correo.co", "Ana", "userId");

        JsonNode marca = otro.next(EVENT);
        assertThat(marca.path("data").path("ticketId").asText()).isEqualTo("marca");
        dueno.ws.close();
        otro.ws.close();
    }

    @Test
    void waitlistClosedLlegaACadaTurnoDeLaListaYANadieMas() throws Exception {
        UUID[] turnos = {UUID.randomUUID(), UUID.randomUUID()};
        UUID fueraId = UUID.randomUUID();
        Client primero = authenticated(turnos[0], "PASAJERO", null);
        Client segundo = authenticated(turnos[1], "PASAJERO", null);
        Client fuera = authenticated(fueraId, "PASAJERO", null);
        primero.subscribed("usuario:" + turnos[0] + ":avisos");
        segundo.subscribed("usuario:" + turnos[1] + ":avisos");
        fuera.subscribed("usuario:" + fueraId + ":avisos");

        publish("WaitlistClosed", "{\"itineraryId\":\"i9\",\"userIds\":[\"%s\",\"%s\"]}".formatted(turnos[0], turnos[1]));
        publish("TicketNoShow", "{\"userId\":\"%s\",\"ticketId\":\"marca\"}".formatted(fueraId));

        for (Client turno : new Client[] {primero, segundo}) {
            JsonNode cerrada = turno.next(EVENT);
            assertThat(cerrada.path("event").asText()).isEqualTo("WaitlistClosed");
            assertThat(cerrada.path("data").toString()).isEqualTo("{\"itineraryId\":\"i9\"}");
        }
        assertThat(fuera.next(EVENT).path("data").path("ticketId").asText()).isEqualTo("marca");
        primero.ws.close();
        segundo.ws.close();
        fuera.ws.close();
    }

    @Test
    void laFlotaLaSiguenSuOperadorYElAdminYOtraEmpresaNo() throws Exception {
        UUID companyId = UUID.randomUUID();
        String flota = "empresa:" + companyId + ":flota";
        Client operador = authenticated(UUID.randomUUID(), "OPERADOR", companyId);
        Client admin = authenticated(UUID.randomUUID(), "ADMIN", null);
        operador.subscribed(flota);
        admin.subscribed(flota);
        Client otraEmpresa = authenticated(UUID.randomUUID(), "OPERADOR", UUID.randomUUID());
        assertThat(otraEmpresa.subscribe(flota).path("code").asText()).isEqualTo("FORBIDDEN_TOPIC");
        assertThat(anonymous().subscribe(flota).path("code").asText()).isEqualTo("FORBIDDEN_TOPIC");

        UUID itinerary = UUID.randomUUID();
        String base = "{\"itineraryId\":\"%s\",\"companyId\":\"%s\",".formatted(itinerary, companyId);
        publish("PositionUpdated", base + "\"busId\":\"%s\",\"lat\":4.6,\"conductorId\":\"cond-1\"}".formatted(UUID.randomUUID()));
        publish("EtaUpdated", base + "\"retrasoMinutos\":6}");
        publish("TrackingStatusChanged", base + "\"estado\":\"EN_PAUSA\"}");

        for (Client client : new Client[] {operador, admin}) {
            JsonNode posicion = client.next(EVENT);
            assertThat(posicion.path("event").asText()).isEqualTo("PositionUpdated");
            assertThat(posicion.path("topic").asText()).isEqualTo(flota);
            assertThat(posicion.path("data").path("conductorId").asText()).isEqualTo("cond-1");
            assertThat(client.next(EVENT).path("event").asText()).isEqualTo("EtaUpdated");
            assertThat(client.next(EVENT).path("data").path("estado").asText()).isEqualTo("EN_PAUSA");
            assertThat(client.received).noneMatch(msg -> "snapshot".equals(msg.path("type").asText()));
        }
        operador.ws.close();
        admin.ws.close();
        otraEmpresa.ws.close();
    }

    @Test
    void laFlotaRecibeComoMaximoUnaPosicionCada3sPorViaje() throws Exception {
        UUID companyId = UUID.randomUUID();
        UUID viajeA = UUID.randomUUID();
        UUID viajeB = UUID.randomUUID();
        Client operador = authenticated(UUID.randomUUID(), "OPERADOR", companyId);
        operador.subscribed("empresa:" + companyId + ":flota");
        Client publico = anonymous();
        publico.subscribed("viaje:" + viajeA + ":posicion");
        publico.next("snapshot");

        String pos = "{\"itineraryId\":\"%s\",\"companyId\":\"" + companyId + "\",\"lat\":%d}";
        publish("PositionUpdated", pos.formatted(viajeA, 1));
        publish("PositionUpdated", pos.formatted(viajeA, 2));
        publish("PositionUpdated", pos.formatted(viajeB, 3));
        publish("PositionUpdated", pos.formatted(viajeA, 4));
        publish("TrackingStatusChanged", "{\"itineraryId\":\"%s\",\"companyId\":\"%s\",\"estado\":\"FINALIZADO\"}"
                .formatted(viajeA, companyId));

        // El tópico público del viaje recibe todas sus posiciones; la flota, una por viaje en la ráfaga.
        for (int lat : new int[] {1, 2, 4}) {
            assertThat(publico.next(EVENT).path("data").path("lat").asInt()).isEqualTo(lat);
        }
        assertThat(operador.next(EVENT).path("data").path("lat").asInt()).isEqualTo(1);
        assertThat(operador.next(EVENT).path("data").path("lat").asInt()).isEqualTo(3);
        assertThat(operador.next(EVENT).path("data").path("estado").asText()).isEqualTo("FINALIZADO");
        operador.ws.close();
        publico.ws.close();
    }
}
