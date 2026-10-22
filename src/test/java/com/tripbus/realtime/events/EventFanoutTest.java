package com.tripbus.realtime.events;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tripbus.realtime.config.RealtimeProperties;
import com.tripbus.realtime.ws.ClientSession;
import com.tripbus.realtime.ws.SessionRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.SessionLimitExceededException;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class EventFanoutTest {

    private final UUID itinerary = UUID.randomUUID();
    private final String topic = "itinerario:" + itinerary + ":asientos";
    private final Map<ClientSession, WebSocketSession> raws = new HashMap<>();
    private SimpleMeterRegistry meters;
    private SessionRegistry registry;
    private EventFanout fanout;

    @BeforeEach
    void setUp() {
        meters = new SimpleMeterRegistry();
        registry = new SessionRegistry(meters);
        RealtimeProperties properties = new RealtimeProperties(null, null, null, null, null, null, 0, 0,
                null, null, null, null, false);
        fanout = new EventFanout(mock(RedisConnectionFactory.class), mock(StringRedisTemplate.class), registry,
                new ObjectMapper(), properties, meters);
    }

    private static WebSocketSession raw(String id) {
        WebSocketSession raw = mock(WebSocketSession.class);
        when(raw.getId()).thenReturn(id);
        when(raw.isOpen()).thenReturn(true);
        return raw;
    }

    private void register(ClientSession session) {
        registry.add(session);
        registry.subscribe(session, topic);
    }

    private MapRecord<String, String, String> seatHeld() {
        return StreamRecords.newRecord().in("tripbus:events").ofMap(Map.of(
                "type", "SeatHeld",
                "eventId", UUID.randomUUID().toString(),
                "occurredAt", Instant.now().toString(),
                "payload", "{\"itineraryId\":\"" + itinerary + "\",\"seat\":\"12\",\"userId\":\"privado\"}"));
    }

    private double delivered() {
        return meters.get("tripbus.realtime.messages.delivered").counter().count();
    }

    @Test
    void clienteLentoNoCortaLaDifusionALosDemas() throws IOException {
        WebSocketSession first = raw("primero");
        WebSocketSession slow = raw("lento");
        WebSocketSession third = raw("tercero");
        doThrow(new SessionLimitExceededException("buffer lleno", CloseStatus.SESSION_NOT_RELIABLE))
                .when(slow).sendMessage(any());
        register(new ClientSession(first, Instant.now()));
        ClientSession slowSession = new ClientSession(slow, Instant.now());
        register(slowSession);
        register(new ClientSession(third, Instant.now()));

        fanout.onEvent(seatHeld());

        verify(first).sendMessage(any(TextMessage.class));
        verify(third).sendMessage(any(TextMessage.class));
        verify(slow).close(CloseStatus.SESSION_NOT_RELIABLE);
        assertThat(delivered()).isEqualTo(2.0);
        assertThat(registry.subscribers(topic)).hasSize(2).doesNotContain(slowSession);
        assertThat(registry.get("lento")).isNull();
    }

    @Test
    void falloInesperadoDeUnSuscriptorNoImpideEnviarAlSiguiente() throws IOException {
        WebSocketSession first = raw("primero");
        WebSocketSession third = raw("tercero");
        ClientSession broken = spy(new ClientSession(raw("roto"), Instant.now()));
        doThrow(new IllegalArgumentException("inesperado")).when(broken).send(anyString());
        register(new ClientSession(first, Instant.now()));
        register(broken);
        register(new ClientSession(third, Instant.now()));

        fanout.onEvent(seatHeld());

        verify(first).sendMessage(any(TextMessage.class));
        verify(third).sendMessage(any(TextMessage.class));
        verify(broken).close(CloseStatus.SESSION_NOT_RELIABLE);
        assertThat(delivered()).isEqualTo(2.0);
        assertThat(registry.subscribers(topic)).hasSize(2).doesNotContain(broken);
    }

    @Test
    void noReenviaElUserIdDeOtrosPasajeros() throws IOException {
        WebSocketSession raw = raw("uno");
        register(new ClientSession(raw, Instant.now()));

        fanout.onEvent(seatHeld());

        var captor = ArgumentCaptor.forClass(TextMessage.class);
        verify(raw).sendMessage(captor.capture());
        assertThat(captor.getValue().getPayload()).contains("\"seat\":\"12\"").doesNotContain("privado");
        assertThat(delivered()).isEqualTo(1.0);
    }

    @Test
    void topicosPublicosNoLlevanIdsDePasajerosNiReservasANingunaProfundidad() throws IOException {
        var payload = new ObjectMapper().readTree("""
                {"itineraryId":"i","seatNumbers":["7"],"userId":"u","holdId":"h","bookingId":"b",
                 "payload":{"nota":"ok","userId":"u2","pasajeros":[{"userId":"u3","asiento":"9"}]}}""");

        var limpio = EventFanout.sanitize(payload);

        assertThat(limpio.toString()).doesNotContain("userId", "holdId", "bookingId")
                .contains("\"seatNumbers\":[\"7\"]", "\"nota\":\"ok\"", "\"asiento\":\"9\"");
        assertThat(payload.has("holdId")).as("no modifica el payload original").isTrue();
        assertThat(EventFanout.sanitize(new ObjectMapper().readTree("\"texto\"")).asText()).isEqualTo("texto");
    }

    @Test
    void laPosicionPorViajeLlegaAAmbosTopicosSinConductorId() throws IOException {
        UUID busId = UUID.randomUUID();
        WebSocketSession porBus = raw("bus");
        WebSocketSession porViaje = raw("viaje");
        ClientSession busSession = new ClientSession(porBus, Instant.now());
        ClientSession viajeSession = new ClientSession(porViaje, Instant.now());
        registry.add(busSession);
        registry.add(viajeSession);
        registry.subscribe(busSession, "bus:" + busId + ":posicion");
        registry.subscribe(viajeSession, "viaje:" + itinerary + ":posicion");

        fanout.onEvent(StreamRecords.newRecord().in("tripbus:events").ofMap(Map.of(
                "type", "PositionUpdated", "eventId", UUID.randomUUID().toString(),
                "occurredAt", Instant.now().toString(),
                "payload", "{\"busId\":\"" + busId + "\",\"itineraryId\":\"" + itinerary
                        + "\",\"lat\":4.6,\"conductorId\":\"secreto\"}")));

        var captor = ArgumentCaptor.forClass(TextMessage.class);
        verify(porViaje).sendMessage(captor.capture());
        assertThat(captor.getValue().getPayload()).contains("\"lat\":4.6", "viaje:" + itinerary + ":posicion")
                .doesNotContain("conductorId", "secreto");
        verify(porBus).sendMessage(any(TextMessage.class));
        assertThat(delivered()).isEqualTo(2.0);
    }

    @Test
    void elAvisoDeTiqueteLlevaSoloLosIdsSinQrNiUserId() throws IOException {
        UUID userId = UUID.randomUUID();
        String privado = "usuario:" + userId + ":tiquetes";
        WebSocketSession raw = raw("comprador");
        ClientSession session = new ClientSession(raw, Instant.now());
        registry.add(session);
        registry.subscribe(session, privado);

        fanout.onEvent(StreamRecords.newRecord().in("tripbus:events").ofMap(Map.of(
                "type", "TicketIssued",
                "eventId", UUID.randomUUID().toString(),
                "occurredAt", Instant.now().toString(),
                "payload", """
                        {"bookingId":"b1","ticketId":"t1","qrCode":"QR-SECRETO","itineraryId":"i1",
                         "seatNumbers":["3","4"],"userId":"%s","offerId":"o1"}""".formatted(userId))));

        var captor = ArgumentCaptor.forClass(TextMessage.class);
        verify(raw).sendMessage(captor.capture());
        var data = new ObjectMapper().readTree(captor.getValue().getPayload()).path("data");
        assertThat(data.toString()).isEqualTo(
                "{\"bookingId\":\"b1\",\"ticketId\":\"t1\",\"itineraryId\":\"i1\",\"seatNumbers\":[\"3\",\"4\"]}");
        // El uuid solo aparece en el nombre del tema, que el dueño ya conoce.
        assertThat(captor.getValue().getPayload()).doesNotContain("QR-SECRETO", "qrCode", "userId", "offerId");
    }

    @Test
    void elAvisoDeTiqueteOmiteLosCamposQueNoVienen() throws IOException {
        var data = EventFanout.data("TicketIssued", new ObjectMapper().readTree("{\"ticketId\":\"t1\",\"qrCode\":\"q\"}"));
        assertThat(data.toString()).isEqualTo("{\"ticketId\":\"t1\"}");
    }

    private ClientSession authenticated(String id, String userId, String jti) {
        WebSocketSession raw = raw(id);
        ClientSession session = new ClientSession(raw, Instant.now());
        raws.put(session, raw);
        session.authenticate(userId, jti, "PASAJERO", Instant.now().plusSeconds(60));
        register(session);
        return session;
    }

    private static MapRecord<String, String, String> record(String id, String type, String payload) {
        return StreamRecords.newRecord().in("tripbus:events").withId(id).ofMap(Map.of(
                "type", type, "eventId", UUID.randomUUID().toString(),
                "occurredAt", Instant.now().toString(), "payload", payload));
    }

    @Test
    void cadaEventoLlevaElIdDelStreamComoSeqYAvanzaLastSeq() throws IOException {
        WebSocketSession raw = raw("uno");
        register(new ClientSession(raw, Instant.now()));
        assertThat(fanout.lastSeq()).isEqualTo("0-0");

        fanout.onEvent(record("100-0", "SeatHeld", "{\"itineraryId\":\"" + itinerary + "\"}"));
        fanout.onEvent(record("100-1", "SeatReleased", "{\"itineraryId\":\"" + itinerary + "\"}"));
        // Los que no se difunden (sin tipo o sin suscriptores) también avanzan la posición.
        fanout.onEvent(StreamRecords.newRecord().in("tripbus:events").withId("101-0").ofMap(Map.of("x", "y")));

        var captor = ArgumentCaptor.forClass(TextMessage.class);
        verify(raw, times(2)).sendMessage(captor.capture());
        ObjectMapper mapper = new ObjectMapper();
        assertThat(mapper.readTree(captor.getAllValues().get(0).getPayload()).path("seq").asText()).isEqualTo("100-0");
        assertThat(mapper.readTree(captor.getAllValues().get(1).getPayload()).path("seq").asText()).isEqualTo("100-1");
        assertThat(fanout.lastSeq()).isEqualTo("101-0");
    }

    @Test
    void sessionRevokedCierraSoloLasSesionesDeEseJtiSinReenviarlo() throws IOException {
        ClientSession revoked = authenticated("revocada", "u1", "j1");
        ClientSession otherToken = authenticated("otro-token", "u1", "j2");
        WebSocketSession anonymousRaw = raw("anonima");
        ClientSession anonymous = new ClientSession(anonymousRaw, Instant.now());
        anonymous.connectAnonymously();
        register(anonymous);

        fanout.onEvent(record("200-0", "SessionRevoked", "{\"jti\":\"j1\",\"userId\":\"u1\"}"));

        var captor = ArgumentCaptor.forClass(TextMessage.class);
        WebSocketSession revokedRaw = raws.get(revoked);
        verify(revokedRaw).sendMessage(captor.capture());
        assertThat(captor.getValue().getPayload()).contains("UNAUTHENTICATED");
        verify(revokedRaw).close(argThat(s -> s.getCode() == 4401));
        assertThat(registry.get("revocada")).isNull();
        assertThat(registry.get("otro-token")).isSameAs(otherToken);
        assertThat(registry.get("anonima")).isSameAs(anonymous);
        verify(raws.get(otherToken), never()).sendMessage(any());
        verify(anonymousRaw, never()).sendMessage(any());
        assertThat(delivered()).isZero();
        assertThat(fanout.lastSeq()).isEqualTo("200-0");
    }

    @Test
    void userSuspendedCierraTodasLasSesionesDelUsuario() {
        authenticated("a", "u1", "j1");
        authenticated("b", "u1", "j2");
        ClientSession other = authenticated("c", "u2", "j3");

        fanout.onEvent(record("300-0", "UserSuspended", "{\"userId\":\"u1\"}"));

        assertThat(registry.all()).containsExactly(other);
    }

    @Test
    void revocacionSinIdentificadorNoCierraNada() {
        authenticated("a", "u1", "j1");

        fanout.onEvent(record("400-0", "SessionRevoked", "{\"userId\":\"u1\"}"));
        fanout.onEvent(record("400-1", "UserSuspended", "{\"userId\":\" \"}"));

        assertThat(registry.get("a")).isNotNull();
    }
}
