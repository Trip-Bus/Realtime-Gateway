package com.tripbus.realtime.events;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tripbus.realtime.config.RealtimeProperties;
import com.tripbus.realtime.config.TraceContexts;
import com.tripbus.realtime.ws.ClientSession;
import com.tripbus.realtime.ws.SessionRegistry;
import com.tripbus.realtime.ws.Topics;
import com.tripbus.realtime.ws.WsMetrics;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
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
import java.time.Clock;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
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
    private ReplayBuffer replay;

    @BeforeEach
    void setUp() {
        meters = new SimpleMeterRegistry();
        registry = new SessionRegistry(meters);
        replay = mock(ReplayBuffer.class);
        fanout = newFanout();
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
        var data = EventFanout.data(Topics.Kind.TIQUETES, "TicketIssued",
                new ObjectMapper().readTree("{\"ticketId\":\"t1\",\"qrCode\":\"q\"}"));
        assertThat(data.toString()).isEqualTo("{\"ticketId\":\"t1\"}");
    }

    @Test
    void elAvisoDelPasajeroLlevaSoloIdsYDatosDeLaDecision() throws IOException {
        var payload = new ObjectMapper().readTree("""
                {"userId":"u","bookingId":"b","ticketId":"t","itineraryId":"i","causa":"CANCELACION",
                 "plazoDecision":"2026-11-01T00:00:00Z","opciones":["REUBICAR",{"tipo":"X","email":"e@x.co"}],
                 "bookingIdNuevo":"b2","qrCode":"QR","email":"a@b.co","numeroDocumento":"123","nombre":"Ana",
                 "monto":1000,"paymentId":"p"}""");

        var data = EventFanout.data(Topics.Kind.AVISOS, "BookingAffected", payload);

        assertThat(data.toString()).contains("\"bookingId\":\"b\"", "\"ticketId\":\"t\"", "\"itineraryId\":\"i\"",
                        "\"causa\":\"CANCELACION\"", "\"plazoDecision\"", "\"REUBICAR\"", "\"bookingIdNuevo\":\"b2\"")
                .doesNotContain("userId", "qrCode", "QR", "email", "numeroDocumento", "nombre", "Ana", "monto",
                        "paymentId");
    }

    @Test
    void losCanalesDelConductorYDeLaEmpresaConservanConductorIdSinDatosDePasajeros() throws IOException {
        var payload = new ObjectMapper().readTree("""
                {"companyId":"c","conductorId":"d","tipo":"SOS","mensaje":"auxilio","motivo":"robo",
                 "pasajero":{"userId":"u","email":"e","nombre":"Ana"}}""");

        for (Topics.Kind kind : new Topics.Kind[] {Topics.Kind.ALERTAS, Topics.Kind.VIAJE_CONDUCTOR}) {
            var data = EventFanout.data(kind, "TripAlertRaised", payload).toString();
            assertThat(data).as(kind.name()).contains("\"conductorId\":\"d\"", "auxilio", "robo")
                    .doesNotContain("userId", "email", "Ana");
        }
    }

    @Test
    void losPublicosQuitanDatosPersonalesYElMotivoDeUnSosPeroNoElNombreDeLasParadas() throws IOException {
        var payload = new ObjectMapper().readTree("""
                {"itineraryId":"i","conductorId":"d","email":"e","numeroDocumento":"1","nombre":"Ana",
                 "qrCode":"q","mensaje":"m","motivo":"Vía en obras",
                 "paradas":[{"orden":1,"nombre":"Terminal Norte","conductorId":"d2"}],
                 "alerta":{"tipo":"SOS","motivo":"secreto"}}""");

        var data = EventFanout.sanitize(payload).toString();

        assertThat(data).contains("\"motivo\":\"Vía en obras\"", "\"nombre\":\"Terminal Norte\"", "\"tipo\":\"SOS\"")
                .doesNotContain("conductorId", "email", "numeroDocumento", "Ana", "qrCode", "mensaje", "secreto");
    }

    @Test
    void unAvisoDelPasajeroSinUserIdNoSeDifunde() throws IOException {
        WebSocketSession raw = raw("pasajero");
        ClientSession session = new ClientSession(raw, Instant.now());
        registry.add(session);
        registry.subscribe(session, "usuario:" + UUID.randomUUID() + ":avisos");

        fanout.onEvent(record("500-0", "RefundProcessed", "{\"bookingId\":\"b\",\"refundId\":\"r\"}"));

        verify(raw, never()).sendMessage(any());
        assertThat(fanout.lastSeq()).isEqualTo("500-0");
    }

    @Test
    void unIdQueNoEsUuidNoSeDifundeAunqueAlguienSigaEseNombre() throws IOException {
        WebSocketSession raw = raw("raro");
        ClientSession session = new ClientSession(raw, Instant.now());
        registry.add(session);
        // RealtimeHandler nunca lo permitiría (Topics.parse lo rechaza); aquí se fuerza para probar la difusión.
        registry.subscribe(session, "usuario:no-es-uuid:avisos");

        fanout.onEvent(record("700-0", "TicketUpdated", "{\"userId\":\"NO-ES-UUID\",\"ticketId\":\"t\"}"));

        verify(raw, never()).sendMessage(any());
        assertThat(delivered()).isZero();
    }

    @Test
    void ticketIssuedLlegaATiquetesYAvisosConSuFormatoCadaUno() throws IOException {
        UUID userId = UUID.randomUUID();
        WebSocketSession tiquetes = raw("tiquetes");
        WebSocketSession avisos = raw("avisos");
        ClientSession a = new ClientSession(tiquetes, Instant.now());
        ClientSession b = new ClientSession(avisos, Instant.now());
        registry.add(a);
        registry.add(b);
        registry.subscribe(a, "usuario:" + userId + ":tiquetes");
        registry.subscribe(b, "usuario:" + userId + ":avisos");

        fanout.onEvent(record("600-0", "TicketIssued", """
                {"bookingId":"b1","ticketId":"t1","qrCode":"QR-SECRETO","itineraryId":"i1",
                 "seatNumbers":["3"],"userId":"%s","offerId":"o1"}""".formatted(userId)));

        var captor = ArgumentCaptor.forClass(TextMessage.class);
        verify(avisos).sendMessage(captor.capture());
        assertThat(captor.getValue().getPayload()).contains("usuario:" + userId + ":avisos", "\"ticketId\":\"t1\"")
                .doesNotContain("QR-SECRETO", "offerId");
        verify(tiquetes).sendMessage(any(TextMessage.class));
        assertThat(delivered()).isEqualTo(2.0);
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

    /** Un EventFanout cuyo reloj devuelve, en orden, los instantes dados. */
    private EventFanout fanoutAt(Instant first, Instant... rest) {
        Clock clock = mock(Clock.class);
        when(clock.instant()).thenReturn(first, rest);
        EventFanout limitado = newFanout();
        limitado.useClock(clock);
        return limitado;
    }

    private EventFanout newFanout() {
        return newFanout(new DefaultListableBeanFactory().getBeanProvider(DomainEventListener.class));
    }

    private EventFanout newFanout(ObjectProvider<DomainEventListener> listeners) {
        RealtimeProperties properties = new RealtimeProperties(null, null, null, null, null, null, 0, 0, 0,
                null, null, null, null, false);
        return new EventFanout(mock(RedisConnectionFactory.class), mock(StringRedisTemplate.class), registry,
                new ObjectMapper(), properties, meters, replay, new WsMetrics(meters, registry),
                TraceContexts.noop(), listeners);
    }

    private WebSocketSession subscribed(String id, String topicName) {
        WebSocketSession raw = raw(id);
        ClientSession session = new ClientSession(raw, Instant.now());
        registry.add(session);
        registry.subscribe(session, topicName);
        return raw;
    }

    private static List<String> sent(WebSocketSession raw, int times) throws IOException {
        var captor = ArgumentCaptor.forClass(TextMessage.class);
        verify(raw, times(times)).sendMessage(captor.capture());
        return captor.getAllValues().stream().map(TextMessage::getPayload).toList();
    }

    @Test
    void laVitrinaRecibeComoMaximoUnCupoPorSegundoPorOfertaYElCupoDeLaOfertaTodos() throws IOException {
        UUID offer = UUID.randomUUID();
        UUID otra = UUID.randomUUID();
        WebSocketSession vitrina = subscribed("vitrina", "ofertas:activas");
        WebSocketSession cupo = subscribed("cupo", "oferta:" + offer + ":cupo");
        Instant t0 = Instant.parse("2026-11-02T10:00:00Z");
        EventFanout limitado = fanoutAt(t0, t0.plusMillis(400), t0.plusMillis(500), t0.plusMillis(1000));
        String quota = "{\"offerId\":\"%s\",\"cupoRestante\":%d}";

        limitado.onEvent(record("1-0", "OfferQuotaUpdated", quota.formatted(offer, 9)));
        limitado.onEvent(record("1-1", "OfferQuotaUpdated", quota.formatted(offer, 8)));
        limitado.onEvent(record("1-2", "OfferQuotaUpdated", quota.formatted(otra, 5)));
        limitado.onEvent(record("1-3", "OfferQuotaUpdated", quota.formatted(offer, 7)));
        // Los demás eventos de la oferta no tienen límite (y no consultan el reloj).
        limitado.onEvent(record("1-4", "OfferSoldOut", "{\"offerId\":\"%s\",\"origen\":\"Bogotá\"}".formatted(offer)));

        assertThat(sent(vitrina, 4)).satisfiesExactly(
                m -> assertThat(m).contains("\"cupoRestante\":9"),
                m -> assertThat(m).contains("\"cupoRestante\":5"),
                m -> assertThat(m).contains("\"cupoRestante\":7"),
                m -> assertThat(m).contains("OfferSoldOut", "Bogotá", "\"topic\":\"ofertas:activas\""));
        assertThat(sent(cupo, 4)).hasSize(4);
    }

    @Test
    void laFlotaRecibeComoMaximoUnaPosicionCada3sPorViajeYTodoLoDemas() throws IOException {
        UUID company = UUID.randomUUID();
        UUID viajeA = UUID.randomUUID();
        UUID viajeB = UUID.randomUUID();
        WebSocketSession flota = subscribed("flota", "empresa:" + company + ":flota");
        WebSocketSession publico = subscribed("publico", "viaje:" + viajeA + ":posicion");
        Instant t0 = Instant.parse("2026-11-02T10:00:00Z");
        EventFanout limitado = fanoutAt(t0, t0.plusSeconds(1), t0.plusSeconds(1), t0.plusMillis(2999),
                t0.plusSeconds(3));
        String pos = "{\"itineraryId\":\"%s\",\"companyId\":\"" + company + "\",\"lat\":%d,\"conductorId\":\"d1\","
                + "\"email\":\"e@x.co\"}";
        String otro = "{\"itineraryId\":\"%s\",\"companyId\":\"%s\",%s}";

        limitado.onEvent(record("2-0", "PositionUpdated", pos.formatted(viajeA, 1)));
        limitado.onEvent(record("2-1", "PositionUpdated", pos.formatted(viajeA, 2)));
        limitado.onEvent(record("2-2", "PositionUpdated", pos.formatted(viajeB, 3)));
        limitado.onEvent(record("2-3", "EtaUpdated", otro.formatted(viajeA, company, "\"retrasoMinutos\":4")));
        limitado.onEvent(record("2-4", "TrackingStatusChanged", otro.formatted(viajeA, company, "\"estado\":\"EN_PAUSA\"")));
        limitado.onEvent(record("2-5", "PositionUpdated", pos.formatted(viajeA, 4)));
        limitado.onEvent(record("2-6", "PositionUpdated", pos.formatted(viajeA, 5)));

        assertThat(sent(flota, 5)).satisfiesExactly(
                m -> assertThat(m).contains("\"lat\":1", "\"conductorId\":\"d1\"").doesNotContain("e@x.co"),
                m -> assertThat(m).contains("\"lat\":3"),
                m -> assertThat(m).contains("EtaUpdated"),
                m -> assertThat(m).contains("EN_PAUSA"),
                m -> assertThat(m).contains("\"lat\":5"));
        // El tópico público del viaje recibe todas sus posiciones (4), la ETA y el estado.
        assertThat(sent(publico, 6)).noneMatch(m -> m.contains("conductorId"));
    }

    @Test
    void losAvisosNuevosLlevanSusDatosSinRetencionNiOtrosTurnos() throws IOException {
        var mapper = new ObjectMapper();
        var ofrecido = EventFanout.data(Topics.Kind.AVISOS, "WaitlistSeatOffered", mapper.readTree("""
                {"userId":"u","itineraryId":"i","holdId":"h-secreto","seatNumbers":["3"],
                 "expiraEn":"2026-11-02T10:15:00Z"}""")).toString();
        assertThat(ofrecido).contains("\"itineraryId\":\"i\"", "\"seatNumbers\":[\"3\"]", "\"expiraEn\"")
                .doesNotContain("holdId", "h-secreto", "userId");

        var cerrada = EventFanout.data(Topics.Kind.AVISOS, "WaitlistClosed",
                mapper.readTree("{\"itineraryId\":\"i\",\"userIds\":[\"u1\",\"u2\"]}")).toString();
        assertThat(cerrada).isEqualTo("{\"itineraryId\":\"i\"}");

        var alerta = EventFanout.data(Topics.Kind.AVISOS, "OfferAlert", mapper.readTree("""
                {"userId":"u","offerId":"o","origen":"Bogotá","destino":"Tunja","precioOferta":30000,
                 "inicio":"a","fin":"b","email":"e@x.co"}""")).toString();
        assertThat(alerta).contains("\"offerId\":\"o\"", "Bogotá", "Tunja", "30000", "\"inicio\"", "\"fin\"")
                .doesNotContain("email", "userId");

        var noShow = EventFanout.data(Topics.Kind.AVISOS, "TicketNoShow", mapper.readTree(
                "{\"userId\":\"u\",\"bookingId\":\"b\",\"ticketId\":\"t\",\"itineraryId\":\"i\"}")).toString();
        assertThat(noShow).isEqualTo("{\"bookingId\":\"b\",\"ticketId\":\"t\",\"itineraryId\":\"i\"}");
    }

    @Test
    void waitlistClosedLlegaACadaTurnoSinLosDemasUserIds() throws IOException {
        UUID uno = UUID.randomUUID();
        UUID dos = UUID.randomUUID();
        WebSocketSession primero = subscribed("uno", "usuario:" + uno + ":avisos");
        WebSocketSession segundo = subscribed("dos", "usuario:" + dos + ":avisos");

        fanout.onEvent(record("3-0", "WaitlistClosed", "{\"itineraryId\":\"i\",\"userIds\":[\"%s\",\"%s\"]}"
                .formatted(uno, dos)));

        assertThat(sent(primero, 1).get(0)).contains("usuario:" + uno + ":avisos").doesNotContain(dos.toString());
        assertThat(sent(segundo, 1).get(0)).contains("usuario:" + dos + ":avisos").doesNotContain(uno.toString());
    }

    @Test
    void cadaMensajeSeGuardaEnElBuferDeReconexionAunqueNadieLoSigaEnEstaReplica() {
        fanout.onEvent(record("800-0", "SeatHeld", "{\"itineraryId\":\"" + itinerary + "\",\"userId\":\"x\"}"));

        verify(replay).append(argThat(topic::equals), argThat("800-0"::equals),
                argThat(json -> json.contains("\"seq\":\"800-0\"") && !json.contains("userId")));
    }

    @Test
    void losListenersRecibenCadaEventoYUnoQueFallaNoAfectaALosDemas() {
        DefaultListableBeanFactory beans = new DefaultListableBeanFactory();
        List<String> recibidos = new java.util.ArrayList<>();
        beans.registerSingleton("falla", (DomainEventListener) (type, seq, eventId, payload) -> {
            throw new IllegalStateException("roto");
        });
        beans.registerSingleton("anota", (DomainEventListener) (type, seq, eventId, payload) ->
                recibidos.add(type + "@" + seq + ":" + payload.path("userId").asText()));
        EventFanout conListeners = newFanout(beans.getBeanProvider(DomainEventListener.class));

        conListeners.onEvent(record("900-0", "BookingAffected", "{\"userId\":\"u-1\"}"));
        // Un evento sin tópicos (tipo desconocido) también llega a los listeners.
        conListeners.onEvent(record("900-1", "AlgoNuevo", "{}"));

        assertThat(recibidos).containsExactly("BookingAffected@900-0:u-1", "AlgoNuevo@900-1:");
        assertThat(conListeners.lastSeq()).isEqualTo("900-1");
    }

    @Test
    @SuppressWarnings("unchecked")
    void arrancaDesdeElUltimoIdDelStreamOCeroSiNoSePuedeLeer() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        org.springframework.data.redis.core.StreamOperations<String, Object, Object> streams =
                mock(org.springframework.data.redis.core.StreamOperations.class);
        when(redis.opsForStream()).thenReturn(streams);
        MapRecord<String, Object, Object> last = StreamRecords.newRecord().in("tripbus:events").withId("1234-5")
                .ofMap(Map.<Object, Object>of("type", "SeatHeld"));
        when(streams.reverseRange(anyString(), any(), any())).thenReturn(List.of(last))
                .thenThrow(new IllegalStateException("redis caído"));
        RealtimeProperties properties = new RealtimeProperties(null, null, null, null, null, null, 0, 0, 0,
                null, null, null, null, false);
        EventFanout lector = new EventFanout(mock(RedisConnectionFactory.class), redis, registry, new ObjectMapper(),
                properties, meters, replay, new WsMetrics(meters, registry), TraceContexts.noop(),
                new DefaultListableBeanFactory().getBeanProvider(DomainEventListener.class));

        lector.stop();
        assertThat(lector.isRunning()).isFalse();
        lector.start();
        assertThat(lector.lastSeq()).isEqualTo("1234-5");
        assertThat(lector.isRunning()).isTrue();
        lector.stop();
        lector.start();
        assertThat(lector.lastSeq()).isEqualTo("0-0");
        lector.stop();
        assertThat(lector.isRunning()).isFalse();
    }
}
