package com.tripbus.realtime.ws;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tripbus.realtime.config.RealtimeLimits;
import com.tripbus.realtime.config.RealtimeProperties;
import com.tripbus.realtime.config.TraceContexts;
import com.tripbus.realtime.events.EventFanout;
import com.tripbus.realtime.events.ReplayBuffer;
import com.tripbus.realtime.snapshot.SnapshotService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.http.HttpHeaders;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** HT-11 en el handler: IP del cliente, límite de conexiones por IP y por usuario y límite de mensajes. */
class RealtimeHandlerLimitesTest {

    private final SessionRegistry registry = new SessionRegistry(new SimpleMeterRegistry());
    private final ConnectionPresence presence = mock(ConnectionPresence.class);
    private final JwtDecoder jwtDecoder = mock(JwtDecoder.class);
    private final SimpleMeterRegistry meters = new SimpleMeterRegistry();

    private RealtimeHandler handler(String ipHeader, int maxMessages) {
        RealtimeLimits limits = new RealtimeLimits(0, 0, maxMessages, Duration.ofSeconds(10), null, ipHeader, null);
        return new RealtimeHandler(registry, mock(SnapshotService.class), jwtDecoder, new ObjectMapper(),
                new RealtimeProperties(null, null, null, null, null, null, 0, 0, 0, null, null, null, null, false),
                mock(TokenRevocations.class), mock(EventFanout.class), limits, presence, mock(ReplayBuffer.class),
                new WsMetrics(meters, registry), TraceContexts.noop());
    }

    private static WebSocketSession raw(String id, HttpHeaders headers, InetSocketAddress remote) {
        WebSocketSession raw = mock(WebSocketSession.class);
        when(raw.getId()).thenReturn(id);
        when(raw.isOpen()).thenReturn(true);
        when(raw.getHandshakeHeaders()).thenReturn(headers);
        when(raw.getRemoteAddress()).thenReturn(remote);
        return raw;
    }

    private static HttpHeaders forwarded(String... values) {
        HttpHeaders headers = new HttpHeaders();
        for (String value : values) {
            headers.add("X-Forwarded-For", value);
        }
        return headers;
    }

    private static List<String> sent(WebSocketSession raw) throws Exception {
        var captor = ArgumentCaptor.forClass(TextMessage.class);
        verify(raw, atLeast(0)).sendMessage(captor.capture());
        return captor.getAllValues().stream().map(TextMessage::getPayload).toList();
    }

    private String ipOf(RealtimeHandler handler, WebSocketSession raw) {
        handler.afterConnectionEstablished(raw);
        return registry.get(raw.getId()).ip();
    }

    @Test
    void laIpEsElUltimoValorDelHeaderDelProxySiEsUnaIpOLaDeLaConexion() {
        when(presence.acquireIp(any())).thenReturn(true);
        RealtimeHandler conHeader = handler("X-Forwarded-For", 0);
        InetSocketAddress remote = new InetSocketAddress("127.0.0.1", 5000);

        assertThat(ipOf(conHeader, raw("a", forwarded("1.1.1.1", "9.9.9.9, 10.0.0.7"), remote))).isEqualTo("10.0.0.7");
        assertThat(ipOf(conHeader, raw("b", forwarded("::1"), remote))).isEqualTo("::1");
        // Valor que no es una IP (inyección en la clave de Redis) ⇒ la de la conexión.
        assertThat(ipOf(conHeader, raw("c", forwarded("10.0.0.1, x:y\nz"), remote))).isEqualTo("127.0.0.1");
        assertThat(ipOf(conHeader, raw("d", new HttpHeaders(), remote))).isEqualTo("127.0.0.1");
        assertThat(ipOf(conHeader, raw("e", null, null))).isNull();
        // Sin header configurado se ignora el que mande el cliente.
        assertThat(ipOf(handler("", 0), raw("f", forwarded("10.0.0.9"), remote))).isEqualTo("127.0.0.1");
    }

    @Test
    void ipConElMaximoDeConexionesCierraCon4429() throws Exception {
        when(presence.acquireIp(any())).thenReturn(false);
        WebSocketSession raw = raw("llena", null, new InetSocketAddress("127.0.0.1", 5000));

        handler("", 0).afterConnectionEstablished(raw);

        assertThat(sent(raw)).singleElement().satisfies(m -> assertThat(m).contains("TOO_MANY_CONNECTIONS"));
        verify(raw).close(RealtimeHandler.TOO_MANY_CONNECTIONS);
        assertThat(registry.get("llena")).isNull();
        verify(presence).release(any());
    }

    @Test
    void usuarioConElMaximoDeConexionesCierraCon4429SinMandarConnected() throws Exception {
        when(presence.acquireIp(any())).thenReturn(true);
        when(presence.acquireUser(any(), anyString())).thenReturn(false);
        String userId = UUID.randomUUID().toString();
        when(jwtDecoder.decode("t")).thenReturn(Jwt.withTokenValue("t").header("alg", "RS256").subject(userId)
                .claim("role", "PASAJERO").issuedAt(Instant.now()).expiresAt(Instant.now().plusSeconds(60)).build());
        RealtimeHandler handler = handler("", 0);
        WebSocketSession raw = raw("quinta", null, null);

        handler.afterConnectionEstablished(raw);
        handler.handleMessage(raw, new TextMessage("{\"type\":\"connect\",\"token\":\"t\"}"));

        assertThat(sent(raw)).singleElement().satisfies(m -> assertThat(m).contains("TOO_MANY_CONNECTIONS"));
        verify(raw).close(RealtimeHandler.TOO_MANY_CONNECTIONS);
        handler.afterConnectionClosed(raw, RealtimeHandler.TOO_MANY_CONNECTIONS);
        assertThat(meters.counter("tripbus.ws.cierres", "codigo", "4429").count()).isEqualTo(1.0);
    }

    @Test
    void porEncimaDelLimiteDeMensajesRespondeRateLimitedYNoProcesaElMensaje() throws Exception {
        when(presence.acquireIp(any())).thenReturn(true);
        RealtimeHandler handler = handler("", 2);
        WebSocketSession raw = raw("rapida", null, null);
        handler.afterConnectionEstablished(raw);

        handler.handleMessage(raw, new TextMessage("{\"type\":\"ping\"}"));
        handler.handleMessage(raw, new TextMessage("{\"type\":\"unsubscribe\",\"topic\":\" OFERTAS:ACTIVAS \"}"));
        handler.handleMessage(raw, new TextMessage("{\"type\":\"ping\"}"));

        assertThat(sent(raw)).satisfiesExactly(
                m -> assertThat(m).isEqualTo("{\"type\":\"pong\"}"),
                m -> assertThat(m).contains("unsubscribed", "\"topic\":\"ofertas:activas\""),
                m -> assertThat(m).contains("RATE_LIMITED", "Máximo 2 mensajes cada 10 s"));
        verify(raw, never()).close(any(CloseStatus.class));
    }
}
