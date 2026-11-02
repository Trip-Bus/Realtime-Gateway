package com.tripbus.realtime.ws;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tripbus.realtime.config.RealtimeProperties;
import com.tripbus.realtime.events.EventFanout;
import com.tripbus.realtime.snapshot.SnapshotService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.security.oauth2.jwt.BadJwtException;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Mensajes del cliente que no necesitan un servidor real y cierres del heartbeat: sin respuesta (4408),
 * token vencido y token anterior a un cambio de contraseña (4401).
 */
class RealtimeHandlerTest {

    private final SessionRegistry registry = new SessionRegistry(new SimpleMeterRegistry());
    private final TokenRevocations revocations = mock(TokenRevocations.class);
    private final JwtDecoder jwtDecoder = mock(JwtDecoder.class);
    private final RealtimeHandler handler = new RealtimeHandler(registry, mock(SnapshotService.class),
            jwtDecoder, new ObjectMapper(), new RealtimeProperties(null, null, null, null,
                    Duration.ofSeconds(45), Duration.ofSeconds(10), 0, 0, 0, null, null, null, null, false),
            revocations, mock(EventFanout.class));

    private static WebSocketSession raw(String id) {
        WebSocketSession raw = mock(WebSocketSession.class);
        when(raw.getId()).thenReturn(id);
        when(raw.isOpen()).thenReturn(true);
        return raw;
    }

    private ClientSession authenticated(WebSocketSession raw, String userId, Instant issuedAt, Instant expiresAt) {
        ClientSession session = new ClientSession(raw, Instant.now());
        session.authenticate(userId, "jti-" + userId, "PASAJERO", null, issuedAt, expiresAt);
        registry.add(session);
        return session;
    }

    private static String lastMessage(WebSocketSession raw) throws IOException {
        var captor = ArgumentCaptor.forClass(TextMessage.class);
        verify(raw).sendMessage(captor.capture());
        return captor.getValue().getPayload();
    }

    /** Abre la sesión en el handler y le manda los mensajes dados; devuelve lo que recibió. */
    private List<String> exchange(WebSocketSession raw, String... messages) throws Exception {
        handler.afterConnectionEstablished(raw);
        for (String message : messages) {
            handler.handleMessage(raw, new TextMessage(message));
        }
        var captor = ArgumentCaptor.forClass(TextMessage.class);
        verify(raw, atLeast(0)).sendMessage(captor.capture());
        return captor.getAllValues().stream().map(TextMessage::getPayload).toList();
    }

    @Test
    void mensajesInvalidosRespondenErrorSinCerrarLaConexion() throws Exception {
        WebSocketSession raw = raw("cliente");

        List<String> received = exchange(raw, "no es json", "{\"type\":\"connect\"}", "{\"type\":\"bailar\"}",
                "{\"type\":\"subscribe\"}", "{\"type\":\"unsubscribe\"}", "{\"type\":\"pong\"}");

        assertThat(received).satisfiesExactly(
                m -> assertThat(m).contains("INVALID_MESSAGE"),
                m -> assertThat(m).contains("\"anonymous\":true"),
                m -> assertThat(m).contains("UNKNOWN_TYPE", "bailar"),
                m -> assertThat(m).contains("INVALID_TOPIC"),
                m -> assertThat(m).contains("\"type\":\"unsubscribed\"", "\"topic\":\"\""));
        verify(raw, never()).close(any());
    }

    @Test
    void mensajeDeUnaSesionDesconocidaSeIgnoraYElErrorDeTransporteLaQuita() throws Exception {
        WebSocketSession desconocida = raw("desconocida");
        handler.handleMessage(desconocida, new TextMessage("{\"type\":\"ping\"}"));
        verify(desconocida, never()).sendMessage(any());

        WebSocketSession raw = raw("rota");
        handler.afterConnectionEstablished(raw);
        handler.handleTransportError(raw, new IOException("reset"));
        assertThat(registry.get("rota")).isNull();
    }

    @Test
    void tokenInvalidoEnConnectCierraCon4401YUnoSinVencimientoNoLoInforma() throws Exception {
        when(jwtDecoder.decode("malo")).thenThrow(new BadJwtException("firma"));
        when(jwtDecoder.decode("sin-exp")).thenReturn(Jwt.withTokenValue("sin-exp").header("alg", "RS256")
                .subject("u9").claim("role", "PASAJERO").issuedAt(Instant.now()).build());
        WebSocketSession invalida = raw("invalida");
        WebSocketSession sinExp = raw("sin-exp");

        assertThat(exchange(invalida, "{\"type\":\"connect\",\"token\":\"malo\"}")).singleElement()
                .satisfies(m -> assertThat(m).contains("INVALID_TOKEN"));
        verify(invalida).close(RealtimeHandler.UNAUTHORIZED);
        assertThat(exchange(sinExp, "{\"type\":\"connect\",\"token\":\"sin-exp\"}")).singleElement()
                .satisfies(m -> assertThat(m).contains("\"type\":\"connected\"").doesNotContain("tokenExpiresAt"));
    }

    @Test
    void sinRespuestaAlHeartbeatSeCierraCon4408SinConsultarRedis() throws IOException {
        WebSocketSession raw = raw("callada");
        ClientSession session = authenticated(raw, "u1", Instant.now(), Instant.now().plusSeconds(600));
        session.touch(Instant.now().minusSeconds(60));

        handler.heartbeat();

        verify(raw).close(RealtimeHandler.HEARTBEAT_TIMEOUT);
        assertThat(registry.get("callada")).isNull();
        verify(revocations, never()).isPasswordChangedSince(anyString(), any());
    }

    @Test
    void tokenVencidoSeCierraCon4401() throws IOException {
        WebSocketSession raw = raw("vencida");
        authenticated(raw, "u2", Instant.now().minusSeconds(700), Instant.now().minusSeconds(1));

        handler.heartbeat();

        assertThat(lastMessage(raw)).contains("TOKEN_EXPIRED");
        verify(raw).close(RealtimeHandler.UNAUTHORIZED);
        assertThat(registry.get("vencida")).isNull();
    }

    @Test
    void tokenAnteriorAlCambioDeContrasenaSeCierraYLasDemasRecibenPing() throws IOException {
        Instant issuedAt = Instant.now().minusSeconds(30);
        WebSocketSession vieja = raw("vieja");
        WebSocketSession vigente = raw("vigente");
        WebSocketSession anonima = raw("anonima");
        authenticated(vieja, "u3", issuedAt, Instant.now().plusSeconds(600));
        authenticated(vigente, "u4", issuedAt, Instant.now().plusSeconds(600));
        ClientSession sinCuenta = new ClientSession(anonima, Instant.now());
        sinCuenta.connectAnonymously();
        registry.add(sinCuenta);
        when(revocations.isPasswordChangedSince("u3", issuedAt)).thenReturn(true);

        handler.heartbeat();

        assertThat(lastMessage(vieja)).contains("UNAUTHENTICATED");
        verify(vieja).close(RealtimeHandler.UNAUTHORIZED);
        assertThat(registry.get("vieja")).isNull();
        assertThat(lastMessage(vigente)).isEqualTo("{\"type\":\"ping\"}");
        assertThat(lastMessage(anonima)).isEqualTo("{\"type\":\"ping\"}");
        // Una consulta por sesión autenticada y heartbeat; ninguna por la anónima.
        verify(revocations, times(1)).isPasswordChangedSince("u3", issuedAt);
        verify(revocations, times(1)).isPasswordChangedSince("u4", issuedAt);
        verify(revocations, times(2)).isPasswordChangedSince(anyString(), any());
    }
}
