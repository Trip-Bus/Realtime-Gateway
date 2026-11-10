package com.tripbus.realtime.ws;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.SessionLimitExceededException;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ClientSessionTest {

    private static WebSocketSession raw(String id, boolean open) {
        WebSocketSession raw = mock(WebSocketSession.class);
        when(raw.getId()).thenReturn(id);
        when(raw.isOpen()).thenReturn(open);
        return raw;
    }

    @Test
    void clienteLentoNoPropagaLaExcepcionYSeCierraSoloEsaSesion() throws IOException {
        WebSocketSession raw = raw("lenta", true);
        doThrow(new SessionLimitExceededException("buffer lleno", CloseStatus.SESSION_NOT_RELIABLE))
                .when(raw).sendMessage(any());
        ClientSession session = new ClientSession(raw, Instant.now());

        boolean[] sent = new boolean[1];
        assertThatCode(() -> sent[0] = session.send("{\"type\":\"event\"}")).doesNotThrowAnyException();

        assertThat(sent[0]).isFalse();
        verify(raw).close(CloseStatus.SESSION_NOT_RELIABLE);
    }

    @Test
    void envioExitosoDevuelveTrue() throws IOException {
        WebSocketSession raw = raw("ok", true);
        ClientSession session = new ClientSession(raw, Instant.now());

        assertThat(session.send("{\"type\":\"ping\"}")).isTrue();
        verify(raw).sendMessage(new TextMessage("{\"type\":\"ping\"}"));
    }

    @Test
    void sesionCerradaNoEnviaNiVuelveACerrar() throws IOException {
        WebSocketSession raw = raw("cerrada", false);
        ClientSession session = new ClientSession(raw, Instant.now());

        assertThat(session.send("{}")).isFalse();
        session.close(CloseStatus.SESSION_NOT_RELIABLE);

        verify(raw, never()).sendMessage(any());
        verify(raw, never()).close(any());
    }

    @Test
    void errorDeIoTambienCierraYDevuelveFalse() throws IOException {
        WebSocketSession raw = raw("rota", true);
        doThrow(new IOException("broken pipe")).when(raw).sendMessage(any());
        doThrow(new IllegalStateException("cerrándose")).when(raw).close(any());
        ClientSession session = new ClientSession(raw, Instant.now());

        assertThat(session.send("{}")).isFalse();
        verify(raw).close(CloseStatus.SESSION_NOT_RELIABLE);
    }

    @Test
    void sesionAnonimaQuedaIdentificadaYAlAutenticarseDejaDeSerAnonima() {
        ClientSession session = new ClientSession(raw("anon", true), Instant.now());
        assertThat(session.identified()).isFalse();

        session.connectAnonymously();
        assertThat(session.anonymous()).isTrue();
        assertThat(session.authenticated()).isFalse();
        assertThat(session.identified()).isTrue();
        assertThat(session.tokenExpiresAt()).isNull();

        session.authenticate("u", "jti-1", "PASAJERO", Instant.now().plusSeconds(60));
        assertThat(session.jti()).isEqualTo("jti-1");
        assertThat(session.anonymous()).isFalse();
        assertThat(session.authenticated()).isTrue();
        assertThat(session.identified()).isTrue();
        assertThat(session.companyId()).isNull();

        session.authenticate("op", "jti-2", "OPERADOR", "empresa-1", Instant.now().plusSeconds(60));
        assertThat(session.role()).isEqualTo("OPERADOR");
        assertThat(session.companyId()).isEqualTo("empresa-1");
    }

    @Test
    void limiteDeMensajesPorVentanaFija() {
        ClientSession session = new ClientSession(raw("charlatana", true), Instant.now(), "10.0.0.1");
        Instant t0 = Instant.parse("2026-11-04T12:00:00Z");
        Duration window = Duration.ofSeconds(10);

        for (int i = 0; i < 3; i++) {
            assertThat(session.allowMessage(t0.plusSeconds(i), 3, window)).isTrue();
        }
        assertThat(session.allowMessage(t0.plusSeconds(9), 3, window)).isFalse();
        // Al cumplirse los 10 s empieza otra ventana.
        assertThat(session.allowMessage(t0.plusSeconds(10), 3, window)).isTrue();
        assertThat(session.ip()).isEqualTo("10.0.0.1");
        assertThat(new ClientSession(raw("sin-ip", true), t0).ip()).isNull();
    }

    private static List<String> sent(WebSocketSession raw) throws IOException {
        var captor = ArgumentCaptor.forClass(TextMessage.class);
        verify(raw, atLeast(0)).sendMessage(captor.capture());
        return captor.getAllValues().stream().map(TextMessage::getPayload).toList();
    }

    @Test
    void duranteElReenvioLosEventosEnVivoSeRetienenYSalenDespuesSinRepetirSeq() throws IOException {
        WebSocketSession raw = raw("reconecta", true);
        ClientSession session = new ClientSession(raw, Instant.now());
        session.beginReplay("t");

        // En vivo mientras se lee el búfer: 5-0 (también está en el búfer) y 7-0 (posterior).
        assertThat(session.sendEvent("t", "5-0", "vivo-5")).isTrue();
        assertThat(session.sendEvent("t", "7-0", "vivo-7")).isTrue();
        // Otro tópico no se retiene.
        assertThat(session.sendEvent("otro", "6-0", "otro-6")).isTrue();
        assertThat(sent(raw)).containsExactly("otro-6");

        int count = session.endReplay("t", List.of(new ClientSession.ReplayEvent("4-0", "bufer-4"),
                new ClientSession.ReplayEvent("5-0", "bufer-5")), "3-0", last -> "resumed:" + last);

        assertThat(count).isEqualTo(3);
        assertThat(sent(raw)).containsExactly("otro-6", "bufer-4", "bufer-5", "vivo-7", "resumed:7-0");
        // Ya no se retiene.
        session.sendEvent("t", "8-0", "vivo-8");
        assertThat(sent(raw)).endsWith("vivo-8");
    }

    @Test
    void sinBuferLosRetenidosSalenTodosYSinCola() throws IOException {
        WebSocketSession raw = raw("snapshot", true);
        ClientSession session = new ClientSession(raw, Instant.now());
        session.beginReplay("t");
        session.sendEvent("t", "5-0", "vivo-5");
        session.sendEvent("t", "6-0", "vivo-6");

        assertThat(session.endReplay("t", List.of(), null, null)).isEqualTo(2);
        assertThat(sent(raw)).containsExactly("vivo-5", "vivo-6");
        // Terminar un reenvío que no empezó no envía nada.
        assertThat(session.endReplay("nunca", List.of(), "1-0", null)).isZero();
    }
}
