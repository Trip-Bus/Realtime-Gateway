package com.tripbus.realtime.ws;

import org.junit.jupiter.api.Test;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.SessionLimitExceededException;

import java.io.IOException;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
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
}
