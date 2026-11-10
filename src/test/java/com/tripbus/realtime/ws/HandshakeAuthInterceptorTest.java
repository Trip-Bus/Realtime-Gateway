package com.tripbus.realtime.ws;

import com.tripbus.realtime.config.RealtimeProperties;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.ServletServerHttpRequest;
import org.springframework.http.server.ServletServerHttpResponse;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtException;
import org.springframework.web.socket.WebSocketHandler;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class HandshakeAuthInterceptorTest {

    private final JwtDecoder decoder = mock(JwtDecoder.class);
    private final MockHttpServletResponse servletResponse = new MockHttpServletResponse();
    private final Map<String, Object> attributes = new HashMap<>();

    private static RealtimeProperties properties(boolean allowQueryToken) {
        return new RealtimeProperties(null, null, null, null, null, null, 0, 0, 0, null, null, null, null, allowQueryToken);
    }

    private boolean handshake(boolean allowQueryToken, String query) {
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/ws");
        request.setQueryString(query);
        return new HandshakeAuthInterceptor(decoder, properties(allowQueryToken)).beforeHandshake(
                new ServletServerHttpRequest(request), new ServletServerHttpResponse(servletResponse),
                mock(WebSocketHandler.class), attributes);
    }

    @Test
    void conQueryTokenDeshabilitadoIgnoraElTokenSinFallarElHandshake() {
        assertThat(handshake(false, "token=valido")).isTrue();

        verify(decoder, never()).decode(anyString());
        assertThat(attributes).doesNotContainKey(HandshakeAuthInterceptor.JWT_ATTRIBUTE);
        assertThat(servletResponse.getStatus()).isEqualTo(HttpStatus.OK.value());
    }

    @Test
    void conQueryTokenDeshabilitadoNiSiquieraUnTokenInvalidoFallaElHandshake() {
        assertThat(handshake(false, "token=malo")).isTrue();
        verify(decoder, never()).decode(anyString());
    }

    @Test
    void conQueryTokenHabilitadoAutenticaConElTokenDeLaUrl() {
        Jwt jwt = Jwt.withTokenValue("valido").header("alg", "RS256").subject("u-1")
                .expiresAt(Instant.now().plusSeconds(60)).build();
        when(decoder.decode("valido")).thenReturn(jwt);

        assertThat(handshake(true, "token=valido")).isTrue();
        assertThat(attributes).containsEntry(HandshakeAuthInterceptor.JWT_ATTRIBUTE, jwt);
    }

    @Test
    void conQueryTokenHabilitadoUnTokenInvalidoResponde401() {
        when(decoder.decode("malo")).thenThrow(new JwtException("firma inválida"));

        assertThat(handshake(true, "token=malo")).isFalse();
        assertThat(servletResponse.getStatus()).isEqualTo(HttpStatus.UNAUTHORIZED.value());
    }

    @Test
    void sinTokenEnLaUrlAbreYEsperaElMensajeConnect() {
        assertThat(handshake(true, null)).isTrue();
        assertThat(handshake(true, "token=")).isTrue();
        verify(decoder, never()).decode(anyString());
    }
}
