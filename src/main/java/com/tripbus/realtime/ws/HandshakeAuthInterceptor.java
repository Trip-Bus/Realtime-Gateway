package com.tripbus.realtime.ws;

import org.springframework.http.HttpStatus;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtException;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.server.HandshakeInterceptor;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.Map;

/**
 * US-020: handshake autenticado. Si el cliente envía ?token=JWT se valida aquí (401 si es
 * inválido). Si no lo envía, debe mandar el mensaje {"type":"connect","token":...} apenas abra.
 */
@Component
public class HandshakeAuthInterceptor implements HandshakeInterceptor {

    static final String JWT_ATTRIBUTE = "tripbus.jwt";

    private final JwtDecoder jwtDecoder;

    public HandshakeAuthInterceptor(JwtDecoder jwtDecoder) {
        this.jwtDecoder = jwtDecoder;
    }

    @Override
    public boolean beforeHandshake(ServerHttpRequest request, ServerHttpResponse response, WebSocketHandler handler,
                                   Map<String, Object> attributes) {
        String token = UriComponentsBuilder.fromUri(request.getURI()).build().getQueryParams().getFirst("token");
        if (token == null || token.isBlank()) {
            return true;
        }
        try {
            Jwt jwt = jwtDecoder.decode(token);
            attributes.put(JWT_ATTRIBUTE, jwt);
            return true;
        } catch (JwtException e) {
            response.setStatusCode(HttpStatus.UNAUTHORIZED);
            return false;
        }
    }

    @Override
    public void afterHandshake(ServerHttpRequest request, ServerHttpResponse response, WebSocketHandler handler,
                               Exception exception) {
        // nada
    }
}
