package com.tripbus.realtime.ws;

import com.tripbus.realtime.config.RealtimeProperties;
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
 * US-020/US-030: handshake autenticado. La forma recomendada es abrir /ws sin token y mandar
 * {"type":"connect","token":...} apenas abra (el token no queda en logs, historial ni proxies).
 * Solo si tripbus.realtime.allow-query-token=true (IDE y QA; nunca PROD) se acepta además
 * ?token=JWT: se valida aquí y responde 401 si es inválido. Con false, ?token se ignora sin fallar
 * el handshake y el cliente debe autenticarse con connect.
 *
 * La autenticación depende solo del JWT: ningún header X-Forwarded-* interviene. El Origin lo
 * valida aparte WebSocketConfig con WS_ALLOWED_ORIGINS.
 */
@Component
public class HandshakeAuthInterceptor implements HandshakeInterceptor {

    static final String JWT_ATTRIBUTE = "tripbus.jwt";

    private final JwtDecoder jwtDecoder;
    private final RealtimeProperties properties;

    public HandshakeAuthInterceptor(JwtDecoder jwtDecoder, RealtimeProperties properties) {
        this.jwtDecoder = jwtDecoder;
        this.properties = properties;
    }

    @Override
    public boolean beforeHandshake(ServerHttpRequest request, ServerHttpResponse response, WebSocketHandler handler,
                                   Map<String, Object> attributes) {
        if (!properties.allowQueryToken()) {
            return true;
        }
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
