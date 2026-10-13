package com.tripbus.realtime.ws;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/**
 * US-006/US-030: tokens revocados por logout. identity guarda {@code tripbus:revoked:{jti}} en Redis
 * con TTL igual a lo que le queda al token; la firma sola no basta para rechazarlos.
 */
@Component
public class TokenRevocations {

    static final String KEY_PREFIX = "tripbus:revoked:";

    private static final Logger log = LoggerFactory.getLogger(TokenRevocations.class);

    private final StringRedisTemplate redis;

    public TokenRevocations(StringRedisTemplate redis) {
        this.redis = redis;
    }

    /**
     * true si el token fue revocado. Si Redis no responde se deja pasar con WARN: la expiración del
     * token sigue protegiendo y el gateway no deja de funcionar por una caída de Redis.
     */
    public boolean isRevoked(String jti) {
        if (jti == null || jti.isBlank()) {
            return false;
        }
        try {
            return Boolean.TRUE.equals(redis.hasKey(KEY_PREFIX + jti));
        } catch (RuntimeException e) {
            log.warn("No se pudo consultar la revocación del token ({}); se permite la conexión",
                    e.getClass().getSimpleName());
            return false;
        }
    }
}
