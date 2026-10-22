package com.tripbus.realtime.ws;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/**
 * US-006/US-030: tokens revocados por logout. identity guarda {@code tripbus:revoked:{jti}} en Redis
 * con TTL igual a lo que le queda al token; la firma sola no basta para rechazarlos. US-104: también
 * cuentas suspendidas ({@code tripbus:suspended:{sub}}).
 */
@Component
public class TokenRevocations {

    static final String KEY_PREFIX = "tripbus:revoked:";
    static final String SUSPENDED_PREFIX = "tripbus:suspended:";

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
        return exists(KEY_PREFIX, jti, "la revocación del token");
    }

    /**
     * US-104: true si la cuenta está suspendida. identity guarda {@code tripbus:suspended:{sub}}; un token
     * emitido antes de la suspensión sigue con firma válida. Redis caído ⇒ mismo criterio que isRevoked.
     */
    public boolean isSuspended(String userId) {
        return exists(SUSPENDED_PREFIX, userId, "la suspensión de la cuenta");
    }

    private boolean exists(String prefix, String id, String what) {
        if (id == null || id.isBlank()) {
            return false;
        }
        try {
            return Boolean.TRUE.equals(redis.hasKey(prefix + id));
        } catch (RuntimeException e) {
            log.warn("No se pudo consultar {} ({}); se permite la conexión", what, e.getClass().getSimpleName());
            return false;
        }
    }
}
