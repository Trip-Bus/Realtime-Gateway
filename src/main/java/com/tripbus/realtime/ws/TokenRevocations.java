package com.tripbus.realtime.ws;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * US-006/US-030: tokens revocados por logout. identity guarda {@code tripbus:revoked:{jti}} en Redis
 * con TTL igual a lo que le queda al token; la firma sola no basta para rechazarlos. US-104: también
 * cuentas suspendidas ({@code tripbus:suspended:{sub}}). US-129: tokens emitidos antes de un cambio de
 * contraseña ({@code tripbus:pwd-changed:{sub}}).
 */
@Component
public class TokenRevocations {

    static final String KEY_PREFIX = "tripbus:revoked:";
    static final String SUSPENDED_PREFIX = "tripbus:suspended:";
    static final String PASSWORD_CHANGED_PREFIX = "tripbus:pwd-changed:";

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

    /**
     * US-129: true si el usuario cambió la contraseña después de emitido el token. identity guarda
     * {@code tripbus:pwd-changed:{sub}} = epoch en segundos del cambio; el {@code iat} del JWT también
     * va en segundos, así que el token emitido en el mismo segundo (el que identity entrega con el
     * cambio) sigue valiendo. Con la clave y sin {@code iat} no hay cómo probar que es posterior: se
     * rechaza, como en identity. Una sola consulta (GET). Redis caído o valor ilegible ⇒ WARN y se
     * permite, mismo criterio que isRevoked.
     */
    public boolean isPasswordChangedSince(String userId, Instant issuedAt) {
        if (userId == null || userId.isBlank()) {
            return false;
        }
        String changedAt;
        try {
            changedAt = redis.opsForValue().get(PASSWORD_CHANGED_PREFIX + userId);
        } catch (RuntimeException e) {
            log.warn("No se pudo consultar el cambio de contraseña ({}); se permite la conexión",
                    e.getClass().getSimpleName());
            return false;
        }
        if (changedAt == null) {
            return false;
        }
        try {
            long epochSecond = Long.parseLong(changedAt.trim());
            return issuedAt == null || issuedAt.getEpochSecond() < epochSecond;
        } catch (NumberFormatException e) {
            log.warn("Valor ilegible en la clave de cambio de contraseña; se permite la conexión");
            return false;
        }
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
