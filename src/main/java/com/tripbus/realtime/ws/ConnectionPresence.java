package com.tripbus.realtime.ws;

import com.tripbus.realtime.config.RealtimeLimits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

/**
 * Conexiones abiertas en TODAS las réplicas (HT-11, US-185), en Redis: un sorted set por IP
 * ({@code tripbus:ws:conn:ip:{ip}}) y otro por usuario ({@code tripbus:ws:conn:user:{userId}}), con un miembro
 * {@code {réplica}:{sesión}} por conexión cuyo puntaje es hasta cuándo vale. El heartbeat lo renueva; si una
 * réplica muere, sus conexiones dejan de contar al pasar {@code presence-ttl}.
 *
 * <p>Si Redis no responde se deja conectar (con WARN), como con los tokens revocados: el gateway no deja de
 * funcionar por una caída de Redis.
 */
@Component
public class ConnectionPresence {

    static final String IP_PREFIX = "tripbus:ws:conn:ip:";
    static final String USER_PREFIX = "tripbus:ws:conn:user:";

    private static final Logger log = LoggerFactory.getLogger(ConnectionPresence.class);

    /**
     * KEYS[1] = set; ARGV = ahora (ms), vence (ms), límite, miembro, ttl de la clave (ms). 1 si entra (o ya estaba),
     * 0 si el set ya tiene el máximo de conexiones vigentes.
     */
    private static final RedisScript<Long> ACQUIRE = RedisScript.of("""
            redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
            if not redis.call('ZSCORE', KEYS[1], ARGV[4]) and redis.call('ZCARD', KEYS[1]) >= tonumber(ARGV[3]) then
              return 0
            end
            redis.call('ZADD', KEYS[1], ARGV[2], ARGV[4])
            redis.call('PEXPIRE', KEYS[1], ARGV[5])
            return 1
            """, Long.class);

    private final StringRedisTemplate redis;
    private final RealtimeLimits limits;
    private final Clock clock;
    private final String replicaId = UUID.randomUUID().toString();

    @Autowired
    public ConnectionPresence(StringRedisTemplate redis, RealtimeLimits limits) {
        this(redis, limits, Clock.systemUTC());
    }

    ConnectionPresence(StringRedisTemplate redis, RealtimeLimits limits, Clock clock) {
        this.redis = redis;
        this.limits = limits;
        this.clock = clock;
    }

    /** Cuenta la conexión en su IP; false si la IP ya tiene {@code max-connections-per-ip} abiertas. */
    public boolean acquireIp(ClientSession session) {
        String ip = session.ip();
        if (ip == null) {
            return true;
        }
        if (!acquire(IP_PREFIX + ip, session, limits.maxConnectionsPerIp())) {
            return false;
        }
        session.countedIp(ip);
        return true;
    }

    /** Cuenta la conexión del usuario; false si ya tiene {@code max-connections-per-user} abiertas. */
    public boolean acquireUser(ClientSession session, String userId) {
        String previous = session.countedUserId();
        if (previous != null && !previous.equals(userId)) {
            remove(USER_PREFIX + previous, session);
            session.countedUserId(null);
        }
        if (!acquire(USER_PREFIX + userId, session, limits.maxConnectionsPerUser())) {
            return false;
        }
        session.countedUserId(userId);
        return true;
    }

    /** Deja de contar la conexión (al cerrarse). Idempotente. */
    public void release(ClientSession session) {
        String ip = session.countedIp();
        String userId = session.countedUserId();
        if (ip != null) {
            remove(IP_PREFIX + ip, session);
            session.countedIp(null);
        }
        if (userId != null) {
            remove(USER_PREFIX + userId, session);
            session.countedUserId(null);
        }
    }

    /** Renueva la vigencia de las conexiones de esta réplica (lo llama el heartbeat). */
    public void refresh(Collection<ClientSession> sessions) {
        List<String[]> entries = new ArrayList<>();
        for (ClientSession session : sessions) {
            if (session.countedIp() != null) {
                entries.add(new String[]{IP_PREFIX + session.countedIp(), member(session)});
            }
            if (session.countedUserId() != null) {
                entries.add(new String[]{USER_PREFIX + session.countedUserId(), member(session)});
            }
        }
        if (entries.isEmpty()) {
            return;
        }
        long expiresAt = clock.millis() + limits.presenceTtl().toMillis();
        long keyTtl = limits.presenceTtl().toMillis();
        try {
            redis.executePipelined((RedisCallback<Object>) connection -> {
                StringRedisConnection strings = (StringRedisConnection) connection;
                for (String[] entry : entries) {
                    strings.zAdd(entry[0], expiresAt, entry[1]);
                    strings.pExpire(entry[0], keyTtl);
                }
                return null;
            });
        } catch (RuntimeException e) {
            log.warn("No se pudo renovar la presencia de las conexiones ({})", e.getClass().getSimpleName());
        }
    }

    /**
     * true si el usuario tiene al menos una conexión vigente en alguna réplica. Si Redis no responde devuelve
     * true: sin poder confirmarlo no se manda un push.
     */
    public boolean hasOpenConnection(String userId) {
        try {
            Long open = redis.opsForZSet().count(USER_PREFIX + userId, clock.millis(), Double.POSITIVE_INFINITY);
            return open != null && open > 0;
        } catch (RuntimeException e) {
            log.warn("No se pudo consultar las conexiones del usuario ({}); no se envía push",
                    e.getClass().getSimpleName());
            return true;
        }
    }

    private boolean acquire(String key, ClientSession session, int limit) {
        long now = clock.millis();
        try {
            Long ok = redis.execute(ACQUIRE, List.of(key), String.valueOf(now),
                    String.valueOf(now + limits.presenceTtl().toMillis()), String.valueOf(limit), member(session),
                    String.valueOf(limits.presenceTtl().toMillis()));
            return ok == null || ok == 1L;
        } catch (RuntimeException e) {
            log.warn("No se pudo contar la conexión en Redis ({}); se permite", e.getClass().getSimpleName());
            return true;
        }
    }

    private void remove(String key, ClientSession session) {
        try {
            redis.opsForZSet().remove(key, member(session));
        } catch (RuntimeException e) {
            log.warn("No se pudo descontar la conexión en Redis ({})", e.getClass().getSimpleName());
        }
    }

    private String member(ClientSession session) {
        return replicaId + ":" + session.id();
    }
}
