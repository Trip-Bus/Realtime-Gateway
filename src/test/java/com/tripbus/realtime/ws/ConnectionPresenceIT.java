package com.tripbus.realtime.ws;

import com.tripbus.realtime.RedisTestSupport;
import com.tripbus.realtime.config.RealtimeLimits;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.socket.WebSocketSession;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** HT-11 y US-185: conexiones contadas en Redis 7 real entre dos "réplicas". */
@Testcontainers
class ConnectionPresenceIT {

    @Container
    static GenericContainer<?> redis = RedisTestSupport.container();

    private static StringRedisTemplate redisA;
    private static StringRedisTemplate redisB;

    private final RealtimeLimits limits = new RealtimeLimits(2, 1, 0, null, Duration.ofSeconds(30), null, null);

    @BeforeAll
    static void connect() {
        redisA = RedisTestSupport.template(redis);
        redisB = RedisTestSupport.template(redis);
    }

    private static ClientSession session(String id, String ip) {
        WebSocketSession raw = mock(WebSocketSession.class);
        when(raw.getId()).thenReturn(id);
        when(raw.isOpen()).thenReturn(true);
        return new ClientSession(raw, Instant.now(), ip);
    }

    @Test
    void limitaLasConexionesPorIpEntreReplicasYLiberaAlCerrar() {
        ConnectionPresence a = new ConnectionPresence(redisA, limits);
        ConnectionPresence b = new ConnectionPresence(redisB, limits);
        String ip = "10.1.1." + (int) (Math.random() * 200);
        ClientSession s1 = session("1", ip);
        ClientSession s2 = session("1", ip); // mismo id de sesión, otra réplica
        ClientSession s3 = session("3", ip);

        assertThat(a.acquireIp(s1)).isTrue();
        assertThat(a.acquireIp(s1)).as("la misma conexión no cuenta dos veces").isTrue();
        assertThat(b.acquireIp(s2)).isTrue();
        assertThat(a.acquireIp(s3)).isFalse();

        a.release(s1);
        a.release(s1);
        assertThat(a.acquireIp(s3)).isTrue();
        // Sin IP conocida no se cuenta.
        assertThat(a.acquireIp(session("4", null))).isTrue();
    }

    @Test
    void limitaPorUsuarioYSabeSiTieneConexionesAbiertasEnCualquierReplica() {
        ConnectionPresence a = new ConnectionPresence(redisA, limits);
        ConnectionPresence b = new ConnectionPresence(redisB, limits);
        String user = UUID.randomUUID().toString();
        String other = UUID.randomUUID().toString();
        ClientSession s1 = session("u1", null);
        ClientSession s2 = session("u2", null);

        assertThat(b.hasOpenConnection(user)).isFalse();
        assertThat(a.acquireUser(s1, user)).isTrue();
        assertThat(a.acquireUser(s1, user)).as("reautenticar con el mismo usuario").isTrue();
        assertThat(b.hasOpenConnection(user)).isTrue();
        assertThat(b.acquireUser(s2, user)).isFalse();

        // Reautenticar la sesión con otro usuario libera al primero.
        assertThat(a.acquireUser(s1, other)).isTrue();
        assertThat(b.hasOpenConnection(user)).isFalse();
        assertThat(b.acquireUser(s2, user)).isTrue();
        b.release(s2);
        assertThat(a.hasOpenConnection(user)).isFalse();
    }

    @Test
    void unaReplicaQueMuereDejaDeContarAlVencerLaMarcaYElHeartbeatLaRenueva() {
        Instant t0 = Instant.now();
        String user = UUID.randomUUID().toString();
        String ip = "10.2.2." + (int) (Math.random() * 200);
        Instant[] vivaNow = {t0};
        Clock vivaClock = new Clock() {
            @Override
            public java.time.ZoneId getZone() {
                return ZoneOffset.UTC;
            }

            @Override
            public Clock withZone(java.time.ZoneId zone) {
                return this;
            }

            @Override
            public Instant instant() {
                return vivaNow[0];
            }
        };
        ConnectionPresence muerta = new ConnectionPresence(redisA, limits, Clock.fixed(t0, ZoneOffset.UTC));
        ConnectionPresence viva = new ConnectionPresence(redisB, limits, vivaClock);
        ClientSession huerfana = session("h", ip);
        ClientSession renovada = session("r", ip);
        assertThat(muerta.acquireIp(huerfana)).isTrue();
        assertThat(muerta.acquireUser(huerfana, user)).isTrue();
        assertThat(viva.acquireIp(renovada)).isTrue();

        // 25 s después la réplica viva renueva su conexión (heartbeat); la muerta ya no lo hace.
        vivaNow[0] = t0.plusSeconds(25);
        viva.refresh(List.of(renovada));

        // A los 31 s la marca de la muerta (30 s) venció y la renovada sigue contando.
        ConnectionPresence despues = new ConnectionPresence(redisB, limits,
                Clock.fixed(t0.plusSeconds(31), ZoneOffset.UTC));
        assertThat(despues.hasOpenConnection(user)).isFalse();
        assertThat(despues.acquireIp(session("n", ip))).isTrue();
        assertThat(despues.acquireIp(session("m", ip))).as("renovada + n = límite 2").isFalse();
    }

    @Test
    void conRedisCaidoDejaConectarPeroNoMandaPush() {
        StringRedisTemplate broken = mock(StringRedisTemplate.class);
        when(broken.execute(any(org.springframework.data.redis.core.script.RedisScript.class), any(List.class),
                any(Object[].class))).thenThrow(new IllegalStateException("caído"));
        when(broken.opsForZSet()).thenThrow(new IllegalStateException("caído"));
        when(broken.executePipelined(any(org.springframework.data.redis.core.RedisCallback.class)))
                .thenThrow(new IllegalStateException("caído"));
        ConnectionPresence presence = new ConnectionPresence(broken, limits);
        ClientSession session = session("x", "10.9.9.9");

        assertThat(presence.acquireIp(session)).isTrue();
        assertThat(presence.acquireUser(session, "u")).isTrue();
        presence.refresh(List.of(session));
        presence.release(session);
        assertThat(presence.hasOpenConnection("u")).isTrue();
        // Nada que renovar: ni siquiera llama a Redis.
        presence.refresh(List.of(session("y", null)));
    }

    @Test
    @SuppressWarnings("unchecked")
    void respuestasVaciasDeRedisCuentanComoPermitidoYSinConexiones() {
        StringRedisTemplate empty = mock(StringRedisTemplate.class);
        org.springframework.data.redis.core.ZSetOperations<String, String> zsets =
                mock(org.springframework.data.redis.core.ZSetOperations.class);
        when(empty.opsForZSet()).thenReturn(zsets);
        when(zsets.count(org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.anyDouble(),
                org.mockito.ArgumentMatchers.anyDouble())).thenReturn(null);
        ConnectionPresence presence = new ConnectionPresence(empty, limits);

        assertThat(presence.acquireIp(session("z", "10.3.3.3"))).isTrue();
        assertThat(presence.hasOpenConnection("u")).isFalse();
    }
}
