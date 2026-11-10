package com.tripbus.realtime.events;

import com.tripbus.realtime.RedisTestSupport;
import com.tripbus.realtime.config.RealtimeLimits;
import com.tripbus.realtime.ws.ClientSession.ReplayEvent;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** US-184: búfer de reconexión contra Redis 7 real, compartido entre dos "réplicas". */
@Testcontainers
class ReplayBufferIT {

    @Container
    static GenericContainer<?> redis = RedisTestSupport.container();

    private static StringRedisTemplate replicaA;
    private static StringRedisTemplate replicaB;

    /** Los seq de las pruebas son "ahora" en ms: el búfer descarta lo más viejo que el ttl. */
    private final long now = System.currentTimeMillis();
    private final RealtimeLimits limits = new RealtimeLimits(0, 0, 0, null, null, null,
            new RealtimeLimits.Replay(3, Duration.ofMinutes(10)));

    @BeforeAll
    static void connect() {
        replicaA = RedisTestSupport.template(redis);
        replicaB = RedisTestSupport.template(redis);
    }

    private String seq(int n) {
        return now + "-" + n;
    }

    private static List<String> seqs(List<ReplayEvent> events) {
        return events.stream().map(ReplayEvent::seq).toList();
    }

    @Test
    void reenviaLoPosteriorAlUltimoRecibidoAunqueLoHayaEscritoOtraReplica() {
        String topic = "itinerario:" + UUID.randomUUID() + ":asientos";
        ReplayBuffer a = new ReplayBuffer(replicaA, limits);
        ReplayBuffer b = new ReplayBuffer(replicaB, limits);
        for (int i = 1; i <= 3; i++) {
            a.append(topic, seq(i), "{\"n\":" + i + "}");
            // La otra réplica escribe el mismo evento: Redis lo rechaza y no queda repetido.
            b.append(topic, seq(i), "{\"n\":" + i + "}");
        }

        Optional<List<ReplayEvent>> missed = b.since(topic, seq(1));

        assertThat(missed).isPresent();
        assertThat(seqs(missed.get())).containsExactly(seq(2), seq(3));
        assertThat(missed.get().get(1).json()).isEqualTo("{\"n\":3}");
        // Nada nuevo después del último: lista vacía (no snapshot).
        assertThat(a.since(topic, seq(3))).hasValue(List.of());
        assertThat(replicaA.getExpire(ReplayBuffer.PREFIX + topic)).isPositive();
    }

    @Test
    void siYaSeDescartaronEventosIntermediosOVencieronNoHayReenvio() {
        String topic = "oferta:" + UUID.randomUUID() + ":cupo";
        ReplayBuffer buffer = new ReplayBuffer(replicaA, limits);
        for (int i = 1; i <= 5; i++) {
            buffer.append(topic, seq(i), "{}");
        }
        // Solo quedan los 3 últimos (size = 3): faltaría el 2.
        assertThat(buffer.since(topic, seq(1))).isEmpty();
        assertThat(seqs(buffer.since(topic, seq(3)).orElseThrow())).containsExactly(seq(4), seq(5));
        // Tópico sin búfer.
        assertThat(buffer.since("oferta:" + UUID.randomUUID() + ":cupo", seq(1))).isEmpty();

        // 11 minutos después todo venció (ttl 10 min), aunque la clave siga.
        Clock later = Clock.fixed(Instant.ofEpochMilli(now).plus(Duration.ofMinutes(11)), ZoneOffset.UTC);
        assertThat(new ReplayBuffer(replicaA, limits, later).since(topic, seq(3))).isEmpty();
    }

    @Test
    void conRedisCaidoNoLanzaYPideSnapshot() {
        StringRedisTemplate broken = mock(StringRedisTemplate.class);
        when(broken.execute(any(org.springframework.data.redis.core.script.RedisScript.class), any(List.class),
                any(Object[].class))).thenThrow(new IllegalStateException("redis caído"));
        when(broken.opsForStream()).thenThrow(new IllegalStateException("redis caído"));
        ReplayBuffer buffer = new ReplayBuffer(broken, limits);

        buffer.append("t", seq(1), "{}");
        assertThat(buffer.since("t", seq(1))).isEmpty();
    }

    @Test
    @SuppressWarnings("unchecked")
    void entradasSinMensajeSeIgnoranYUnaLecturaNulaPideSnapshot() {
        String topic = "viaje:" + UUID.randomUUID() + ":eventos";
        ReplayBuffer buffer = new ReplayBuffer(replicaA, limits);
        buffer.append(topic, seq(1), "{\"n\":1}");
        // Una entrada ajena sin el campo del mensaje.
        replicaA.opsForStream().add(org.springframework.data.redis.connection.stream.StreamRecords.newRecord()
                .in(ReplayBuffer.PREFIX + topic).withId(org.springframework.data.redis.connection.stream.RecordId.of(seq(2)))
                .ofMap(java.util.Map.of("otro", "x")));
        assertThat(seqs(buffer.since(topic, seq(1)).orElseThrow())).isEmpty();

        StringRedisTemplate nulo = mock(StringRedisTemplate.class);
        org.springframework.data.redis.core.StreamOperations<String, Object, Object> streams =
                mock(org.springframework.data.redis.core.StreamOperations.class);
        when(nulo.opsForStream()).thenReturn(streams);
        when(streams.range(org.mockito.ArgumentMatchers.anyString(), any())).thenReturn(null);
        assertThat(new ReplayBuffer(nulo, limits).since(topic, seq(1))).isEmpty();
    }
}
