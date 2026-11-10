package com.tripbus.realtime.ws;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tripbus.realtime.RedisTestSupport;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** US-163: personas mirando una oferta, sumadas en Redis 7 real entre dos réplicas simuladas. */
@Testcontainers
class OfferViewersIT {

    @Container
    static GenericContainer<?> redis = RedisTestSupport.container();

    private static StringRedisTemplate redisA;
    private static StringRedisTemplate redisB;
    private final ObjectMapper mapper = new ObjectMapper();

    @BeforeAll
    static void connect() {
        redisA = RedisTestSupport.template(redis);
        redisB = RedisTestSupport.template(redis);
    }

    /** Reloj que la prueba adelanta a mano. */
    private static final class TestClock extends Clock {
        private Instant now = Instant.now();

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }

    /** Una réplica: su registro de sesiones y su OfferViewers. */
    private final class Replica {
        final SessionRegistry registry = new SessionRegistry(new SimpleMeterRegistry());
        final OfferViewers viewers;

        Replica(StringRedisTemplate redis, Clock clock) {
            viewers = new OfferViewers(registry, redis, mapper, new WsMetrics(new SimpleMeterRegistry(), registry),
                    5000, clock);
        }

        WebSocketSession watch(String topic) {
            WebSocketSession raw = mock(WebSocketSession.class);
            when(raw.getId()).thenReturn(UUID.randomUUID().toString());
            when(raw.isOpen()).thenReturn(true);
            ClientSession session = new ClientSession(raw, Instant.now());
            registry.add(session);
            registry.subscribe(session, topic);
            return raw;
        }
    }

    /** Valores de "personas" recibidos, en orden. */
    private List<Long> personas(WebSocketSession raw) throws IOException {
        var captor = ArgumentCaptor.forClass(TextMessage.class);
        verify(raw, atLeast(0)).sendMessage(captor.capture());
        return captor.getAllValues().stream().map(m -> {
            try {
                var json = mapper.readTree(m.getPayload());
                assertThat(json.path("event").asText()).isEqualTo("OfferViewers");
                assertThat(json.path("data").size()).as("solo offerId y personas").isEqualTo(2);
                return json.path("data").path("personas").asLong();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }).toList();
    }

    @Test
    void sumaLasSesionesDeTodasLasReplicasYSoloAvisaCuandoCambia() throws IOException {
        UUID offer = UUID.randomUUID();
        String topic = "oferta:" + offer + ":cupo";
        TestClock clock = new TestClock();
        Replica a = new Replica(redisA, clock);
        Replica b = new Replica(redisB, clock);
        WebSocketSession enA = a.watch(topic);
        a.watch("oferta:" + UUID.randomUUID() + ":cupo");
        a.watch("ofertas:activas");
        WebSocketSession enB1 = b.watch(topic);
        WebSocketSession enB2 = b.watch(topic);

        a.viewers.tick();
        b.viewers.tick();
        a.viewers.tick();
        a.viewers.tick(); // sin cambios: no envía

        assertThat(personas(enA)).containsExactly(1L, 3L);
        assertThat(personas(enB1)).containsExactly(3L);
        assertThat(personas(enB2)).containsExactly(3L);

        // Las dos sesiones de B se van: B borra su campo y A ve 1.
        b.registry.remove(enB1.getId());
        b.registry.remove(enB2.getId());
        b.viewers.tick();
        a.viewers.tick();
        assertThat(personas(enA)).containsExactly(1L, 3L, 1L);
        assertThat(redisA.opsForHash().size(OfferViewers.PREFIX + offer)).isEqualTo(1);
    }

    @Test
    void unaReplicaQueDejaDeReportarDejaDeContarAlVencerSuCampo() throws IOException {
        UUID offer = UUID.randomUUID();
        String topic = "oferta:" + offer + ":cupo";
        TestClock clock = new TestClock();
        Replica a = new Replica(redisA, clock);
        Replica caida = new Replica(redisB, clock);
        WebSocketSession enA = a.watch(topic);
        caida.watch(topic);
        caida.viewers.tick();
        a.viewers.tick();

        // 16 s después (vence a los 3 intervalos de 5 s) la réplica caída ya no cuenta.
        clock.now = clock.now.plusSeconds(16);
        a.viewers.tick();

        assertThat(personas(enA)).containsExactly(2L, 1L);
        assertThat(redisA.getExpire(OfferViewers.PREFIX + offer)).isPositive();
    }

    @Test
    void conRedisCaidoNoEnviaNada() throws IOException {
        StringRedisTemplate broken = mock(StringRedisTemplate.class);
        when(broken.execute(any(org.springframework.data.redis.core.script.RedisScript.class), any(List.class),
                any(Object[].class))).thenThrow(new IllegalStateException("caído"));
        Replica replica = new Replica(broken, Clock.systemUTC());
        WebSocketSession raw = replica.watch("oferta:" + UUID.randomUUID() + ":cupo");

        replica.viewers.tick();

        assertThat(personas(raw)).isEmpty();
    }

    @Test
    void siRedisNoDevuelveTotalCuentaSoloLasLocales() throws IOException {
        StringRedisTemplate empty = mock(StringRedisTemplate.class);
        Replica replica = new Replica(empty, Clock.systemUTC());
        WebSocketSession raw = replica.watch("oferta:" + UUID.randomUUID() + ":cupo");

        replica.viewers.tick();

        assertThat(personas(raw)).containsExactly(1L);
    }
}
