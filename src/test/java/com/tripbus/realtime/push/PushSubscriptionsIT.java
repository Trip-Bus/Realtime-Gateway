package com.tripbus.realtime.push;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tripbus.realtime.RedisTestSupport;
import com.tripbus.realtime.config.PushProperties;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** US-185: suscripciones Web Push en Redis 7 real. */
@Testcontainers
class PushSubscriptionsIT {

    @Container
    static GenericContainer<?> redis = RedisTestSupport.container();

    private static StringRedisTemplate template;
    private final PushProperties properties = new PushProperties(null, null, null, 2, false, null);

    @BeforeAll
    static void connect() {
        template = RedisTestSupport.template(redis);
    }

    private static PushSubscriptions.Subscription sub(String endpoint, long createdAt) {
        return new PushSubscriptions.Subscription(endpoint, "p", "a", createdAt);
    }

    @Test
    void guardaReemplazaRecortaYBorraLasDelUsuarioYSusOperadores() throws Exception {
        PushSubscriptions subscriptions = new PushSubscriptions(template, new ObjectMapper(), properties);
        String user = UUID.randomUUID().toString();
        String company = UUID.randomUUID().toString();

        subscriptions.save(user, sub("https://a", 1), company);
        subscriptions.save(user, sub("https://a", 5), company);
        subscriptions.save(user, sub("https://b", 3), null);
        subscriptions.save(user, sub("https://c", 4), null);

        // Máximo 2: se borra la más vieja (b, 3); "a" se reemplazó con createdAt 5.
        assertThat(subscriptions.of(user)).extracting(PushSubscriptions.Subscription::endpoint)
                .containsExactlyInAnyOrder("https://a", "https://c");
        assertThat(subscriptions.operatorsOf(company)).containsExactly(user);

        // Una entrada ilegible se ignora.
        template.opsForHash().put(PushSubscriptions.USER_PREFIX + user, "https://roto", "{no es json");
        assertThat(subscriptions.of(user)).hasSize(2);

        assertThat(subscriptions.remove(user, "https://c")).isTrue();
        assertThat(subscriptions.remove(user, "https://c")).isFalse();
        subscriptions.removeAll(user, company);
        assertThat(subscriptions.of(user)).isEmpty();
        assertThat(subscriptions.operatorsOf(company)).isEmpty();
        subscriptions.removeAll(user, null);
    }

    @Test
    @SuppressWarnings("unchecked")
    void respuestasNulasDeRedisSonVacias() {
        StringRedisTemplate empty = mock(StringRedisTemplate.class);
        SetOperations<String, String> sets = mock(SetOperations.class);
        HashOperations<String, Object, Object> hashes = mock(HashOperations.class);
        when(empty.opsForSet()).thenReturn(sets);
        when(empty.<Object, Object>opsForHash()).thenReturn(hashes);
        when(sets.members(anyString())).thenReturn(null);
        when(hashes.delete(anyString(), any())).thenReturn(null);
        PushSubscriptions subscriptions = new PushSubscriptions(empty, new ObjectMapper(), properties);

        assertThat(subscriptions.operatorsOf("e")).isEmpty();
        assertThat(subscriptions.remove("u", "https://a")).isFalse();
    }
}
