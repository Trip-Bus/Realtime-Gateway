package com.tripbus.realtime.push;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tripbus.realtime.config.PushProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * US-185: suscripciones Web Push en Redis. Un hash por usuario ({@code tripbus:push:subs:{userId}}, campo =
 * endpoint) con hasta {@code max-subscriptions-per-user} (5); al pasarse se borra la más vieja (el navegador
 * renueva sus suscripciones). Los operadores quedan además en {@code tripbus:push:operadores:{companyId}},
 * para avisarles las alertas críticas de su empresa.
 */
@Component
public class PushSubscriptions {

    static final String USER_PREFIX = "tripbus:push:subs:";
    static final String OPERATORS_PREFIX = "tripbus:push:operadores:";

    private static final Logger log = LoggerFactory.getLogger(PushSubscriptions.class);

    /** Lo que guarda el navegador al suscribirse (PushSubscription.toJSON()) y cuándo se registró. */
    public record Subscription(String endpoint, String p256dh, String auth, long createdAt) {
    }

    private final StringRedisTemplate redis;
    private final ObjectMapper objectMapper;
    private final int maxPerUser;

    public PushSubscriptions(StringRedisTemplate redis, ObjectMapper objectMapper, PushProperties properties) {
        this.redis = redis;
        this.objectMapper = objectMapper;
        this.maxPerUser = properties.maxSubscriptionsPerUser();
    }

    /** Guarda (o reemplaza, si ya existe el endpoint) y deja como máximo las {@code max} más nuevas. */
    public void save(String userId, Subscription subscription, String operatorCompanyId) throws JsonProcessingException {
        String key = USER_PREFIX + userId;
        redis.opsForHash().put(key, subscription.endpoint(), objectMapper.writeValueAsString(subscription));
        List<Subscription> all = of(userId);
        if (all.size() > maxPerUser) {
            all.stream().sorted(Comparator.comparingLong(Subscription::createdAt))
                    .limit((long) all.size() - maxPerUser)
                    .forEach(old -> redis.opsForHash().delete(key, old.endpoint()));
        }
        if (operatorCompanyId != null) {
            redis.opsForSet().add(OPERATORS_PREFIX + operatorCompanyId, userId);
        }
    }

    /** Borra una suscripción; true si existía. */
    public boolean remove(String userId, String endpoint) {
        Long removed = redis.opsForHash().delete(USER_PREFIX + userId, endpoint);
        return removed != null && removed > 0;
    }

    /** Borra todas las suscripciones del usuario (y lo saca de los operadores de su empresa). */
    public void removeAll(String userId, String operatorCompanyId) {
        redis.delete(USER_PREFIX + userId);
        if (operatorCompanyId != null) {
            redis.opsForSet().remove(OPERATORS_PREFIX + operatorCompanyId, userId);
        }
    }

    /** Suscripciones del usuario; las ilegibles se ignoran. */
    public List<Subscription> of(String userId) {
        List<Subscription> subscriptions = new ArrayList<>();
        for (Map.Entry<Object, Object> entry : redis.opsForHash().entries(USER_PREFIX + userId).entrySet()) {
            try {
                subscriptions.add(objectMapper.readValue(entry.getValue().toString(), Subscription.class));
            } catch (JsonProcessingException e) {
                log.warn("Suscripción push ilegible; se ignora");
            }
        }
        return subscriptions;
    }

    /** Operadores de la empresa que registraron alguna suscripción. */
    public Set<String> operatorsOf(String companyId) {
        Set<String> members = redis.opsForSet().members(OPERATORS_PREFIX + companyId);
        return members == null ? Set.of() : members;
    }
}
