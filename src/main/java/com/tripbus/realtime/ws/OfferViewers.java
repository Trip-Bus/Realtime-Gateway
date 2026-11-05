package com.tripbus.realtime.ws;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * US-163: personas mirando una oferta. Cada réplica publica en Redis cuántas sesiones suyas siguen
 * {@code oferta:{id}:cupo} (un hash por oferta, {@code tripbus:ws:viewers:{offerId}}, con un campo por réplica
 * que vence si la réplica deja de renovarlo) y lee el total de todas. Cada {@code viewers-interval-ms} (5 s),
 * si el total cambió, envía a sus suscriptores {@code OfferViewers {offerId, personas}} en el mismo tópico.
 * Solo cuenta: nunca dice quién mira. No viene del stream: no lleva seq ni entra al búfer de reconexión.
 */
@Component
public class OfferViewers {

    static final String PREFIX = "tripbus:ws:viewers:";
    static final String EVENT = "OfferViewers";

    private static final Logger log = LoggerFactory.getLogger(OfferViewers.class);

    /**
     * KEYS[1] = hash de la oferta; ARGV = réplica, sesiones de la réplica, ahora (ms), vence (ms), ttl de la clave.
     * Escribe (o borra, con 0) el campo de la réplica, borra los vencidos y devuelve la suma de los vigentes.
     */
    private static final RedisScript<Long> REPORT = RedisScript.of("""
            if tonumber(ARGV[2]) > 0 then
              redis.call('HSET', KEYS[1], ARGV[1], ARGV[2] .. ':' .. ARGV[4])
            else
              redis.call('HDEL', KEYS[1], ARGV[1])
            end
            local now = tonumber(ARGV[3])
            local total = 0
            local entries = redis.call('HGETALL', KEYS[1])
            for i = 1, #entries, 2 do
              local value = entries[i + 1]
              local sep = string.find(value, ':', 1, true)
              local expires = sep and tonumber(string.sub(value, sep + 1)) or 0
              if expires > now then
                total = total + tonumber(string.sub(value, 1, sep - 1))
              else
                redis.call('HDEL', KEYS[1], entries[i])
              end
            end
            if total > 0 then
              redis.call('PEXPIRE', KEYS[1], ARGV[5])
            end
            return total
            """, Long.class);

    private final SessionRegistry registry;
    private final StringRedisTemplate redis;
    private final ObjectMapper objectMapper;
    private final WsMetrics metrics;
    private final long intervalMs;
    private final Clock clock;
    private final String replicaId = UUID.randomUUID().toString();
    /** Último total enviado por tópico. */
    private final Map<String, Long> lastSent = new ConcurrentHashMap<>();
    /** Tópicos (y su offerId) en los que esta réplica tiene un campo en Redis, para borrarlo al quedarse sin sesiones. */
    private final Map<String, String> reported = new ConcurrentHashMap<>();

    @Autowired
    public OfferViewers(SessionRegistry registry, StringRedisTemplate redis, ObjectMapper objectMapper,
                        WsMetrics metrics, @Value("${tripbus.realtime.viewers-interval-ms:5000}") long intervalMs) {
        this(registry, redis, objectMapper, metrics, intervalMs, Clock.systemUTC());
    }

    OfferViewers(SessionRegistry registry, StringRedisTemplate redis, ObjectMapper objectMapper, WsMetrics metrics,
                 long intervalMs, Clock clock) {
        this.registry = registry;
        this.redis = redis;
        this.objectMapper = objectMapper;
        this.metrics = metrics;
        this.intervalMs = intervalMs;
        this.clock = clock;
    }

    @Scheduled(fixedDelayString = "${tripbus.realtime.viewers-interval-ms:5000}")
    public void tick() {
        Map<String, String> topics = new HashMap<>(reported);
        for (String topic : registry.topics()) {
            Topics.Parsed parsed = Topics.parse(topic);
            if (parsed != null && parsed.kind() == Topics.Kind.CUPO) {
                topics.put(topic, parsed.id().toString());
            }
        }
        topics.forEach(this::update);
    }

    private void update(String topic, String offerId) {
        Set<ClientSession> local = registry.subscribers(topic);
        long now = clock.millis();
        Long total;
        try {
            // El campo vale 3 intervalos: una réplica caída deja de contar a los ~15 s.
            total = redis.execute(REPORT, List.of(PREFIX + offerId), replicaId, String.valueOf(local.size()),
                    String.valueOf(now), String.valueOf(now + 3 * intervalMs), String.valueOf(3 * intervalMs));
        } catch (RuntimeException e) {
            log.warn("No se pudo actualizar las personas mirando la oferta ({})", e.getClass().getSimpleName());
            return;
        }
        if (local.isEmpty()) {
            reported.remove(topic);
            lastSent.remove(topic);
            return;
        }
        reported.put(topic, offerId);
        long personas = total == null ? local.size() : total;
        Long previous = lastSent.put(topic, personas);
        if (previous != null && previous == personas) {
            return;
        }
        ObjectNode out = objectMapper.createObjectNode();
        out.put("type", "event");
        out.put("topic", topic);
        out.put("event", EVENT);
        out.putObject("data").put("offerId", offerId).put("personas", personas);
        String json = out.toString();
        for (ClientSession session : local) {
            if (session.send(json)) {
                metrics.sent(Topics.Kind.CUPO);
            }
        }
    }
}
