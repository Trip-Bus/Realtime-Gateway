package com.tripbus.realtime.events;

import com.tripbus.realtime.config.RealtimeLimits;
import com.tripbus.realtime.ws.ClientSession.ReplayEvent;
import com.tripbus.realtime.ws.Seq;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * US-184: los últimos {@code tripbus.realtime.replay.size} eventos de cada tópico, durante
 * {@code tripbus.realtime.replay.ttl}, para reenviar lo que un cliente perdió al reconectar. Vive en Redis
 * (un stream por tópico, {@code tripbus:replay:{topic}}, con el mismo id que el evento en el bus), así que lo
 * comparten todas las réplicas: el cliente puede reconectar a otra.
 *
 * <p>Cada réplica lee todos los eventos y escribe los mismos: Redis rechaza un id que no sea mayor que el
 * último del stream, y ese rechazo es justo lo que evita duplicados (el script lo ignora).
 */
@Component
public class ReplayBuffer {

    static final String PREFIX = "tripbus:replay:";
    static final String FIELD = "e";

    private static final Logger log = LoggerFactory.getLogger(ReplayBuffer.class);

    /** KEYS[1] = búfer; ARGV = tamaño, seq, json, id mínimo que se conserva, ttl en ms. */
    private static final RedisScript<Long> APPEND = RedisScript.of("""
            redis.pcall('XADD', KEYS[1], 'MAXLEN', ARGV[1], ARGV[2], 'e', ARGV[3])
            redis.pcall('XTRIM', KEYS[1], 'MINID', ARGV[4])
            redis.call('PEXPIRE', KEYS[1], ARGV[5])
            return 1
            """, Long.class);

    private final StringRedisTemplate redis;
    private final RealtimeLimits.Replay settings;
    private final Clock clock;

    @Autowired
    public ReplayBuffer(StringRedisTemplate redis, RealtimeLimits limits) {
        this(redis, limits, Clock.systemUTC());
    }

    ReplayBuffer(StringRedisTemplate redis, RealtimeLimits limits, Clock clock) {
        this.redis = redis;
        this.settings = limits.replay();
        this.clock = clock;
    }

    /** Guarda el mensaje {@code json} del evento {@code seq} en el búfer de {@code topic}. Nunca lanza. */
    public void append(String topic, String seq, String json) {
        try {
            redis.execute(APPEND, List.of(PREFIX + topic), String.valueOf(settings.size()), seq, json,
                    String.valueOf(oldestKept()), String.valueOf(settings.ttl().toMillis()));
        } catch (RuntimeException e) {
            log.warn("No se pudo guardar el evento {} para reconexión ({})", seq, e.getClass().getSimpleName());
        }
    }

    /**
     * Los eventos de {@code topic} posteriores a {@code lastSeq}, en orden, si el búfer los tiene TODOS: su
     * entrada más vieja (dentro del ttl) es {@code lastSeq} o anterior. Si no se puede asegurar (búfer vacío o
     * vencido, ya se descartaron eventos intermedios, Redis no responde) devuelve vacío y hay que mandar el
     * snapshot.
     */
    public Optional<List<ReplayEvent>> since(String topic, String lastSeq) {
        List<MapRecord<String, Object, Object>> records;
        try {
            records = redis.opsForStream().range(PREFIX + topic, Range.unbounded());
        } catch (RuntimeException e) {
            log.warn("No se pudo leer el búfer de reconexión ({})", e.getClass().getSimpleName());
            return Optional.empty();
        }
        if (records == null) {
            return Optional.empty();
        }
        long oldestKept = oldestKept();
        List<ReplayEvent> alive = new ArrayList<>();
        for (MapRecord<String, Object, Object> entry : records) {
            String seq = entry.getId().getValue();
            Object json = entry.getValue().get(FIELD);
            if (Seq.millis(seq) >= oldestKept && json != null) {
                alive.add(new ReplayEvent(seq, json.toString()));
            }
        }
        if (alive.isEmpty() || Seq.compare(alive.get(0).seq(), lastSeq) > 0) {
            return Optional.empty();
        }
        return Optional.of(alive.stream().filter(e -> Seq.compare(e.seq(), lastSeq) > 0).toList());
    }

    private long oldestKept() {
        return clock.millis() - settings.ttl().toMillis();
    }
}
