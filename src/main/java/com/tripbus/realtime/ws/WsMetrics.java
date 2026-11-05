package com.tripbus.realtime.ws;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

import java.util.EnumMap;
import java.util.Locale;
import java.util.Map;

/**
 * Métricas de las conexiones (HT-11), en /actuator/prometheus. Son de ESTA réplica: Prometheus las suma.
 * <ul>
 *   <li>{@code tripbus_ws_conexiones{autenticada}}: conexiones abiertas (anónimas y sin identificar = false)</li>
 *   <li>{@code tripbus_ws_suscripciones{tipo_topico}}: suscripciones por tipo de tópico</li>
 *   <li>{@code tripbus_ws_mensajes_enviados_total{tipo_topico}}: eventos entregados a una sesión</li>
 *   <li>{@code tripbus_ws_cierres_total{codigo}}: conexiones cerradas por código WebSocket</li>
 *   <li>{@code tripbus_ws_replay_total{resultado}}: resume con {@code completo} (se reenvió el búfer),
 *       {@code snapshot} (ya no estaba) o {@code rechazado} (sin permiso o mensaje inválido)</li>
 * </ul>
 */
@Component
public class WsMetrics {

    public static final String REPLAY_COMPLETE = "completo";
    public static final String REPLAY_SNAPSHOT = "snapshot";
    public static final String REPLAY_REJECTED = "rechazado";

    private static final String TOPIC_KIND = "tipo_topico";

    private final MeterRegistry meters;
    private final Map<Topics.Kind, Counter> sent = new EnumMap<>(Topics.Kind.class);

    public WsMetrics(MeterRegistry meters, SessionRegistry registry) {
        this.meters = meters;
        for (boolean authenticated : new boolean[]{true, false}) {
            Gauge.builder("tripbus.ws.conexiones", registry, r -> count(r, authenticated))
                    .tag("autenticada", String.valueOf(authenticated))
                    .description("Conexiones WebSocket abiertas en esta réplica")
                    .register(meters);
        }
        for (Topics.Kind kind : Topics.Kind.values()) {
            Gauge.builder("tripbus.ws.suscripciones", registry, r -> r.subscriptions(kind))
                    .tag(TOPIC_KIND, tag(kind))
                    .description("Suscripciones activas en esta réplica")
                    .register(meters);
            sent.put(kind, Counter.builder("tripbus.ws.mensajes.enviados").tag(TOPIC_KIND, tag(kind))
                    .description("Eventos entregados a las sesiones").register(meters));
        }
        // Visibles en 0 desde el arranque (los cierres por código se crean con el primero de cada código).
        for (String result : new String[]{REPLAY_COMPLETE, REPLAY_SNAPSHOT, REPLAY_REJECTED}) {
            replayCounter(result);
        }
    }

    private Counter replayCounter(String result) {
        return Counter.builder("tripbus.ws.replay").tag("resultado", result)
                .description("Reconexiones con resume por resultado").register(meters);
    }

    /** Valor de la etiqueta tipo_topico: el nombre del tipo en minúsculas (asientos, cupo, avisos...). */
    public static String tag(Topics.Kind kind) {
        return kind.name().toLowerCase(Locale.ROOT);
    }

    public void sent(Topics.Kind kind) {
        sent(kind, 1);
    }

    public void sent(Topics.Kind kind, int count) {
        sent.get(kind).increment(count);
    }

    public void closed(int code) {
        Counter.builder("tripbus.ws.cierres").tag("codigo", String.valueOf(code))
                .description("Conexiones WebSocket cerradas por código").register(meters).increment();
    }

    public void replay(String result) {
        replayCounter(result).increment();
    }

    private static double count(SessionRegistry registry, boolean authenticated) {
        return registry.all().stream().filter(s -> s.authenticated() == authenticated).count();
    }
}
