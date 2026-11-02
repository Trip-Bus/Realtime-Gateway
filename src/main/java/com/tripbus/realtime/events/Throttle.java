package com.tripbus.realtime.events;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Como máximo un envío por clave dentro de una ventana (p. ej. un OfferQuotaUpdated por oferta y
 * segundo en la vitrina). Lo que llega dentro de la ventana se descarta: el tópico detallado de la
 * oferta o del viaje sigue recibiéndolo todo. Solo en memoria y por réplica, como las suscripciones.
 */
final class Throttle {

    /** Por encima de este tamaño se olvidan las claves cuya ventana ya pasó (ofertas cerradas, viajes finalizados). */
    static final int PRUNE_THRESHOLD = 10_000;

    private final Duration window;
    private final Map<String, Instant> lastSent = new ConcurrentHashMap<>();

    Throttle(Duration window) {
        this.window = window;
    }

    /** true si el envío de {@code key} en {@code now} cabe en el límite (y lo registra). */
    boolean allow(String key, Instant now) {
        if (lastSent.size() > PRUNE_THRESHOLD) {
            lastSent.values().removeIf(sent -> !now.isBefore(sent.plus(window)));
        }
        boolean[] allowed = {false};
        lastSent.compute(key, (k, sent) -> {
            if (sent == null || !now.isBefore(sent.plus(window))) {
                allowed[0] = true;
                return now;
            }
            return sent;
        });
        return allowed[0];
    }

    int size() {
        return lastSent.size();
    }
}
