package com.tripbus.realtime.events;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Reacciona a cada evento del bus que difunde {@link EventFanout}, después de entregarlo a las conexiones de
 * esta réplica (p. ej. el Web Push, US-185). Corre en el hilo que lee el stream: no debe bloquearlo.
 */
public interface DomainEventListener {

    /**
     * @param type    tipo del evento (BookingAffected, TripAlertRaised...)
     * @param seq     id de la entrada en el stream
     * @param eventId id del evento (puede ser null)
     * @param payload payload completo, sin filtrar: el listener decide qué usa
     */
    void onEvent(String type, String seq, String eventId, JsonNode payload);
}
