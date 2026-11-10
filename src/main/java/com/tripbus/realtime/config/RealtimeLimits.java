package com.tripbus.realtime.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Límites de las conexiones (HT-11) y búfer de reconexión (US-184). Mismo prefijo que
 * {@link RealtimeProperties}, en un record aparte para no alargar su constructor.
 *
 * @param maxConnectionsPerIp   conexiones abiertas por IP, contadas en Redis entre réplicas
 * @param maxConnectionsPerUser conexiones autenticadas abiertas por usuario, contadas en Redis entre réplicas
 * @param maxClientMessages     mensajes que puede mandar una sesión dentro de {@code clientMessageWindow}
 * @param clientMessageWindow   ventana del límite de mensajes
 * @param presenceTtl           vida de la marca de una conexión en Redis sin renovar (la renueva el heartbeat):
 *                              si una réplica muere, sus conexiones dejan de contar pasado este tiempo
 * @param clientIpHeader        header con la IP del cliente puesto por el proxy de confianza (se toma el
 *                              último valor); vacío = la IP de la conexión TCP
 * @param replay                búfer de los últimos eventos de cada tópico
 */
@ConfigurationProperties(prefix = "tripbus.realtime")
public record RealtimeLimits(
        int maxConnectionsPerIp,
        int maxConnectionsPerUser,
        int maxClientMessages,
        Duration clientMessageWindow,
        Duration presenceTtl,
        String clientIpHeader,
        Replay replay) {

    public RealtimeLimits {
        maxConnectionsPerIp = maxConnectionsPerIp > 0 ? maxConnectionsPerIp : 20;
        maxConnectionsPerUser = maxConnectionsPerUser > 0 ? maxConnectionsPerUser : 5;
        maxClientMessages = maxClientMessages > 0 ? maxClientMessages : 30;
        clientMessageWindow = clientMessageWindow != null ? clientMessageWindow : Duration.ofSeconds(10);
        presenceTtl = presenceTtl != null ? presenceTtl : Duration.ofSeconds(60);
        clientIpHeader = clientIpHeader != null ? clientIpHeader.trim() : "";
        replay = replay != null ? replay : new Replay(0, null);
    }

    /** Todos los valores por defecto. */
    public static RealtimeLimits defaults() {
        return new RealtimeLimits(0, 0, 0, null, null, null, null);
    }

    /**
     * @param size eventos que se guardan por tópico
     * @param ttl  cuánto tiempo se guarda cada evento
     */
    public record Replay(int size, Duration ttl) {
        public Replay {
            size = size > 0 ? size : 200;
            ttl = ttl != null ? ttl : Duration.ofMinutes(10);
        }
    }
}
