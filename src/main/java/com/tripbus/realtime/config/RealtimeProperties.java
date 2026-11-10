package com.tripbus.realtime.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "tripbus.realtime")
public record RealtimeProperties(
        String jwksUri,
        String eventsStream,
        String allowedOrigins,
        Duration heartbeatInterval,
        Duration heartbeatTimeout,
        Duration authTimeout,
        int maxSubscriptions,
        // Suscripciones simultáneas de una sesión anónima (seguimiento público, US-039).
        int maxAnonymousSubscriptions,
        // Suscripciones de OPERADOR y ADMIN, que siguen varios viajes y su canal de alertas. Sin valor: max-subscriptions.
        int maxStaffSubscriptions,
        String seatInventoryUrl,
        String flashOffersUrl,
        String fleetUrl,
        String publicApiUrl,
        // Acepta ?token=JWT en el handshake de /ws (IDE y QA). En PROD false: solo el mensaje connect.
        boolean allowQueryToken) {

    public RealtimeProperties {
        eventsStream = eventsStream != null ? eventsStream : "tripbus:events";
        allowedOrigins = allowedOrigins != null ? allowedOrigins : "*";
        heartbeatInterval = heartbeatInterval != null ? heartbeatInterval : Duration.ofSeconds(20);
        heartbeatTimeout = heartbeatTimeout != null ? heartbeatTimeout : Duration.ofSeconds(45);
        authTimeout = authTimeout != null ? authTimeout : Duration.ofSeconds(10);
        maxSubscriptions = maxSubscriptions > 0 ? maxSubscriptions : 20;
        maxAnonymousSubscriptions = maxAnonymousSubscriptions > 0 ? maxAnonymousSubscriptions : 5;
        maxStaffSubscriptions = maxStaffSubscriptions > 0 ? maxStaffSubscriptions : maxSubscriptions;
    }

    /** Límite de suscripciones de una sesión autenticada con este rol. */
    public int maxSubscriptionsFor(String role) {
        return "OPERADOR".equals(role) || "ADMIN".equals(role) ? maxStaffSubscriptions : maxSubscriptions;
    }
}
