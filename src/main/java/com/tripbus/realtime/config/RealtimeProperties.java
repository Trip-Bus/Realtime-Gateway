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
        String seatInventoryUrl,
        String flashOffersUrl,
        String fleetUrl,
        String publicApiUrl) {

    public RealtimeProperties {
        eventsStream = eventsStream != null ? eventsStream : "tripbus:events";
        allowedOrigins = allowedOrigins != null ? allowedOrigins : "*";
        heartbeatInterval = heartbeatInterval != null ? heartbeatInterval : Duration.ofSeconds(20);
        heartbeatTimeout = heartbeatTimeout != null ? heartbeatTimeout : Duration.ofSeconds(45);
        authTimeout = authTimeout != null ? authTimeout : Duration.ofSeconds(10);
        maxSubscriptions = maxSubscriptions > 0 ? maxSubscriptions : 20;
    }
}
