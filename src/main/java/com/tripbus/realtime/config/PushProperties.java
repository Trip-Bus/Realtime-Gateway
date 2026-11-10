package com.tripbus.realtime.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.StringUtils;

import java.time.Duration;

/**
 * Web Push (US-185). Sin las tres llaves VAPID el push queda apagado y el resto del servicio funciona igual.
 *
 * @param vapidPublicKey          llave pública VAPID (base64url, punto P-256 sin comprimir)
 * @param vapidPrivateKey         llave privada VAPID (base64url)
 * @param vapidSubject            contacto del emisor ({@code mailto:} o {@code https:})
 * @param maxSubscriptionsPerUser suscripciones guardadas por usuario; al pasarse se borra la más vieja
 * @param allowHttpEndpoints      acepta endpoints {@code http://} (solo pruebas: los navegadores dan https)
 * @param ttl                     cuánto guarda el servicio de push un aviso si el dispositivo está apagado
 */
@ConfigurationProperties(prefix = "tripbus.push")
public record PushProperties(
        String vapidPublicKey,
        String vapidPrivateKey,
        String vapidSubject,
        int maxSubscriptionsPerUser,
        boolean allowHttpEndpoints,
        Duration ttl) {

    public PushProperties {
        maxSubscriptionsPerUser = maxSubscriptionsPerUser > 0 ? maxSubscriptionsPerUser : 5;
        ttl = ttl != null ? ttl : Duration.ofHours(1);
    }

    /** true si están las tres llaves VAPID. */
    public boolean configured() {
        return StringUtils.hasText(vapidPublicKey) && StringUtils.hasText(vapidPrivateKey)
                && StringUtils.hasText(vapidSubject);
    }
}
