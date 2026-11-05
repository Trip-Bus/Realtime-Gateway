package com.tripbus.realtime.push;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tripbus.realtime.config.PushProperties;
import com.tripbus.realtime.events.DomainEventListener;
import com.tripbus.realtime.ws.ConnectionPresence;
import nl.martijndwars.webpush.Encoding;
import nl.martijndwars.webpush.Notification;
import nl.martijndwars.webpush.PushService;
import nl.martijndwars.webpush.Urgency;
import org.apache.http.Header;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.util.EntityUtils;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.jose4j.lang.JoseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.GeneralSecurityException;
import java.security.Security;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * US-185: notificaciones Web Push con la app cerrada. Por cada evento de {@link PushMessages} busca al
 * destinatario (pasajero, conductor u operadores de la empresa) y, solo si NO tiene ninguna conexión abierta en
 * ninguna réplica ({@link ConnectionPresence}), le envía un título y un texto corto a cada suscripción. Como
 * todas las réplicas leen todos los eventos, la primera que reclama {@code tripbus:push:enviado:{evento}:{usuario}}
 * en Redis es la única que envía. Una suscripción que responde 404 o 410 ya no existe y se borra.
 *
 * <p>Sin las llaves VAPID (o con llaves inválidas) queda apagado: no hace nada y el resto funciona igual.
 */
@Component
public class PushNotifier implements DomainEventListener {

    static final String SENT_PREFIX = "tripbus:push:enviado:";

    private static final Logger log = LoggerFactory.getLogger(PushNotifier.class);
    private static final Duration SEND_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration DEDUP_TTL = Duration.ofHours(1);

    private final PushProperties properties;
    private final PushSubscriptions subscriptions;
    private final ConnectionPresence presence;
    private final StringRedisTemplate redis;
    private final ObjectMapper objectMapper;
    private final PushService pushService;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(SEND_TIMEOUT).build();

    public PushNotifier(PushProperties properties, PushSubscriptions subscriptions, ConnectionPresence presence,
                        StringRedisTemplate redis, ObjectMapper objectMapper) {
        this.properties = properties;
        this.subscriptions = subscriptions;
        this.presence = presence;
        this.redis = redis;
        this.objectMapper = objectMapper;
        this.pushService = createService(properties);
    }

    private static PushService createService(PushProperties properties) {
        if (!properties.configured()) {
            log.info("Web Push apagado: faltan VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY o VAPID_SUBJECT");
            return null;
        }
        if (Security.getProvider(BouncyCastleProvider.PROVIDER_NAME) == null) {
            Security.addProvider(new BouncyCastleProvider());
        }
        try {
            return new PushService(properties.vapidPublicKey().trim(), properties.vapidPrivateKey().trim(),
                    properties.vapidSubject().trim());
        } catch (GeneralSecurityException | RuntimeException e) {
            log.warn("Web Push apagado: las llaves VAPID no son válidas ({})", e.getClass().getSimpleName());
            return null;
        }
    }

    public boolean enabled() {
        return pushService != null;
    }

    /** Llave pública VAPID que el navegador necesita para suscribirse (null si está apagado). */
    public String publicKey() {
        return enabled() ? properties.vapidPublicKey().trim() : null;
    }

    @Override
    public void onEvent(String type, String seq, String eventId, JsonNode payload) {
        if (!enabled()) {
            return;
        }
        PushMessages.Rule rule = PushMessages.ruleFor(type, payload);
        if (rule == null) {
            return;
        }
        String event = eventId != null && !eventId.isBlank() ? eventId : seq;
        // Fuera del hilo del stream: consultar Redis y llamar al servicio de push puede tardar.
        executor.submit(() -> notifyRecipients(type, event, rule, payload));
    }

    private void notifyRecipients(String type, String event, PushMessages.Rule rule, JsonNode payload) {
        try {
            Collection<String> recipients = rule.recipient() == PushMessages.Recipient.COMPANY_OPERATORS
                    ? subscriptions.operatorsOf(payload.path("companyId").asText(""))
                    : PushMessages.directRecipient(rule, payload);
            for (String userId : recipients) {
                if (!presence.hasOpenConnection(userId) && claim(event, userId)) {
                    sendAll(type, userId, rule.message());
                }
            }
        } catch (RuntimeException e) {
            log.warn("No se pudo enviar el push de {} ({})", type, e.getClass().getSimpleName());
        }
    }

    /** true si esta réplica es la primera en reclamar el envío de este evento a este usuario. */
    private boolean claim(String event, String userId) {
        return Boolean.TRUE.equals(redis.opsForValue().setIfAbsent(SENT_PREFIX + event + ":" + userId, "1", DEDUP_TTL));
    }

    private void sendAll(String type, String userId, PushMessages.Message message) {
        List<PushSubscriptions.Subscription> all = subscriptions.of(userId);
        if (all.isEmpty()) {
            return;
        }
        String body = objectMapper.createObjectNode().put("title", message.title()).put("body", message.body())
                .put("tipo", type).toString();
        for (PushSubscriptions.Subscription subscription : all) {
            send(userId, subscription, body);
        }
    }

    private void send(String userId, PushSubscriptions.Subscription subscription, String body) {
        String host = host(subscription.endpoint());
        try {
            Notification notification = Notification.builder()
                    .endpoint(subscription.endpoint())
                    .userPublicKey(subscription.p256dh())
                    .userAuth(subscription.auth())
                    .payload(body)
                    .ttl((int) properties.ttl().toSeconds())
                    .urgency(Urgency.HIGH)
                    .build();
            int status = post(pushService.preparePost(notification, Encoding.AES128GCM));
            if (status == 404 || status == 410) {
                subscriptions.remove(userId, subscription.endpoint());
                log.info("Suscripción push vencida en {} ({}): se borra", host, status);
            } else if (status >= 300) {
                log.warn("El servicio de push {} respondió {}", host, status);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (GeneralSecurityException | IOException | JoseException | RuntimeException e) {
            // Sin el endpoint completo ni el contenido: solo el host del servicio de push.
            log.warn("No se pudo enviar el push a {} ({})", host, e.getClass().getSimpleName());
        }
    }

    /**
     * Envía la petición que arma la librería (cifrada aes128gcm y firmada con VAPID) con el cliente HTTP del JDK,
     * que tiene plazo de conexión y de respuesta. Devuelve el código HTTP.
     */
    private int post(HttpPost prepared) throws IOException, InterruptedException {
        HttpRequest.Builder request = HttpRequest.newBuilder(prepared.getURI()).timeout(SEND_TIMEOUT);
        // TTL, Urgency, Content-Type, Content-Encoding y Authorization (VAPID); el cuerpo siempre va cifrado.
        for (Header header : prepared.getAllHeaders()) {
            request.header(header.getName(), header.getValue());
        }
        request.POST(HttpRequest.BodyPublishers.ofByteArray(EntityUtils.toByteArray(prepared.getEntity())));
        return http.send(request.build(), HttpResponse.BodyHandlers.discarding()).statusCode();
    }

    private static String host(String endpoint) {
        try {
            return URI.create(endpoint).getHost();
        } catch (IllegalArgumentException e) {
            return "?";
        }
    }
}
