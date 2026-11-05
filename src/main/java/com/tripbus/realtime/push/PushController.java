package com.tripbus.realtime.push;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.tripbus.realtime.config.PushProperties;
import com.tripbus.realtime.ws.TokenRevocations;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtException;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.time.Clock;
import java.util.Base64;
import java.util.Locale;
import java.util.Map;

/**
 * US-185: registro de suscripciones Web Push. Autenticado con el mismo JWT del WebSocket
 * ({@code Authorization: Bearer ...}), que se rechaza también si fue revocado, la cuenta está suspendida o es
 * anterior a un cambio de contraseña.
 * <ul>
 *   <li>{@code GET /push/public-key} → {@code {"publicKey":"..."}} (pública, sin token)</li>
 *   <li>{@code POST /push/subscriptions} {@code {endpoint, keys:{p256dh, auth}}} → 201</li>
 *   <li>{@code DELETE /push/subscriptions} {@code {endpoint}} (o sin cuerpo: todas las del usuario) → 204</li>
 * </ul>
 * Con el push apagado (sin llaves VAPID) GET y POST responden 503 {@code PUSH_DISABLED}; DELETE sigue
 * funcionando para limpiar.
 */
@RestController
@RequestMapping("/push")
public class PushController {

    private static final String BEARER = "bearer ";
    private static final String OPERADOR = "OPERADOR";
    private static final int MAX_ENDPOINT_LENGTH = 2048;

    private final PushNotifier notifier;
    private final PushSubscriptions subscriptions;
    private final JwtDecoder jwtDecoder;
    private final TokenRevocations revocations;
    private final PushProperties properties;
    private final Clock clock = Clock.systemUTC();

    /** Cuerpo de POST: lo que devuelve {@code PushSubscription.toJSON()} en el navegador. */
    public record SubscriptionRequest(String endpoint, Keys keys) {
    }

    public record Keys(String p256dh, String auth) {
    }

    /** Cuerpo de DELETE. */
    public record EndpointRequest(String endpoint) {
    }

    public PushController(PushNotifier notifier, PushSubscriptions subscriptions, JwtDecoder jwtDecoder,
                          TokenRevocations revocations, PushProperties properties) {
        this.notifier = notifier;
        this.subscriptions = subscriptions;
        this.jwtDecoder = jwtDecoder;
        this.revocations = revocations;
        this.properties = properties;
    }

    @GetMapping("/public-key")
    public ResponseEntity<Map<String, String>> publicKey() {
        if (!notifier.enabled()) {
            return disabled();
        }
        return ResponseEntity.ok(Map.of("publicKey", notifier.publicKey()));
    }

    @PostMapping("/subscriptions")
    public ResponseEntity<Map<String, String>> subscribe(
            @RequestHeader(value = HttpHeaders.AUTHORIZATION, required = false) String authorization,
            @RequestBody(required = false) SubscriptionRequest body) throws JsonProcessingException {
        Jwt jwt = authenticate(authorization);
        if (jwt == null) {
            return unauthorized();
        }
        if (!notifier.enabled()) {
            return disabled();
        }
        String invalid = validate(body);
        if (invalid != null) {
            return error(HttpStatus.BAD_REQUEST, "INVALID_SUBSCRIPTION", invalid);
        }
        subscriptions.save(jwt.getSubject(), new PushSubscriptions.Subscription(body.endpoint().trim(),
                body.keys().p256dh().trim(), body.keys().auth().trim(), clock.millis()), operatorCompany(jwt));
        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of("endpoint", body.endpoint().trim()));
    }

    @DeleteMapping("/subscriptions")
    public ResponseEntity<Map<String, String>> unsubscribe(
            @RequestHeader(value = HttpHeaders.AUTHORIZATION, required = false) String authorization,
            @RequestBody(required = false) EndpointRequest body) {
        Jwt jwt = authenticate(authorization);
        if (jwt == null) {
            return unauthorized();
        }
        if (body == null || body.endpoint() == null || body.endpoint().isBlank()) {
            subscriptions.removeAll(jwt.getSubject(), operatorCompany(jwt));
        } else {
            subscriptions.remove(jwt.getSubject(), body.endpoint().trim());
        }
        return ResponseEntity.noContent().build();
    }

    /** El JWT del header si es válido y vigente; null si no. */
    private Jwt authenticate(String authorization) {
        if (authorization == null || !authorization.toLowerCase(Locale.ROOT).startsWith(BEARER)) {
            return null;
        }
        Jwt jwt;
        try {
            jwt = jwtDecoder.decode(authorization.substring(BEARER.length()).trim());
        } catch (JwtException e) {
            return null;
        }
        if (jwt.getSubject() == null || revocations.isRevoked(jwt.getId()) || revocations.isSuspended(jwt.getSubject())
                || revocations.isPasswordChangedSince(jwt.getSubject(), jwt.getIssuedAt())) {
            return null;
        }
        return jwt;
    }

    /** companyId de un OPERADOR (recibe las alertas críticas de su empresa); null en otro rol. */
    private static String operatorCompany(Jwt jwt) {
        String companyId = jwt.getClaimAsString("companyId");
        return OPERADOR.equals(jwt.getClaimAsString("role")) && companyId != null && !companyId.isBlank()
                ? companyId : null;
    }

    /** Mensaje del primer problema, o null si la suscripción es válida. */
    private String validate(SubscriptionRequest body) {
        if (body == null || body.endpoint() == null || body.keys() == null) {
            return "Se esperaba {endpoint, keys:{p256dh, auth}}";
        }
        String endpoint = body.endpoint().trim();
        URI uri;
        try {
            uri = URI.create(endpoint);
        } catch (IllegalArgumentException e) {
            return "endpoint no es una URL";
        }
        boolean secure = "https".equalsIgnoreCase(uri.getScheme());
        boolean allowedHttp = properties.allowHttpEndpoints() && "http".equalsIgnoreCase(uri.getScheme());
        if (endpoint.length() > MAX_ENDPOINT_LENGTH || uri.getHost() == null || !(secure || allowedHttp)) {
            return "endpoint debe ser una URL https del servicio de push";
        }
        // p256dh: punto P-256 sin comprimir (65 bytes, empieza en 0x04); auth: 16 bytes.
        byte[] p256dh = decode(body.keys().p256dh());
        byte[] auth = decode(body.keys().auth());
        if (p256dh.length != 65 || p256dh[0] != 0x04 || auth.length != 16) {
            return "keys.p256dh o keys.auth no son válidas";
        }
        return null;
    }

    /** Bytes del base64url; vacío si falta o no es base64url. */
    private static byte[] decode(String base64Url) {
        if (base64Url == null) {
            return new byte[0];
        }
        try {
            return Base64.getUrlDecoder().decode(base64Url.trim());
        } catch (IllegalArgumentException e) {
            return new byte[0];
        }
    }

    private static ResponseEntity<Map<String, String>> unauthorized() {
        return error(HttpStatus.UNAUTHORIZED, "UNAUTHENTICATED", "Falta un token válido (Authorization: Bearer ...)");
    }

    private static ResponseEntity<Map<String, String>> disabled() {
        return error(HttpStatus.SERVICE_UNAVAILABLE, "PUSH_DISABLED", "Las notificaciones push no están configuradas");
    }

    private static ResponseEntity<Map<String, String>> error(HttpStatus status, String code, String message) {
        return ResponseEntity.status(status).body(Map.of("code", code, "message", message));
    }
}
