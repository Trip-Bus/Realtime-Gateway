package com.tripbus.realtime.push;

import com.tripbus.realtime.config.PushProperties;
import com.tripbus.realtime.ws.TokenRevocations;
import org.junit.jupiter.api.Test;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** US-185: validación de la suscripción y de la autenticación de /push/subscriptions. */
class PushControllerTest {

    private final PushNotifier notifier = mock(PushNotifier.class);
    private final PushSubscriptions subscriptions = mock(PushSubscriptions.class);
    private final JwtDecoder jwtDecoder = mock(JwtDecoder.class);
    private final TokenRevocations revocations = mock(TokenRevocations.class);
    private final PushController controller = new PushController(notifier, subscriptions, jwtDecoder, revocations,
            new PushProperties(null, null, null, 0, false, null));
    private final String p256dh = FakePushServer.newKeys().publicKey();
    private final String auth = FakePushServer.newAuth();

    private String token(String subject, String role, String companyId) {
        String token = "t-" + UUID.randomUUID();
        Jwt.Builder jwt = Jwt.withTokenValue(token).header("alg", "RS256").claim("role", role).jti("j-" + token)
                .issuedAt(Instant.now()).expiresAt(Instant.now().plusSeconds(60));
        if (subject != null) {
            jwt.subject(subject);
        }
        if (companyId != null) {
            jwt.claim("companyId", companyId);
        }
        when(jwtDecoder.decode(token)).thenReturn(jwt.build());
        return "Bearer " + token;
    }

    private int post(String authorization, PushController.SubscriptionRequest body) throws Exception {
        return controller.subscribe(authorization, body).getStatusCode().value();
    }

    private PushController.SubscriptionRequest request(String endpoint, String key, String secret) {
        return new PushController.SubscriptionRequest(endpoint, new PushController.Keys(key, secret));
    }

    @Test
    void validaElEndpointYLasLlavesDelNavegador() throws Exception {
        when(notifier.enabled()).thenReturn(true);
        String bearer = token("u", "PASAJERO", null);

        assertThat(post(bearer, null)).isEqualTo(400);
        assertThat(post(bearer, new PushController.SubscriptionRequest("https://push.example/a", null))).isEqualTo(400);
        assertThat(post(bearer, request("https://push.example/a b", p256dh, auth))).isEqualTo(400);
        assertThat(post(bearer, request("http://push.example/a", p256dh, auth))).as("http sin permiso").isEqualTo(400);
        assertThat(post(bearer, request("https:/sin-host", p256dh, auth))).isEqualTo(400);
        assertThat(post(bearer, request("https://push.example/" + "x".repeat(2100), p256dh, auth))).isEqualTo(400);
        assertThat(post(bearer, request("https://push.example/a", null, auth))).isEqualTo(400);
        assertThat(post(bearer, request("https://push.example/a", "@@@", auth))).isEqualTo(400);
        assertThat(post(bearer, request("https://push.example/a", auth, auth))).as("p256dh corta").isEqualTo(400);
        assertThat(post(bearer, request("https://push.example/a", "A" + p256dh.substring(1), auth)))
                .as("no empieza en 0x04").isEqualTo(400);
        assertThat(post(bearer, request("https://push.example/a", p256dh, p256dh))).as("auth larga").isEqualTo(400);
        verify(subscriptions, never()).save(anyString(), any(), any());

        assertThat(post(bearer, request(" https://push.example/a ", p256dh, auth))).isEqualTo(201);
        verify(subscriptions).save(eq("u"), any(), eq(null));
    }

    @Test
    void soloLosOperadoresConEmpresaQuedanEnSuEmpresa() throws Exception {
        when(notifier.enabled()).thenReturn(true);

        post(token("op", "OPERADOR", "e-1"), request("https://push.example/op", p256dh, auth));
        post(token("op2", "OPERADOR", " "), request("https://push.example/op2", p256dh, auth));
        post(token("admin", "ADMIN", "e-1"), request("https://push.example/admin", p256dh, auth));
        controller.unsubscribe(token("op", "OPERADOR", "e-1"), null);
        controller.unsubscribe(token("op", "OPERADOR", "e-1"), new PushController.EndpointRequest(" "));

        verify(subscriptions).save(eq("op"), any(), eq("e-1"));
        verify(subscriptions).save(eq("op2"), any(), eq(null));
        verify(subscriptions).save(eq("admin"), any(), eq(null));
        verify(subscriptions, org.mockito.Mockito.times(2)).removeAll("op", "e-1");
    }

    @Test
    void rechazaTokensAusentesRevocadosSuspendidosOViejos() throws Exception {
        when(notifier.enabled()).thenReturn(true);
        PushController.SubscriptionRequest valid = request("https://push.example/a", p256dh, auth);
        String revocado = token("r", "PASAJERO", null);
        when(revocations.isRevoked("j-" + revocado.substring(7))).thenReturn(true);
        String suspendido = token("s", "PASAJERO", null);
        when(revocations.isSuspended("s")).thenReturn(true);
        String viejo = token("v", "PASAJERO", null);
        when(revocations.isPasswordChangedSince(eq("v"), any())).thenReturn(true);

        assertThat(post(null, valid)).isEqualTo(401);
        assertThat(post("Basic abc", valid)).isEqualTo(401);
        assertThat(post(token(null, "PASAJERO", null), valid)).as("sin sub").isEqualTo(401);
        assertThat(post(revocado, valid)).isEqualTo(401);
        assertThat(post(suspendido, valid)).isEqualTo(401);
        assertThat(post(viejo, valid)).isEqualTo(401);
        assertThat(controller.unsubscribe(null, null).getStatusCode().value()).isEqualTo(401);
        verify(subscriptions, never()).save(anyString(), any(), any());
    }
}
