package com.tripbus.realtime.push;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tripbus.realtime.config.PushProperties;
import com.tripbus.realtime.ws.ConnectionPresence;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.time.Duration;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/** US-185 sin Redis real: apagado, llaves inválidas, destinatarios conectados y servicios de push caídos. */
class PushNotifierTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final PushSubscriptions subscriptions = mock(PushSubscriptions.class);
    private final ConnectionPresence presence = mock(ConnectionPresence.class);
    private final StringRedisTemplate redis = mock(StringRedisTemplate.class);

    private PushNotifier notifier(String publicKey, String privateKey) {
        return new PushNotifier(new PushProperties(publicKey, privateKey, "mailto:soporte@tripbus.test", 0, false,
                null), subscriptions, presence, redis, mapper);
    }

    @Test
    void sinLlavesOConLlavesInvalidasQuedaApagadoYNoHaceNada() throws Exception {
        PushNotifier sinLlaves = notifier(null, null);
        PushNotifier invalidas = notifier("no-es-una-llave", "tampoco");

        assertThat(sinLlaves.enabled()).isFalse();
        assertThat(invalidas.enabled()).isFalse();
        assertThat(invalidas.publicKey()).isNull();
        sinLlaves.onEvent("BookingAffected", "1-0", "e", mapper.readTree("{\"userId\":\"u\"}"));
        verifyNoInteractions(subscriptions, presence, redis);
    }

    @Test
    @SuppressWarnings("unchecked")
    void conLaAppAbiertaOYaEnviadoPorOtraReplicaNoEnvia() throws Exception {
        FakePushServer.Keys keys = FakePushServer.newKeys();
        PushNotifier notifier = notifier(" " + keys.publicKey() + " ", keys.privateKey());
        assertThat(notifier.publicKey()).isEqualTo(keys.publicKey());
        ValueOperations<String, String> values = mock(ValueOperations.class);
        when(redis.opsForValue()).thenReturn(values);
        when(presence.hasOpenConnection("abierta")).thenReturn(true);
        when(values.setIfAbsent(anyString(), anyString(), any(Duration.class))).thenReturn(false);

        notifier.onEvent("BookingAffected", "1-0", null, mapper.readTree("{\"userId\":\"abierta\"}"));
        notifier.onEvent("BookingAffected", "2-0", " ", mapper.readTree("{\"userId\":\"cerrada\"}"));
        notifier.onEvent("SeatHeld", "3-0", "e", mapper.readTree("{\"userId\":\"cerrada\"}"));

        // El evento sin eventId se reclama con su seq.
        verify(values, timeout(2000)).setIfAbsent(eq(PushNotifier.SENT_PREFIX + "2-0:cerrada"), eq("1"),
                any(Duration.class));
        verify(presence, timeout(2000)).hasOpenConnection("abierta");
        Thread.sleep(200);
        verify(subscriptions, org.mockito.Mockito.never()).of(anyString());
    }

    @Test
    @SuppressWarnings("unchecked")
    void unServicioDePushCaidoOUnErrorDeRedisNoRompenNada() throws Exception {
        FakePushServer.Keys keys = FakePushServer.newKeys();
        PushNotifier notifier = notifier(keys.publicKey(), keys.privateKey());
        ValueOperations<String, String> values = mock(ValueOperations.class);
        when(redis.opsForValue()).thenReturn(values);
        when(values.setIfAbsent(anyString(), anyString(), any(Duration.class))).thenReturn(true);
        String p256dh = FakePushServer.newKeys().publicKey();
        when(subscriptions.of("u")).thenReturn(List.of(
                // Nada escucha en el puerto 9: la conexión falla.
                new PushSubscriptions.Subscription("http://127.0.0.1:9/caido", p256dh, FakePushServer.newAuth(), 1),
                // Llave del navegador inválida: falla al cifrar.
                new PushSubscriptions.Subscription("http://127.0.0.1:9/otro", "AAAA", FakePushServer.newAuth(), 2)));
        when(subscriptions.of("sin-suscripciones")).thenReturn(List.of());
        when(subscriptions.operatorsOf("e-1")).thenThrow(new IllegalStateException("redis caído"));

        notifier.onEvent("OfferAlert", "1-0", "e1", mapper.readTree("{\"userId\":\"u\"}"));
        notifier.onEvent("OfferAlert", "1-1", "e2", mapper.readTree("{\"userId\":\"sin-suscripciones\"}"));
        notifier.onEvent("TripAlertRaised", "1-2", "e3",
                mapper.readTree("{\"companyId\":\"e-1\",\"gravedad\":\"CRITICA\"}"));

        verify(subscriptions, timeout(5000)).of("u");
        verify(subscriptions, timeout(5000)).of("sin-suscripciones");
        verify(subscriptions, timeout(5000)).operatorsOf("e-1");
        Thread.sleep(1500);
        verify(subscriptions, org.mockito.Mockito.never()).remove(anyString(), anyString());
        assertThat(Set.of(notifier.enabled())).containsExactly(true);
    }

    @Test
    @SuppressWarnings("unchecked")
    void unErrorDelServicioDePushNoBorraLaSuscripcionYUnEndpointRaroNoRompeElLog() throws Exception {
        FakePushServer.Keys keys = FakePushServer.newKeys();
        PushNotifier notifier = notifier(keys.publicKey(), keys.privateKey());
        ValueOperations<String, String> values = mock(ValueOperations.class);
        when(redis.opsForValue()).thenReturn(values);
        when(values.setIfAbsent(anyString(), anyString(), any(Duration.class))).thenReturn(true);
        String p256dh = FakePushServer.newKeys().publicKey();
        try (FakePushServer server = new FakePushServer()) {
            when(subscriptions.of("u")).thenReturn(List.of(
                    new PushSubscriptions.Subscription(server.url("/error/u"), p256dh, FakePushServer.newAuth(), 1),
                    new PushSubscriptions.Subscription("http://[raro", p256dh, FakePushServer.newAuth(), 2)));

            notifier.onEvent("BookingRelocated", "1-0", "e", mapper.readTree("{\"userId\":\"u\"}"));

            for (int i = 0; i < 100 && server.to("/error/u").isEmpty(); i++) {
                Thread.sleep(50);
            }
            assertThat(server.to("/error/u")).hasSize(1);
            Thread.sleep(300);
            verify(subscriptions, org.mockito.Mockito.never()).remove(anyString(), anyString());
        }
    }
}
