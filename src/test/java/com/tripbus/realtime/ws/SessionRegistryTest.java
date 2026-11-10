package com.tripbus.realtime.ws;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.web.socket.WebSocketSession;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SessionRegistryTest {

    private final SimpleMeterRegistry meters = new SimpleMeterRegistry();
    private final SessionRegistry registry = new SessionRegistry(meters);

    private ClientSession session(String id) {
        WebSocketSession raw = mock(WebSocketSession.class);
        when(raw.getId()).thenReturn(id);
        ClientSession session = new ClientSession(raw, Instant.now());
        registry.add(session);
        return session;
    }

    @Test
    void cuentaSuscripcionesPorTipoYAvisaUnaSolaVezAlQuitarUnaSesion() {
        List<String> removed = new ArrayList<>();
        registry.onRemove(s -> removed.add(s.id()));
        String cupo = "oferta:" + UUID.randomUUID() + ":cupo";
        ClientSession a = session("a");
        ClientSession b = session("b");
        registry.subscribe(a, cupo);
        registry.subscribe(b, cupo);
        registry.subscribe(a, "ofertas:activas");
        // Un tópico inválido (no debería llegar, pero no rompe el conteo).
        registry.subscribe(b, "no-es-un-topico");

        assertThat(registry.subscriptions(Topics.Kind.CUPO)).isEqualTo(2);
        assertThat(registry.subscriptions(Topics.Kind.OFERTAS_ACTIVAS)).isEqualTo(1);
        assertThat(registry.subscriptions(Topics.Kind.AVISOS)).isZero();
        assertThat(registry.topics()).contains(cupo, "ofertas:activas");

        registry.remove("a");
        registry.remove("a");
        registry.remove("desconocida");

        assertThat(removed).containsExactly("a");
        assertThat(registry.subscriptions(Topics.Kind.CUPO)).isEqualTo(1);
        assertThat(registry.topics()).doesNotContain("ofertas:activas");

        WsMetrics metrics = new WsMetrics(meters, registry);
        metrics.sent(Topics.Kind.CUPO);
        metrics.replay(WsMetrics.REPLAY_SNAPSHOT);
        b.authenticate("u", "j", "PASAJERO", Instant.now().plusSeconds(60));
        assertThat(meters.get("tripbus.ws.conexiones").tag("autenticada", "true").gauge().value()).isEqualTo(1.0);
        assertThat(meters.get("tripbus.ws.conexiones").tag("autenticada", "false").gauge().value()).isZero();
        assertThat(meters.get("tripbus.ws.suscripciones").tag("tipo_topico", "cupo").gauge().value()).isEqualTo(1.0);
        assertThat(meters.get("tripbus.ws.mensajes.enviados").tag("tipo_topico", "cupo").counter().count())
                .isEqualTo(1.0);
        assertThat(meters.get("tripbus.ws.replay").tag("resultado", "snapshot").counter().count()).isEqualTo(1.0);
        assertThat(meters.get("tripbus.ws.replay").tag("resultado", "completo").counter().count()).isZero();
    }
}
