package com.tripbus.realtime;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import com.tripbus.realtime.snapshot.SnapshotService;
import com.tripbus.realtime.ws.RealtimeHandler;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.web.client.RestClient;
import org.springframework.web.socket.WebSocketHttpHeaders;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * HT-11: máximo de conexiones por IP y por usuario contadas en Redis (por encima: TOO_MANY_CONNECTIONS y
 * cierre 4429), máximo de 30 mensajes del cliente por 10 s (RATE_LIMITED sin cerrar) y las métricas en
 * /actuator/prometheus. Cada prueba usa su propia IP en el header del proxy (client-ip-header).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "tripbus.realtime.max-connections-per-ip=3",
        "tripbus.realtime.max-connections-per-user=2",
        "tripbus.realtime.client-ip-header=X-Forwarded-For"})
@AutoConfigureObservability
@ActiveProfiles("it")
@Testcontainers
class RealtimeLimitesIT {

    @Container
    @ServiceConnection(name = "redis")
    static GenericContainer<?> redis = RedisTestSupport.container();

    @LocalServerPort
    private int port;
    @Autowired
    private StringRedisTemplate redisTemplate;
    @Autowired
    private ObjectMapper mapper;

    @MockitoBean
    private JwtDecoder jwtDecoder;
    @MockitoBean
    private SnapshotService snapshotService;

    private static String randomIp() {
        return "10." + (int) (Math.random() * 250) + "." + (int) (Math.random() * 250) + "." + (int) (Math.random() * 250);
    }

    /** Conexión cuyo proxy dice que viene de {@code ip} (el cliente pudo agregar otra antes: se ignora). */
    private WsTestClient from(String ip) throws Exception {
        WebSocketHttpHeaders headers = new WebSocketHttpHeaders();
        headers.add("X-Forwarded-For", "1.2.3.4, " + ip);
        return new WsTestClient(port, mapper, headers);
    }

    private String token(UUID userId) {
        String token = "token-" + UUID.randomUUID();
        when(jwtDecoder.decode(token)).thenReturn(Jwt.withTokenValue(token).header("alg", "RS256")
                .subject(userId.toString()).claim("role", "PASAJERO").issuedAt(Instant.now())
                .expiresAt(Instant.now().plusSeconds(600)).build());
        return token;
    }

    private static void assertTooMany(WsTestClient client) throws Exception {
        assertThat(client.next("error").path("code").asText()).isEqualTo("TOO_MANY_CONNECTIONS");
        assertThat(client.awaitClose().getCode()).isEqualTo(RealtimeHandler.TOO_MANY_CONNECTIONS.getCode());
    }

    private String prometheus() {
        return RestClient.create("http://localhost:" + port).get().uri("/actuator/prometheus").retrieve()
                .body(String.class);
    }

    @Test
    void porEncimaDelMaximoDeConexionesPorIpCierraCon4429() throws Exception {
        String ip = randomIp();
        List<WsTestClient> abiertas = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            abiertas.add(from(ip));
        }
        assertTooMany(from(ip));

        // Otra IP no se ve afectada; al cerrar una de las tres, vuelve a haber cupo.
        WsTestClient otraIp = from(randomIp());
        otraIp.send("{\"type\":\"ping\"}");
        otraIp.next("pong");
        abiertas.get(0).ws.close();
        abiertas.get(0).awaitClose();
        WsTestClient nueva = awaitAccepted(ip);
        nueva.send("{\"type\":\"ping\"}");
        assertThat(nueva.next("pong")).isNotNull();
        assertThat(redisTemplate.opsForZSet().size("tripbus:ws:conn:ip:" + ip)).isEqualTo(3);
    }

    /** El cierre se descuenta en cuanto el servidor lo procesa: reintenta unas veces. */
    private WsTestClient awaitAccepted(String ip) throws Exception {
        for (int i = 0; i < 20; i++) {
            WsTestClient client = from(ip);
            client.send("{\"type\":\"ping\"}");
            JsonNode reply = client.received.poll(5, TimeUnit.SECONDS);
            if (reply != null && "pong".equals(reply.path("type").asText())) {
                client.received.add(reply);
                return client;
            }
            Thread.sleep(100);
        }
        throw new AssertionError("no se liberó el cupo de la IP " + ip);
    }

    @Test
    void porEncimaDelMaximoDeConexionesPorUsuarioCierraCon4429SinAfectarAOtros() throws Exception {
        UUID user = UUID.randomUUID();
        for (int i = 0; i < 2; i++) {
            WsTestClient client = from(randomIp());
            client.send("{\"type\":\"connect\",\"token\":\"" + token(user) + "\"}");
            assertThat(client.next("connected").path("userId").asText()).isEqualTo(user.toString());
        }
        WsTestClient tercera = from(randomIp());
        tercera.send("{\"type\":\"connect\",\"token\":\"" + token(user) + "\"}");
        assertTooMany(tercera);

        WsTestClient otroUsuario = from(randomIp());
        otroUsuario.send("{\"type\":\"connect\",\"token\":\"" + token(UUID.randomUUID()) + "\"}");
        assertThat(otroUsuario.next("connected")).isNotNull();
        // Anónimas: solo cuentan por IP.
        WsTestClient anonima = from(randomIp());
        anonima.send("{\"type\":\"connect\"}");
        assertThat(anonima.next("connected").path("anonymous").asBoolean()).isTrue();
    }

    @Test
    void masDe30MensajesEn10sRespondeRateLimitedSinCerrar() throws Exception {
        WsTestClient client = from(randomIp());
        for (int i = 0; i < 31; i++) {
            client.send("{\"type\":\"ping\"}");
        }
        int pongs = 0;
        JsonNode error = null;
        while (pongs + (error == null ? 0 : 1) < 31) {
            JsonNode msg = client.received.poll(5, TimeUnit.SECONDS);
            assertThat(msg).as("respuesta").isNotNull();
            if ("pong".equals(msg.path("type").asText())) {
                pongs++;
            } else if ("error".equals(msg.path("type").asText())) {
                error = msg;
            }
        }
        assertThat(pongs).isEqualTo(30);
        assertThat(error).isNotNull();
        assertThat(error.path("code").asText()).isEqualTo("RATE_LIMITED");
        assertThat(client.ws.isOpen()).isTrue();
        assertThat(client.closed).isNotDone();
    }

    @Test
    void lasMetricasDeConexionesSuscripcionesEnviosCierresYReconexionSeExponen() throws Exception {
        when(snapshotService.snapshot(any())).thenReturn(Optional.of(NullNode.getInstance()));
        UUID itinerary = UUID.randomUUID();
        WsTestClient client = from(randomIp());
        client.send("{\"type\":\"connect\",\"token\":\"" + token(UUID.randomUUID()) + "\"}");
        client.next("connected");
        client.send("{\"type\":\"subscribe\",\"topic\":\"itinerario:" + itinerary + ":asientos\"}");
        client.next("snapshot");
        redisTemplate.opsForStream().add(StreamRecords.newRecord().in("tripbus:events:realtime-test").ofMap(Map.of(
                "eventId", UUID.randomUUID().toString(), "type", "SeatHeld", "occurredAt", Instant.now().toString(),
                "payload", "{\"itineraryId\":\"" + itinerary + "\",\"seatNumbers\":[\"9\"]}")));
        client.next("event");
        // Un cierre 4429.
        String ip = randomIp();
        for (int i = 0; i < 3; i++) {
            from(ip);
        }
        assertTooMany(from(ip));

        String metrics = prometheus();

        assertThat(metrics).contains("tripbus_ws_conexiones{application=\"realtime-gateway\",autenticada=\"true\"}")
                .contains("tripbus_ws_conexiones{application=\"realtime-gateway\",autenticada=\"false\"}")
                .contains("tripbus_ws_suscripciones{application=\"realtime-gateway\",tipo_topico=\"asientos\"} 1.0")
                .contains("tripbus_ws_mensajes_enviados_total{application=\"realtime-gateway\",tipo_topico=\"asientos\"}")
                .contains("tripbus_ws_cierres_total{application=\"realtime-gateway\",codigo=\"4429\"}")
                .contains("tripbus_ws_replay_total{application=\"realtime-gateway\",resultado=\"completo\"}");
        assertThat(metrics).doesNotContain("tripbus_ws_mensajes_enviados_total{application=\"realtime-gateway\",tipo_topico=\"asientos\"} 0.0");
    }
}
