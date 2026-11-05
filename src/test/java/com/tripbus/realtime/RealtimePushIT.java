package com.tripbus.realtime;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tripbus.realtime.config.PushProperties;
import com.tripbus.realtime.push.FakePushServer;
import com.tripbus.realtime.push.PushNotifier;
import com.tripbus.realtime.push.PushSubscriptions;
import com.tripbus.realtime.snapshot.SnapshotService;
import com.tripbus.realtime.ws.ConnectionPresence;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.security.oauth2.jwt.BadJwtException;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.web.client.RestClient;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

/**
 * US-185: registro de suscripciones (autenticado, máximo 5 en Redis) y envío de Web Push a un servicio de push
 * simulado, SOLO si el destinatario no tiene ninguna conexión abierta; una suscripción que responde 410 se borra
 * y dos réplicas mandan un solo push por evento.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "tripbus.push.allow-http-endpoints=true",
        "tripbus.realtime.presence-ttl=60s"})
@ActiveProfiles("it")
@Testcontainers
class RealtimePushIT {

    private static final FakePushServer.Keys VAPID = FakePushServer.newKeys();
    private static final FakePushServer pushServer = new FakePushServer();

    @Container
    @ServiceConnection(name = "redis")
    static GenericContainer<?> redis = RedisTestSupport.container();

    @DynamicPropertySource
    static void vapid(DynamicPropertyRegistry registry) {
        registry.add("tripbus.push.vapid-public-key", VAPID::publicKey);
        registry.add("tripbus.push.vapid-private-key", VAPID::privateKey);
        registry.add("tripbus.push.vapid-subject", () -> "mailto:soporte@tripbus.test");
    }

    @AfterAll
    static void stop() {
        pushServer.close();
    }

    @LocalServerPort
    private int port;
    @Autowired
    private StringRedisTemplate redisTemplate;
    @Autowired
    private ObjectMapper mapper;
    @Autowired
    private PushProperties pushProperties;
    @Autowired
    private PushSubscriptions subscriptions;
    @Autowired
    private ConnectionPresence presence;

    @MockitoBean
    private JwtDecoder jwtDecoder;
    @MockitoBean
    private SnapshotService snapshotService;

    private RestClient http() {
        return RestClient.builder().baseUrl("http://localhost:" + port)
                .defaultStatusHandler(HttpStatusCode::isError, (request, response) -> { })
                .build();
    }

    private String token(UUID userId, String role, UUID companyId) {
        String token = "token-" + UUID.randomUUID();
        Jwt.Builder jwt = Jwt.withTokenValue(token).header("alg", "RS256").subject(userId.toString())
                .claim("role", role).issuedAt(Instant.now()).expiresAt(Instant.now().plusSeconds(600));
        if (companyId != null) {
            jwt.claim("companyId", companyId.toString());
        }
        when(jwtDecoder.decode(token)).thenReturn(jwt.build());
        return token;
    }

    private int register(String token, String endpoint) {
        return register(token, Map.of("endpoint", endpoint,
                "keys", Map.of("p256dh", FakePushServer.newKeys().publicKey(), "auth", FakePushServer.newAuth())));
    }

    private int register(String token, Object body) {
        return http().post().uri("/push/subscriptions").header("Authorization", "Bearer " + token)
                .contentType(MediaType.APPLICATION_JSON).body(body).retrieve().toBodilessEntity()
                .getStatusCode().value();
    }

    private void publish(String type, String payload) {
        redisTemplate.opsForStream().add(StreamRecords.newRecord().in("tripbus:events:realtime-test").ofMap(Map.of(
                "eventId", UUID.randomUUID().toString(), "type", type, "occurredAt", Instant.now().toString(),
                "payload", payload)));
    }

    private static void await(String what, BooleanSupplier condition) throws InterruptedException {
        for (int i = 0; i < 100; i++) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("no se cumplió: " + what);
    }

    @Test
    void registraSuscripcionesAutenticadasYGuardaComoMaximo5() throws Exception {
        JsonNode key = mapper.readTree(http().get().uri("/push/public-key").retrieve().body(String.class));
        assertThat(key.path("publicKey").asText()).isEqualTo(VAPID.publicKey());

        UUID user = UUID.randomUUID();
        String token = token(user, "PASAJERO", null);
        assertThat(http().post().uri("/push/subscriptions").contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("endpoint", pushServer.url("/x"))).retrieve().toBodilessEntity().getStatusCode().value())
                .isEqualTo(401);
        when(jwtDecoder.decode("malo")).thenThrow(new BadJwtException("firma"));
        assertThat(register("malo", pushServer.url("/x"))).isEqualTo(401);
        assertThat(register(token, Map.of("endpoint", pushServer.url("/x"),
                "keys", Map.of("p256dh", "corta", "auth", FakePushServer.newAuth())))).isEqualTo(400);
        assertThat(register(token, Map.of("endpoint", "ftp://push.example/x",
                "keys", Map.of("p256dh", FakePushServer.newKeys().publicKey(), "auth", FakePushServer.newAuth()))))
                .isEqualTo(400);

        for (int i = 1; i <= 6; i++) {
            assertThat(register(token, pushServer.url("/limite/" + user + "/" + i))).isEqualTo(201);
            Thread.sleep(2);
        }
        List<String> endpoints = subscriptions.of(user.toString()).stream()
                .map(PushSubscriptions.Subscription::endpoint).toList();
        assertThat(endpoints).hasSize(pushProperties.maxSubscriptionsPerUser())
                .doesNotContain(pushServer.url("/limite/" + user + "/1"))
                .contains(pushServer.url("/limite/" + user + "/6"));

        assertThat(http().method(org.springframework.http.HttpMethod.DELETE).uri("/push/subscriptions")
                .header("Authorization", "Bearer " + token).contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("endpoint", pushServer.url("/limite/" + user + "/6"))).retrieve().toBodilessEntity()
                .getStatusCode().value()).isEqualTo(204);
        assertThat(subscriptions.of(user.toString())).hasSize(4);
        assertThat(http().delete().uri("/push/subscriptions").header("Authorization", "Bearer " + token).retrieve()
                .toBodilessEntity().getStatusCode().value()).isEqualTo(204);
        assertThat(subscriptions.of(user.toString())).isEmpty();
        assertThat(http().delete().uri("/push/subscriptions").retrieve().toBodilessEntity().getStatusCode().value())
                .isEqualTo(401);
    }

    @Test
    void seEnviaPushSoloSiElPasajeroNoTieneNingunaConexionAbierta() throws Exception {
        UUID user = UUID.randomUUID();
        String endpoint = "/pasajero/" + user;
        assertThat(register(token(user, "PASAJERO", null), pushServer.url(endpoint))).isEqualTo(201);

        WsTestClient app = new WsTestClient(port, mapper);
        app.send("{\"type\":\"connect\",\"token\":\"" + token(user, "PASAJERO", null) + "\"}");
        app.next("connected");
        publish("BookingAffected", "{\"userId\":\"" + user + "\",\"bookingId\":\"b-1\",\"causa\":\"CANCELACION\"}");
        Thread.sleep(800);
        assertThat(pushServer.to(endpoint)).as("con la app abierta no hay push").isEmpty();

        app.ws.close();
        app.awaitClose();
        await("conexión descontada", () -> !presence.hasOpenConnection(user.toString()));
        publish("BookingAffected", "{\"userId\":\"" + user + "\",\"bookingId\":\"b-1\",\"causa\":\"CANCELACION\"}");
        // Un evento que no se avisa por push.
        publish("RefundProcessed", "{\"userId\":\"" + user + "\",\"refundId\":\"r-1\"}");

        await("push recibido", () -> !pushServer.to(endpoint).isEmpty());
        FakePushServer.Received push = pushServer.to(endpoint).get(0);
        assertThat(push.header("Content-Encoding")).isEqualTo("aes128gcm");
        assertThat(push.header("Authorization")).startsWith("vapid t=").contains("k=" + VAPID.publicKey());
        assertThat(push.header("TTL")).isEqualTo("3600");
        assertThat(push.header("Urgency")).isEqualTo("high");
        assertThat(push.bodyLength()).isPositive();
        Thread.sleep(500);
        assertThat(pushServer.to(endpoint)).hasSize(1);
    }

    @Test
    void unaSuscripcionQueResponde410O404SeBorra() throws Exception {
        UUID user = UUID.randomUUID();
        String token = token(user, "PASAJERO", null);
        register(token, pushServer.url("/gone/" + user));
        register(token, pushServer.url("/missing/" + user));
        register(token, pushServer.url("/vigente/" + user));
        assertThat(subscriptions.of(user.toString())).hasSize(3);

        publish("WaitlistSeatOffered", "{\"userId\":\"" + user + "\",\"itineraryId\":\"" + UUID.randomUUID() + "\"}");

        await("suscripciones vencidas borradas", () -> subscriptions.of(user.toString()).size() == 1);
        assertThat(subscriptions.of(user.toString()).get(0).endpoint()).isEqualTo(pushServer.url("/vigente/" + user));
        assertThat(pushServer.to("/gone/" + user)).hasSize(1);
    }

    @Test
    void conductorYOperadoresDeLaEmpresaParaAlertasCriticas() throws Exception {
        UUID driver = UUID.randomUUID();
        UUID operator = UUID.randomUUID();
        UUID company = UUID.randomUUID();
        register(token(driver, "CONDUCTOR", null), pushServer.url("/conductor/" + driver));
        register(token(operator, "OPERADOR", company), pushServer.url("/operador/" + operator));

        publish("DriverAssigned", "{\"conductorId\":\"" + driver + "\",\"itineraryId\":\"" + UUID.randomUUID() + "\"}");
        publish("TripAlertRaised", "{\"companyId\":\"" + company + "\",\"gravedad\":\"MEDIA\",\"tipo\":\"RETRASO\"}");
        publish("TripAlertRaised", "{\"companyId\":\"" + company + "\",\"gravedad\":\"CRITICA\",\"tipo\":\"SOS\"}");

        await("push al conductor", () -> !pushServer.to("/conductor/" + driver).isEmpty());
        await("push al operador", () -> !pushServer.to("/operador/" + operator).isEmpty());
        Thread.sleep(500);
        assertThat(pushServer.to("/operador/" + operator)).as("solo la CRITICA").hasSize(1);
    }

    @Test
    void conDosReplicasSeEnviaUnSoloPushPorEvento() throws Exception {
        UUID user = UUID.randomUUID();
        String endpoint = "/dos-replicas/" + user;
        register(token(user, "PASAJERO", null), pushServer.url(endpoint));
        PushNotifier otraReplica = new PushNotifier(pushProperties, subscriptions, presence, redisTemplate, mapper);
        String eventId = UUID.randomUUID().toString();
        JsonNode payload = mapper.readTree("{\"userId\":\"" + user + "\",\"offerId\":\"" + UUID.randomUUID() + "\"}");

        redisTemplate.opsForStream().add(StreamRecords.newRecord().in("tripbus:events:realtime-test").ofMap(Map.of(
                "eventId", eventId, "type", "OfferQueueAdmitted", "occurredAt", Instant.now().toString(),
                "payload", payload.toString())));
        otraReplica.onEvent("OfferQueueAdmitted", "1-0", eventId, payload);

        await("push recibido", () -> !pushServer.to(endpoint).isEmpty());
        Thread.sleep(Duration.ofMillis(800));
        assertThat(pushServer.to(endpoint)).hasSize(1);
    }
}
