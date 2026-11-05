package com.tripbus.realtime;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tripbus.realtime.push.FakePushServer;
import com.tripbus.realtime.push.PushNotifier;
import com.tripbus.realtime.snapshot.SnapshotService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.web.client.RestClient;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

/** US-185: sin llaves VAPID el push queda apagado (503 PUSH_DISABLED) y el resto funciona igual. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("it")
@Testcontainers
class RealtimePushApagadoIT {

    @Container
    @ServiceConnection(name = "redis")
    static GenericContainer<?> redis = RedisTestSupport.container();

    @LocalServerPort
    private int port;
    @Autowired
    private ObjectMapper mapper;
    @Autowired
    private PushNotifier notifier;

    @MockitoBean
    private JwtDecoder jwtDecoder;
    @MockitoBean
    private SnapshotService snapshotService;

    @Test
    void sinLlavesVapidPushApagadoYElWebSocketSigueFuncionando() throws Exception {
        UUID user = UUID.randomUUID();
        when(jwtDecoder.decode("t")).thenReturn(Jwt.withTokenValue("t").header("alg", "RS256")
                .subject(user.toString()).claim("role", "PASAJERO").issuedAt(Instant.now())
                .expiresAt(Instant.now().plusSeconds(600)).build());
        RestClient http = RestClient.builder().baseUrl("http://localhost:" + port)
                .defaultStatusHandler(HttpStatusCode::isError, (request, response) -> { }).build();

        ResponseEntity<String> key = http.get().uri("/push/public-key").retrieve().toEntity(String.class);
        assertThat(key.getStatusCode().value()).isEqualTo(503);
        assertThat(key.getBody()).contains("PUSH_DISABLED");
        ResponseEntity<String> post = http.post().uri("/push/subscriptions").header("Authorization", "Bearer t")
                .contentType(MediaType.APPLICATION_JSON).body(Map.of("endpoint", "https://push.example/x",
                        "keys", Map.of("p256dh", FakePushServer.newKeys().publicKey(), "auth", FakePushServer.newAuth())))
                .retrieve().toEntity(String.class);
        assertThat(post.getStatusCode().value()).isEqualTo(503);
        assertThat(http.delete().uri("/push/subscriptions").header("Authorization", "Bearer t").retrieve()
                .toBodilessEntity().getStatusCode().value()).isEqualTo(204);
        assertThat(notifier.enabled()).isFalse();
        assertThat(notifier.publicKey()).isNull();

        WsTestClient client = new WsTestClient(port, mapper);
        client.send("{\"type\":\"connect\",\"token\":\"t\"}");
        assertThat(client.next("connected").path("userId").asText()).isEqualTo(user.toString());
        client.send("{\"type\":\"ping\"}");
        assertThat(client.next("pong")).isNotNull();
        client.ws.close();
    }
}
