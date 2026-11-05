package com.tripbus.realtime;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import com.tripbus.realtime.events.EventFanout;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * HT-10: el snapshot pedido por el WebSocket propaga traceparent al servicio dueño (fleet simulado), como hijo
 * del traceparent que manda el cliente si lo trae; y la difusión de un evento continúa la traza de su campo
 * traceparent (el traceId queda en los logs de la difusión).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "management.tracing.sampling.probability=1.0"})
@AutoConfigureObservability
@ActiveProfiles("it")
@Testcontainers
class RealtimeTrazasIT {

    private static final String TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736";
    private static final String PARENT = "00-" + TRACE_ID + "-00f067aa0ba902b7-01";

    @Container
    @ServiceConnection(name = "redis")
    static GenericContainer<?> redis = RedisTestSupport.container();

    /** traceparent de cada petición que recibe el fleet simulado. */
    static final BlockingQueue<String> fleetTraceparents = new LinkedBlockingQueue<>();
    static final HttpServer fleet = startFleet();

    private static HttpServer startFleet() {
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
            server.createContext("/", exchange -> {
                String traceparent = exchange.getRequestHeaders().getFirst("traceparent");
                fleetTraceparents.add(traceparent == null ? "" : traceparent);
                byte[] body = "{\"estado\":\"EN_RUTA\"}".getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, body.length);
                exchange.getResponseBody().write(body);
                exchange.close();
            });
            server.start();
            return server;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @DynamicPropertySource
    static void fleetUrl(DynamicPropertyRegistry registry) {
        registry.add("tripbus.realtime.fleet-url", () -> "http://localhost:" + fleet.getAddress().getPort());
    }

    @AfterAll
    static void stopFleet() {
        fleet.stop(0);
    }

    @LocalServerPort
    private int port;
    @Autowired
    private StringRedisTemplate redisTemplate;
    @Autowired
    private ObjectMapper mapper;

    @MockitoBean
    private JwtDecoder jwtDecoder;

    @Test
    void elSnapshotPropagaElTraceparentDelClienteOUnoNuevo() throws Exception {
        WsTestClient client = new WsTestClient(port, mapper);
        client.send("{\"type\":\"connect\"}");
        client.next("connected");

        client.send("{\"type\":\"subscribe\",\"topic\":\"viaje:" + UUID.randomUUID() + ":posicion\",\"traceparent\":\""
                + PARENT + "\"}");
        assertThat(client.next("snapshot").path("data").path("estado").asText()).isEqualTo("EN_RUTA");
        String hijo = fleetTraceparents.poll(5, TimeUnit.SECONDS);
        assertThat(hijo).startsWith("00-" + TRACE_ID + "-").isNotEqualTo(PARENT);

        client.send("{\"type\":\"subscribe\",\"topic\":\"viaje:" + UUID.randomUUID() + ":eventos\"}");
        client.next("snapshot");
        String nuevo = fleetTraceparents.poll(5, TimeUnit.SECONDS);
        assertThat(nuevo).matches("00-[0-9a-f]{32}-[0-9a-f]{16}-0[01]").doesNotContain(TRACE_ID);
        client.ws.close();
    }

    @Test
    void laDifusionContinuaLaTrazaDelEvento() throws Exception {
        Logger logger = (Logger) LoggerFactory.getLogger(EventFanout.class);
        // Thread-safe: los EventFanout de otros contextos de prueba también escriben en este logger.
        List<ILoggingEvent> logs = new CopyOnWriteArrayList<>();
        AppenderBase<ILoggingEvent> appender = new AppenderBase<>() {
            @Override
            protected void append(ILoggingEvent event) {
                logs.add(event);
            }
        };
        appender.start();
        logger.addAppender(appender);
        try {
            // Un aviso sin userId deja un WARN durante la difusión: lleva el traceId del evento.
            String eventId = UUID.randomUUID().toString();
            redisTemplate.opsForStream().add(StreamRecords.newRecord().in("tripbus:events:realtime-test").ofMap(Map.of(
                    "eventId", eventId, "type", "BookingAffected",
                    "occurredAt", Instant.now().toString(), "traceparent", PARENT, "payload", "{}")));
            ILoggingEvent warn = null;
            for (int i = 0; i < 100 && warn == null; i++) {
                warn = logs.stream().filter(e -> e.getFormattedMessage().contains(eventId)).findFirst().orElse(null);
                Thread.sleep(50);
            }
            assertThat(warn).as("WARN de la difusión").isNotNull();
            assertThat(warn.getMDCPropertyMap()).containsEntry("traceId", TRACE_ID);
        } finally {
            logger.detachAppender(appender);
        }
    }
}
