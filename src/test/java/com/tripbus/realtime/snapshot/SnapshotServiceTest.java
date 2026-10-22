package com.tripbus.realtime.snapshot;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpServer;
import com.tripbus.realtime.config.RealtimeProperties;
import com.tripbus.realtime.ws.Topics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

/** Rutas y manejo de errores de los snapshots contra un servidor HTTP local que hace de los servicios dueños. */
class SnapshotServiceTest {

    private final Map<String, Integer> status = new ConcurrentHashMap<>();
    private HttpServer server;
    private SnapshotService snapshots;

    @BeforeEach
    void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/", exchange -> {
            String path = exchange.getRequestURI().getPath();
            int code = status.getOrDefault(path, 404);
            byte[] body = ("{\"path\":\"" + path + "\"}").getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(code, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        String url = "http://localhost:" + server.getAddress().getPort();
        snapshots = new SnapshotService(new RealtimeProperties(null, null, null, null, null, null, 0, 0,
                url, url, url, null, false));
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    private Optional<JsonNode> snapshot(String topic) {
        return snapshots.snapshot(Topics.parse(topic));
    }

    @Test
    void laPosicionPorViajeSePideAFleetPorItinerario() {
        UUID id = UUID.randomUUID();
        status.put("/itineraries/" + id + "/bus-position", 200);

        assertThat(snapshot("viaje:" + id + ":posicion")).get()
                .satisfies(data -> assertThat(data.path("path").asText()).isEqualTo("/itineraries/" + id + "/bus-position"));
    }

    @Test
    void cadaTopicoUsaLaRutaDeSuServicio() {
        UUID id = UUID.randomUUID();
        Map<String, String> routes = Map.of(
                "itinerario:" + id + ":asientos", "/seats/itineraries/" + id,
                "oferta:" + id + ":cupo", "/offers/" + id + "/quota",
                "viaje:" + id + ":eventos", "/trips/" + id + "/timeline",
                "bus:" + id + ":posicion", "/buses/" + id + "/position");
        routes.values().forEach(path -> status.put(path, 200));

        routes.forEach((topic, path) ->
                assertThat(snapshot(topic).orElseThrow().path("path").asText()).as(topic).isEqualTo(path));
        assertThat(snapshot("usuario:" + id + ":tiquetes")).get().satisfies(data -> assertThat(data.isNull()).isTrue());
    }

    @Test
    void sinDatosEnFleetDevuelveNullYConErrorVacio() {
        UUID id = UUID.randomUUID();
        assertThat(snapshot("viaje:" + id + ":posicion")).get().satisfies(data -> assertThat(data.isNull()).isTrue());

        status.put("/itineraries/" + id + "/bus-position", 503);
        assertThat(snapshot("viaje:" + id + ":posicion")).isEmpty();
    }

    @Test
    void servicioCaidoDevuelveVacio() {
        UUID id = UUID.randomUUID();
        server.stop(0);
        assertThat(snapshot("viaje:" + id + ":posicion")).isEmpty();
    }
}
