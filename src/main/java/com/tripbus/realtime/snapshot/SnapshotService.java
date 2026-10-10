package com.tripbus.realtime.snapshot;

import com.fasterxml.jackson.databind.JsonNode;
import com.tripbus.realtime.config.RealtimeProperties;
import com.tripbus.realtime.config.RequestIdPropagationInterceptor;
import com.tripbus.realtime.ws.Topics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

import java.util.Optional;

/** Snapshot inicial de un tópico: se lee del servicio dueño del dato (el gateway no guarda nada). */
@Component
public class SnapshotService {

    private static final Logger log = LoggerFactory.getLogger(SnapshotService.class);

    private final RestClient seatInventory;
    private final RestClient flashOffers;
    private final RestClient fleet;

    public SnapshotService(RealtimeProperties properties) {
        this.seatInventory = client(properties.seatInventoryUrl());
        this.flashOffers = client(properties.flashOffersUrl());
        this.fleet = client(properties.fleetUrl());
    }

    private static RestClient client(String baseUrl) {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(1000);
        factory.setReadTimeout(3000);
        return RestClient.builder()
                .baseUrl(baseUrl)
                .requestFactory(factory)
                .requestInterceptor(new RequestIdPropagationInterceptor())
                .build();
    }

    /** Optional.empty() = no se pudo obtener; Optional.of(NullNode) = no hay datos todavía. */
    public Optional<JsonNode> snapshot(Topics.Parsed topic) {
        try {
            JsonNode data = switch (topic.kind()) {
                case ASIENTOS -> seatInventory.get().uri("/seats/itineraries/{id}", topic.id()).retrieve().body(JsonNode.class);
                case CUPO -> flashOffers.get().uri("/offers/{id}/quota", topic.id()).retrieve().body(JsonNode.class);
                case EVENTOS -> fleet.get().uri("/trips/{id}/timeline", topic.id()).retrieve().body(JsonNode.class);
                case POSICION -> fleet.get().uri("/buses/{id}/position", topic.id()).retrieve().body(JsonNode.class);
            };
            return Optional.ofNullable(data);
        } catch (HttpClientErrorException.NotFound e) {
            return Optional.of(com.fasterxml.jackson.databind.node.NullNode.getInstance());
        } catch (RuntimeException e) {
            log.warn("No se pudo obtener el snapshot de {}:{} — {}", topic.kind(), topic.id(), e.getMessage());
            return Optional.empty();
        }
    }
}
