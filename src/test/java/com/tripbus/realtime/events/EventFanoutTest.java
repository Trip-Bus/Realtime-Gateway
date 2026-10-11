package com.tripbus.realtime.events;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tripbus.realtime.config.RealtimeProperties;
import com.tripbus.realtime.ws.ClientSession;
import com.tripbus.realtime.ws.SessionRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.SessionLimitExceededException;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class EventFanoutTest {

    private final UUID itinerary = UUID.randomUUID();
    private final String topic = "itinerario:" + itinerary + ":asientos";
    private SimpleMeterRegistry meters;
    private SessionRegistry registry;
    private EventFanout fanout;

    @BeforeEach
    void setUp() {
        meters = new SimpleMeterRegistry();
        registry = new SessionRegistry(meters);
        RealtimeProperties properties = new RealtimeProperties(null, null, null, null, null, null, 0,
                null, null, null, null, false);
        fanout = new EventFanout(mock(RedisConnectionFactory.class), mock(StringRedisTemplate.class), registry,
                new ObjectMapper(), properties, meters);
    }

    private static WebSocketSession raw(String id) {
        WebSocketSession raw = mock(WebSocketSession.class);
        when(raw.getId()).thenReturn(id);
        when(raw.isOpen()).thenReturn(true);
        return raw;
    }

    private void register(ClientSession session) {
        registry.add(session);
        registry.subscribe(session, topic);
    }

    private MapRecord<String, String, String> seatHeld() {
        return StreamRecords.newRecord().in("tripbus:events").ofMap(Map.of(
                "type", "SeatHeld",
                "eventId", UUID.randomUUID().toString(),
                "occurredAt", Instant.now().toString(),
                "payload", "{\"itineraryId\":\"" + itinerary + "\",\"seat\":\"12\",\"userId\":\"privado\"}"));
    }

    private double delivered() {
        return meters.get("tripbus.realtime.messages.delivered").counter().count();
    }

    @Test
    void clienteLentoNoCortaLaDifusionALosDemas() throws IOException {
        WebSocketSession first = raw("primero");
        WebSocketSession slow = raw("lento");
        WebSocketSession third = raw("tercero");
        doThrow(new SessionLimitExceededException("buffer lleno", CloseStatus.SESSION_NOT_RELIABLE))
                .when(slow).sendMessage(any());
        register(new ClientSession(first, Instant.now()));
        ClientSession slowSession = new ClientSession(slow, Instant.now());
        register(slowSession);
        register(new ClientSession(third, Instant.now()));

        fanout.onEvent(seatHeld());

        verify(first).sendMessage(any(TextMessage.class));
        verify(third).sendMessage(any(TextMessage.class));
        verify(slow).close(CloseStatus.SESSION_NOT_RELIABLE);
        assertThat(delivered()).isEqualTo(2.0);
        assertThat(registry.subscribers(topic)).hasSize(2).doesNotContain(slowSession);
        assertThat(registry.get("lento")).isNull();
    }

    @Test
    void falloInesperadoDeUnSuscriptorNoImpideEnviarAlSiguiente() throws IOException {
        WebSocketSession first = raw("primero");
        WebSocketSession third = raw("tercero");
        ClientSession broken = spy(new ClientSession(raw("roto"), Instant.now()));
        doThrow(new IllegalArgumentException("inesperado")).when(broken).send(anyString());
        register(new ClientSession(first, Instant.now()));
        register(broken);
        register(new ClientSession(third, Instant.now()));

        fanout.onEvent(seatHeld());

        verify(first).sendMessage(any(TextMessage.class));
        verify(third).sendMessage(any(TextMessage.class));
        verify(broken).close(CloseStatus.SESSION_NOT_RELIABLE);
        assertThat(delivered()).isEqualTo(2.0);
        assertThat(registry.subscribers(topic)).hasSize(2).doesNotContain(broken);
    }

    @Test
    void noReenviaElUserIdDeOtrosPasajeros() throws IOException {
        WebSocketSession raw = raw("uno");
        register(new ClientSession(raw, Instant.now()));

        fanout.onEvent(seatHeld());

        var captor = ArgumentCaptor.forClass(TextMessage.class);
        verify(raw).sendMessage(captor.capture());
        assertThat(captor.getValue().getPayload()).contains("\"seat\":\"12\"").doesNotContain("privado");
        assertThat(delivered()).isEqualTo(1.0);
    }
}
