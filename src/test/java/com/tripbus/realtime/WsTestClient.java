package com.tripbus.realtime;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketHttpHeaders;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Cliente WebSocket de las pruebas de integración: su propia cola de mensajes y su cierre. */
public final class WsTestClient {

    public final BlockingQueue<JsonNode> received = new LinkedBlockingQueue<>();
    public final CompletableFuture<CloseStatus> closed = new CompletableFuture<>();
    public final WebSocketSession ws;

    public WsTestClient(int port, ObjectMapper mapper) throws Exception {
        this(port, mapper, new WebSocketHttpHeaders());
    }

    public WsTestClient(int port, ObjectMapper mapper, WebSocketHttpHeaders headers) throws Exception {
        ws = new StandardWebSocketClient().execute(new TextWebSocketHandler() {
            @Override
            protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
                received.add(mapper.readTree(message.getPayload()));
            }

            @Override
            public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
                closed.complete(status);
            }
        }, headers, URI.create("ws://localhost:" + port + "/ws")).get(5, TimeUnit.SECONDS);
    }

    public void send(String json) throws Exception {
        ws.sendMessage(new TextMessage(json));
    }

    /** El siguiente mensaje de ese tipo (descarta los demás). */
    public JsonNode next(String type) throws InterruptedException {
        while (true) {
            JsonNode msg = received.poll(5, TimeUnit.SECONDS);
            assertThat(msg).as("esperando mensaje " + type).isNotNull();
            if (type.equals(msg.path("type").asText())) {
                return msg;
            }
        }
    }

    /** Todos los mensajes (sin ping) hasta el primero de tipo {@code last}, incluido. */
    public List<JsonNode> until(String last) throws InterruptedException {
        List<JsonNode> messages = new ArrayList<>();
        while (true) {
            JsonNode msg = received.poll(5, TimeUnit.SECONDS);
            assertThat(msg).as("esperando mensaje " + last + "; llegaron " + messages).isNotNull();
            if (!"ping".equals(msg.path("type").asText())) {
                messages.add(msg);
            }
            if (last.equals(msg.path("type").asText())) {
                return messages;
            }
        }
    }

    public CloseStatus awaitClose() throws Exception {
        return closed.get(5, TimeUnit.SECONDS);
    }
}
