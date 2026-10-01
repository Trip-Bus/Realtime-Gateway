package com.tripbus.realtime.config;

import com.tripbus.realtime.ws.HandshakeAuthInterceptor;
import com.tripbus.realtime.ws.RealtimeHandler;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {

    private final RealtimeHandler handler;
    private final HandshakeAuthInterceptor authInterceptor;
    private final RealtimeProperties properties;

    public WebSocketConfig(RealtimeHandler handler, HandshakeAuthInterceptor authInterceptor,
                           RealtimeProperties properties) {
        this.handler = handler;
        this.authInterceptor = authInterceptor;
        this.properties = properties;
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(handler, "/ws")
                .addInterceptors(authInterceptor)
                .setAllowedOriginPatterns(properties.allowedOrigins().split(","));
    }
}
