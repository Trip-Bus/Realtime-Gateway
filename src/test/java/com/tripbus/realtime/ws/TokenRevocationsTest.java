package com.tripbus.realtime.ws;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class TokenRevocationsTest {

    private final StringRedisTemplate redis = mock(StringRedisTemplate.class);
    private final TokenRevocations revocations = new TokenRevocations(redis);
    private final Logger logger = (Logger) LoggerFactory.getLogger(TokenRevocations.class);
    private final ListAppender<ILoggingEvent> logs = new ListAppender<>();

    @BeforeEach
    void captureLogs() {
        logs.start();
        logger.addAppender(logs);
    }

    @AfterEach
    void releaseLogs() {
        logger.detachAppender(logs);
    }

    @Test
    void tokenConClaveDeRevocacionEstaRevocado() {
        when(redis.hasKey("tripbus:revoked:j1")).thenReturn(true);
        assertThat(revocations.isRevoked("j1")).isTrue();
    }

    @Test
    void tokenSinClaveNoEstaRevocado() {
        when(redis.hasKey(anyString())).thenReturn(false);
        assertThat(revocations.isRevoked("j2")).isFalse();
        verify(redis).hasKey("tripbus:revoked:j2");
    }

    @Test
    void tokenSinJtiNoConsultaRedis() {
        assertThat(revocations.isRevoked(null)).isFalse();
        assertThat(revocations.isRevoked(" ")).isFalse();
        verifyNoInteractions(redis);
    }

    @Test
    void redisCaidoPermiteLaConexionYRegistraWarn() {
        when(redis.hasKey(anyString())).thenThrow(new RedisConnectionFailureException("sin conexión"));

        assertThat(revocations.isRevoked("j3")).isFalse();

        assertThat(logs.list).singleElement().satisfies(event -> {
            assertThat(event.getLevel()).isEqualTo(Level.WARN);
            assertThat(event.getFormattedMessage()).contains("se permite la conexión").doesNotContain("j3");
        });
    }
}
