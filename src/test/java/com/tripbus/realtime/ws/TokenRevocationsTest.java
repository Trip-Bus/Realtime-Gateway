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
import org.springframework.data.redis.core.ValueOperations;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
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
    void cuentaConClaveDeSuspensionEstaSuspendida() {
        when(redis.hasKey("tripbus:suspended:u1")).thenReturn(true);
        assertThat(revocations.isSuspended("u1")).isTrue();
    }

    @Test
    void cuentaSinClaveOSinSubNoEstaSuspendida() {
        when(redis.hasKey(anyString())).thenReturn(false);
        assertThat(revocations.isSuspended("u2")).isFalse();
        verify(redis).hasKey("tripbus:suspended:u2");
        assertThat(revocations.isSuspended(null)).isFalse();
        assertThat(revocations.isSuspended(" ")).isFalse();
    }

    @Test
    void redisCaidoAlConsultarLaSuspensionPermiteLaConexionYRegistraWarn() {
        when(redis.hasKey(anyString())).thenThrow(new RedisConnectionFailureException("sin conexión"));

        assertThat(revocations.isSuspended("u3")).isFalse();

        assertThat(logs.list).singleElement().satisfies(event -> {
            assertThat(event.getLevel()).isEqualTo(Level.WARN);
            assertThat(event.getFormattedMessage()).contains("suspensión", "se permite la conexión")
                    .doesNotContain("u3");
        });
    }

    @Test
    void tokenEmitidoAntesDelCambioDeContrasenaQuedaViejoYElDelMismoSegundoNo() {
        ValueOperations<String, String> values = values();
        when(values.get("tripbus:pwd-changed:u1")).thenReturn("1793613600");
        Instant changedAt = Instant.ofEpochSecond(1793613600);

        assertThat(revocations.isPasswordChangedSince("u1", changedAt.minusSeconds(1))).isTrue();
        // El token que entrega identity con el cambio tiene iat en ese mismo segundo.
        assertThat(revocations.isPasswordChangedSince("u1", changedAt.plusMillis(900))).isFalse();
        assertThat(revocations.isPasswordChangedSince("u1", changedAt.plusSeconds(5))).isFalse();
        // Sin iat no hay cómo saber si es posterior al cambio: se rechaza, como en identity.
        assertThat(revocations.isPasswordChangedSince("u1", null)).isTrue();
    }

    @Test
    void sinClaveDeCambioDeContrasenaOSinSubNoQuedaViejo() {
        ValueOperations<String, String> values = values();
        assertThat(revocations.isPasswordChangedSince("u2", Instant.EPOCH)).isFalse();
        verify(values).get("tripbus:pwd-changed:u2");
        assertThat(revocations.isPasswordChangedSince(null, Instant.EPOCH)).isFalse();
        assertThat(revocations.isPasswordChangedSince(" ", Instant.EPOCH)).isFalse();
        verify(redis, times(1)).opsForValue();
    }

    @Test
    void redisCaidoOValorIlegibleEnElCambioDeContrasenaPermiteLaConexionYRegistraWarn() {
        ValueOperations<String, String> values = values();
        when(values.get("tripbus:pwd-changed:u3")).thenThrow(new RedisConnectionFailureException("sin conexión"));
        when(values.get("tripbus:pwd-changed:u4")).thenReturn("ayer");

        assertThat(revocations.isPasswordChangedSince("u3", Instant.EPOCH)).isFalse();
        assertThat(revocations.isPasswordChangedSince("u4", Instant.EPOCH)).isFalse();

        assertThat(logs.list).hasSize(2).allSatisfy(event -> {
            assertThat(event.getLevel()).isEqualTo(Level.WARN);
            assertThat(event.getFormattedMessage()).contains("contraseña", "se permite la conexión")
                    .doesNotContain("u3", "u4", "ayer");
        });
    }

    @SuppressWarnings("unchecked")
    private ValueOperations<String, String> values() {
        ValueOperations<String, String> values = mock(ValueOperations.class);
        when(redis.opsForValue()).thenReturn(values);
        return values;
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
