package com.tripbus.realtime.events;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class ThrottleTest {

    private final Instant t0 = Instant.parse("2026-11-02T10:00:00Z");

    @Test
    void dejaUnoPorClaveDentroDeLaVentanaYOtroAlCumplirse() {
        Throttle throttle = new Throttle(Duration.ofSeconds(1));

        assertThat(throttle.allow("a", t0)).isTrue();
        assertThat(throttle.allow("a", t0.plusMillis(999))).isFalse();
        assertThat(throttle.allow("b", t0.plusMillis(500))).isTrue();
        assertThat(throttle.allow("a", t0.plusSeconds(1))).isTrue();
        // La ventana cuenta desde el último envío permitido, no desde el descartado.
        assertThat(throttle.allow("a", t0.plusMillis(1999))).isFalse();
    }

    @Test
    void olvidaLasClavesVencidasCuandoCreceDemasiado() {
        Throttle throttle = new Throttle(Duration.ofSeconds(3));
        for (int i = 0; i <= Throttle.PRUNE_THRESHOLD; i++) {
            throttle.allow("viaje-" + i, t0);
        }
        throttle.allow("reciente", t0.plusSeconds(2));
        assertThat(throttle.size()).isEqualTo(Throttle.PRUNE_THRESHOLD + 2);

        // Pasada la ventana de las primeras, se borran antes de registrar la nueva; "reciente" sigue.
        assertThat(throttle.allow("nueva", t0.plusSeconds(4))).isTrue();

        assertThat(throttle.size()).isEqualTo(2);
        assertThat(throttle.allow("reciente", t0.plusSeconds(4))).isFalse();
    }
}
