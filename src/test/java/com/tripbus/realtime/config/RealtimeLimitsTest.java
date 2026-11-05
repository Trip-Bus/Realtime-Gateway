package com.tripbus.realtime.config;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class RealtimeLimitsTest {

    @Test
    void valoresPorDefecto() {
        RealtimeLimits limits = RealtimeLimits.defaults();
        assertThat(limits.maxConnectionsPerIp()).isEqualTo(20);
        assertThat(limits.maxConnectionsPerUser()).isEqualTo(5);
        assertThat(limits.maxClientMessages()).isEqualTo(30);
        assertThat(limits.clientMessageWindow()).isEqualTo(Duration.ofSeconds(10));
        assertThat(limits.presenceTtl()).isEqualTo(Duration.ofSeconds(60));
        assertThat(limits.clientIpHeader()).isEmpty();
        assertThat(limits.replay().size()).isEqualTo(200);
        assertThat(limits.replay().ttl()).isEqualTo(Duration.ofMinutes(10));
    }

    @Test
    void respetaLosValoresConfigurados() {
        RealtimeLimits limits = new RealtimeLimits(3, 2, 7, Duration.ofSeconds(1), Duration.ofSeconds(5),
                " X-Real-IP ", new RealtimeLimits.Replay(50, Duration.ofMinutes(1)));
        assertThat(limits.maxConnectionsPerIp()).isEqualTo(3);
        assertThat(limits.maxConnectionsPerUser()).isEqualTo(2);
        assertThat(limits.maxClientMessages()).isEqualTo(7);
        assertThat(limits.clientMessageWindow()).isEqualTo(Duration.ofSeconds(1));
        assertThat(limits.presenceTtl()).isEqualTo(Duration.ofSeconds(5));
        assertThat(limits.clientIpHeader()).isEqualTo("X-Real-IP");
        assertThat(limits.replay().size()).isEqualTo(50);
        assertThat(limits.replay().ttl()).isEqualTo(Duration.ofMinutes(1));
    }

    @Test
    void pushApagadoSiFaltaAlgunaLlave() {
        PushProperties defaults = new PushProperties(null, null, null, 0, false, null);
        assertThat(defaults.configured()).isFalse();
        assertThat(defaults.maxSubscriptionsPerUser()).isEqualTo(5);
        assertThat(defaults.ttl()).isEqualTo(Duration.ofHours(1));
        assertThat(new PushProperties("pub", "priv", " ", 0, false, null).configured()).isFalse();
        assertThat(new PushProperties("pub", "", "mailto:a@b.co", 0, false, null).configured()).isFalse();
        assertThat(new PushProperties("", "priv", "mailto:a@b.co", 0, false, null).configured()).isFalse();
        PushProperties push = new PushProperties("pub", "priv", "mailto:a@b.co", 2, true, Duration.ofMinutes(5));
        assertThat(push.configured()).isTrue();
        assertThat(push.maxSubscriptionsPerUser()).isEqualTo(2);
        assertThat(push.ttl()).isEqualTo(Duration.ofMinutes(5));
    }
}
