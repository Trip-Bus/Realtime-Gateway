package com.tripbus.realtime.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RealtimePropertiesTest {

    private static RealtimeProperties properties(int maxSubscriptions, int maxStaffSubscriptions) {
        return new RealtimeProperties(null, null, null, null, null, null, maxSubscriptions, 0, maxStaffSubscriptions,
                null, null, null, null, false);
    }

    @Test
    void operadorYAdminTienenSuPropioLimiteYLosDemasElGeneral() {
        RealtimeProperties properties = properties(5, 8);

        assertThat(properties.maxSubscriptionsFor("OPERADOR")).isEqualTo(8);
        assertThat(properties.maxSubscriptionsFor("ADMIN")).isEqualTo(8);
        assertThat(properties.maxSubscriptionsFor("PASAJERO")).isEqualTo(5);
        assertThat(properties.maxSubscriptionsFor("CONDUCTOR")).isEqualTo(5);
        assertThat(properties.maxSubscriptionsFor(null)).isEqualTo(5);
    }

    @Test
    void sinLimiteDeOperadorUsaElGeneral() {
        RealtimeProperties properties = properties(0, 0);

        assertThat(properties.maxSubscriptions()).isEqualTo(20);
        assertThat(properties.maxStaffSubscriptions()).isEqualTo(20);
        assertThat(properties.maxAnonymousSubscriptions()).isEqualTo(5);
    }
}
