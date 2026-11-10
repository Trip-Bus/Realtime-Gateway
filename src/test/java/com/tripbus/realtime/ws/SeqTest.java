package com.tripbus.realtime.ws;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SeqTest {

    @Test
    void validaLaFormaYQueLosNumerosQuepanEnUnLong() {
        assertThat(Seq.isValid("1760280000000-3")).isTrue();
        assertThat(Seq.isValid("0-0")).isTrue();
        assertThat(Seq.isValid(null)).isFalse();
        assertThat(Seq.isValid("")).isFalse();
        assertThat(Seq.isValid("abc-1")).isFalse();
        assertThat(Seq.isValid("12")).isFalse();
        assertThat(Seq.isValid("-1-2")).isFalse();
        // 19 dígitos que no caben en un long.
        assertThat(Seq.isValid("9999999999999999999-0")).isFalse();
    }

    @Test
    void comparaNumericamenteYNoComoTexto() {
        assertThat(Seq.compare("10-0", "9-5")).isPositive();
        assertThat(Seq.compare("5-10", "5-9")).isPositive();
        assertThat(Seq.compare("5-1", "5-1")).isZero();
        assertThat(Seq.compare("4-99", "5-0")).isNegative();
        assertThat(Seq.millis("1760280000000-3")).isEqualTo(1760280000000L);
    }
}
