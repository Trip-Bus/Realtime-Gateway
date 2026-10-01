package com.tripbus.realtime;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tripbus.realtime.ws.Topics;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class TopicsTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void validaLosCuatroTiposDeTopicoDelDoc() {
        UUID id = UUID.randomUUID();
        assertThat(Topics.parse("itinerario:" + id + ":asientos").kind()).isEqualTo(Topics.Kind.ASIENTOS);
        assertThat(Topics.parse("oferta:" + id + ":cupo").kind()).isEqualTo(Topics.Kind.CUPO);
        assertThat(Topics.parse("viaje:" + id + ":eventos").kind()).isEqualTo(Topics.Kind.EVENTOS);
        assertThat(Topics.parse("bus:" + id + ":posicion").kind()).isEqualTo(Topics.Kind.POSICION);
        assertThat(Topics.parse("bus:" + id + ":asientos")).isNull();
        assertThat(Topics.parse("itinerario:no-es-uuid:asientos")).isNull();
    }

    @Test
    void mapeaEventosDeDominioATopicos() throws Exception {
        UUID id = UUID.randomUUID();
        var seat = mapper.readTree("{\"itineraryId\":\"" + id + "\"}");
        assertThat(Topics.forEvent("SeatHeld", seat)).containsExactly("itinerario:" + id + ":asientos");
        var offer = mapper.readTree("{\"offerId\":\"" + id + "\"}");
        assertThat(Topics.forEvent("OfferQuotaUpdated", offer)).containsExactly("oferta:" + id + ":cupo");
        var pos = mapper.readTree("{\"busId\":\"" + id + "\"}");
        assertThat(Topics.forEvent("PositionUpdated", pos)).containsExactly("bus:" + id + ":posicion");
        assertThat(Topics.forEvent("BookingCreated", seat)).isEmpty();
    }
}
