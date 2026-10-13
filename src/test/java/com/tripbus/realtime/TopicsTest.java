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

    @Test
    void losCuatroTiposDeTopicoSonPublicosYLosInvalidosNo() {
        UUID id = UUID.randomUUID();
        assertThat(Topics.isPublic("bus:" + id + ":posicion")).isTrue();
        assertThat(Topics.isPublic("viaje:" + id + ":eventos")).isTrue();
        assertThat(Topics.isPublic("oferta:" + id + ":cupo")).isTrue();
        assertThat(Topics.isPublic("itinerario:" + id + ":asientos")).isTrue();
        assertThat(Topics.isPublic("bus:" + id + ":asientos")).isFalse();
        assertThat(Topics.isPublic(null)).isFalse();
    }

    @Test
    void elTemaDeTiquetesDelUsuarioEsPrivadoYSinSnapshot() {
        UUID id = UUID.randomUUID();
        Topics.Parsed parsed = Topics.parse("usuario:" + id + ":tiquetes");
        assertThat(parsed.kind()).isEqualTo(Topics.Kind.TIQUETES);
        assertThat(parsed.id()).isEqualTo(id);
        assertThat(parsed.kind().hasSnapshot()).isFalse();
        assertThat(Topics.Kind.POSICION.hasSnapshot()).isTrue();
        assertThat(Topics.isPublic("usuario:" + id + ":tiquetes")).isFalse();
        assertThat(Topics.parse("usuario:" + id + ":posicion")).isNull();
        assertThat(Topics.parse("bus:" + id + ":tiquetes")).isNull();
        assertThat(Topics.parse("usuario:no-es-uuid:tiquetes")).isNull();
    }

    @Test
    void soloElDuenoPuedeSeguirSuTemaDeTiquetes() {
        UUID id = UUID.randomUUID();
        Topics.Parsed parsed = Topics.parse("usuario:" + id + ":tiquetes");
        assertThat(Topics.isOwnedBy(parsed, id.toString())).isTrue();
        assertThat(Topics.isOwnedBy(parsed, id.toString().toUpperCase())).isTrue();
        assertThat(Topics.isOwnedBy(parsed, UUID.randomUUID().toString())).isFalse();
        assertThat(Topics.isOwnedBy(parsed, null)).isFalse();
        assertThat(Topics.isOwnedBy(parsed, "no-es-uuid")).isFalse();
        assertThat(Topics.isOwnedBy(null, id.toString())).isFalse();
    }

    @Test
    void ticketIssuedVaAlTemaPrivadoDelComprador() throws Exception {
        UUID userId = UUID.randomUUID();
        var ticket = mapper.readTree("{\"userId\":\"" + userId.toString().toUpperCase() + "\",\"ticketId\":\""
                + UUID.randomUUID() + "\",\"offerId\":\"" + UUID.randomUUID() + "\"}");
        assertThat(Topics.forEvent("TicketIssued", ticket)).containsExactly("usuario:" + userId + ":tiquetes");
        assertThat(Topics.forEvent("TicketIssued", mapper.readTree("{\"ticketId\":\"x\"}"))).isEmpty();
    }
}
