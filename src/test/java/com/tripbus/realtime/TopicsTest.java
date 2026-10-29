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
    void laPosicionPorViajeEsPublicaConSnapshot() {
        UUID id = UUID.randomUUID();
        Topics.Parsed parsed = Topics.parse("viaje:" + id + ":posicion");
        assertThat(parsed.kind()).isEqualTo(Topics.Kind.POSICION_VIAJE);
        assertThat(parsed.id()).isEqualTo(id);
        assertThat(parsed.kind().hasSnapshot()).isTrue();
        assertThat(Topics.isPublic("viaje:" + id + ":posicion")).isTrue();
        assertThat(Topics.parse("oferta:" + id + ":posicion")).isNull();
    }

    @Test
    void positionUpdatedVaAlBusDeprecadoYAlViajeYTrackingStatusSoloAlViaje() throws Exception {
        UUID busId = UUID.randomUUID();
        UUID itineraryId = UUID.randomUUID();
        var pos = mapper.readTree("{\"busId\":\"" + busId + "\",\"itineraryId\":\""
                + itineraryId.toString().toUpperCase() + "\"}");
        assertThat(Topics.forEvent("PositionUpdated", pos))
                .containsExactly("bus:" + busId + ":posicion", "viaje:" + itineraryId + ":posicion");
        var soloBus = mapper.readTree("{\"busId\":\"" + busId + "\"}");
        assertThat(Topics.forEvent("PositionUpdated", soloBus)).containsExactly("bus:" + busId + ":posicion");
        var estado = mapper.readTree("{\"itineraryId\":\"" + itineraryId + "\",\"estadoRastreo\":\"SIN_SENAL\"}");
        assertThat(Topics.forEvent("TrackingStatusChanged", estado)).containsExactly("viaje:" + itineraryId + ":posicion");
        assertThat(Topics.forEvent("TrackingStatusChanged", soloBus)).isEmpty();
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
    void ticketIssuedVaAlTemaPrivadoDelCompradorYASusAvisos() throws Exception {
        UUID userId = UUID.randomUUID();
        var ticket = mapper.readTree("{\"userId\":\"" + userId.toString().toUpperCase() + "\",\"ticketId\":\""
                + UUID.randomUUID() + "\",\"offerId\":\"" + UUID.randomUUID() + "\"}");
        assertThat(Topics.forEvent("TicketIssued", ticket))
                .containsExactly("usuario:" + userId + ":tiquetes", "usuario:" + userId + ":avisos");
        assertThat(Topics.forEvent("TicketIssued", mapper.readTree("{\"ticketId\":\"x\"}"))).isEmpty();
    }

    @Test
    void losTemasPrivadosNuevosSonValidosPrivadosYSinSnapshot() {
        UUID id = UUID.randomUUID();
        assertThat(Topics.parse("usuario:" + id + ":avisos").kind()).isEqualTo(Topics.Kind.AVISOS);
        assertThat(Topics.parse("conductor:" + id + ":viaje").kind()).isEqualTo(Topics.Kind.VIAJE_CONDUCTOR);
        assertThat(Topics.parse("empresa:" + id + ":alertas").kind()).isEqualTo(Topics.Kind.ALERTAS);
        for (String topic : new String[] {"usuario:" + id + ":avisos", "conductor:" + id + ":viaje",
                "empresa:" + id + ":alertas"}) {
            assertThat(Topics.isPublic(topic)).as(topic).isFalse();
            assertThat(Topics.parse(topic).kind().hasSnapshot()).as(topic).isFalse();
        }
        assertThat(Topics.parse("empresa:" + id + ":avisos")).isNull();
        assertThat(Topics.parse("conductor:" + id + ":alertas")).isNull();
        assertThat(Topics.parse("usuario:" + id + ":viaje")).isNull();
    }

    @Test
    void autorizaCadaTemaPrivadoSegunDuenoRolYEmpresa() {
        String user = UUID.randomUUID().toString();
        String company = UUID.randomUUID().toString();
        String other = UUID.randomUUID().toString();
        Topics.Parsed avisos = Topics.parse("usuario:" + user + ":avisos");
        Topics.Parsed tiquetes = Topics.parse("usuario:" + user + ":tiquetes");
        Topics.Parsed conductor = Topics.parse("conductor:" + user + ":viaje");
        Topics.Parsed alertas = Topics.parse("empresa:" + company + ":alertas");

        assertThat(Topics.canSubscribe(avisos, user, "PASAJERO", null)).isTrue();
        assertThat(Topics.canSubscribe(tiquetes, user, "PASAJERO", null)).isTrue();
        assertThat(Topics.canSubscribe(avisos, other, "ADMIN", null)).isFalse();
        assertThat(Topics.canSubscribe(avisos, null, null, null)).isFalse();

        assertThat(Topics.canSubscribe(conductor, user, "CONDUCTOR", null)).isTrue();
        assertThat(Topics.canSubscribe(conductor, user, "PASAJERO", null)).isFalse();
        assertThat(Topics.canSubscribe(conductor, other, "CONDUCTOR", null)).isFalse();
        assertThat(Topics.canSubscribe(conductor, other, "ADMIN", null)).isFalse();

        assertThat(Topics.canSubscribe(alertas, other, "OPERADOR", company.toUpperCase())).isTrue();
        assertThat(Topics.canSubscribe(alertas, other, "OPERADOR", other)).isFalse();
        assertThat(Topics.canSubscribe(alertas, other, "OPERADOR", null)).isFalse();
        assertThat(Topics.canSubscribe(alertas, other, "OPERADOR", "no-es-uuid")).isFalse();
        assertThat(Topics.canSubscribe(alertas, other, "ADMIN", null)).isTrue();
        assertThat(Topics.canSubscribe(alertas, other, "PASAJERO", company)).isFalse();
        assertThat(Topics.canSubscribe(alertas, null, "ADMIN", null)).isFalse();

        assertThat(Topics.canSubscribe(Topics.parse("viaje:" + other + ":posicion"), null, null, null)).isTrue();
        assertThat(Topics.canSubscribe(null, user, "ADMIN", null)).isFalse();
    }

    @Test
    void enrutaLosAvisosDelPasajeroPorUserId() throws Exception {
        UUID userId = UUID.randomUUID();
        var conUsuario = mapper.readTree("{\"userId\":\"" + userId + "\",\"bookingId\":\"b\"}");
        var sinUsuario = mapper.readTree("{\"bookingId\":\"b\"}");
        for (String type : new String[] {"BookingAffected", "BookingRelocated", "RefundProcessed",
                "TicketRescheduled", "TicketUpdated"}) {
            assertThat(Topics.forEvent(type, conUsuario)).as(type).containsExactly("usuario:" + userId + ":avisos");
            assertThat(Topics.forEvent(type, sinUsuario)).as(type).isEmpty();
            assertThat(Topics.isUserNotice(type)).as(type).isTrue();
        }
        assertThat(Topics.isUserNotice("TicketIssued")).isTrue();
        assertThat(Topics.isUserNotice("SeatHeld")).isFalse();
    }

    @Test
    void enrutaLosEventosDelConductorYDeLaEmpresa() throws Exception {
        UUID conductor = UUID.randomUUID();
        UUID company = UUID.randomUUID();
        UUID itinerary = UUID.randomUUID();
        var asignacion = mapper.readTree("{\"conductorId\":\"" + conductor + "\",\"companyId\":\"" + company
                + "\",\"itineraryId\":\"" + itinerary + "\"}");
        String canal = "conductor:" + conductor + ":viaje";
        String alertas = "empresa:" + company + ":alertas";
        assertThat(Topics.forEvent("DriverAssigned", asignacion)).containsExactly(canal);
        assertThat(Topics.forEvent("DriverUnassigned", asignacion)).containsExactly(canal);
        assertThat(Topics.forEvent("DriverAssignmentConflict", asignacion)).containsExactly(canal, alertas);
        for (String type : new String[] {"TripAlertRaised", "TripAlertResolved", "TripInterrupted"}) {
            assertThat(Topics.forEvent(type, asignacion)).as(type).containsExactly(alertas);
        }
        assertThat(Topics.forEvent("TripAlertRaised", mapper.readTree("{\"itineraryId\":\"" + itinerary + "\"}")))
                .isEmpty();
    }

    @Test
    void trackingStatusSinSenalAvisaALaEmpresa() throws Exception {
        UUID company = UUID.randomUUID();
        UUID itinerary = UUID.randomUUID();
        String base = "{\"itineraryId\":\"" + itinerary + "\",\"companyId\":\"" + company + "\",";
        String posicion = "viaje:" + itinerary + ":posicion";
        String alertas = "empresa:" + company + ":alertas";
        assertThat(Topics.forEvent("TrackingStatusChanged", mapper.readTree(base + "\"estado\":\"SIN_SENAL\"}")))
                .containsExactly(posicion, alertas);
        assertThat(Topics.forEvent("TrackingStatusChanged", mapper.readTree(base + "\"estadoRastreo\":\"SIN_SENAL\"}")))
                .containsExactly(posicion, alertas);
        assertThat(Topics.forEvent("TrackingStatusChanged", mapper.readTree(base + "\"estado\":\"ACTIVO\"}")))
                .containsExactly(posicion);
    }

    @Test
    void etaYReprogramacionVanALosTemasPublicosDelViajeYElSosNunca() throws Exception {
        UUID itinerary = UUID.randomUUID();
        var payload = mapper.readTree("{\"itineraryId\":\"" + itinerary + "\",\"tipo\":\"AVERIA\"}");
        assertThat(Topics.forEvent("EtaUpdated", payload)).containsExactly("viaje:" + itinerary + ":posicion");
        assertThat(Topics.forEvent("ItineraryRescheduled", payload)).containsExactly("viaje:" + itinerary + ":eventos");
        assertThat(Topics.forEvent("TripEventReported", payload)).containsExactly("viaje:" + itinerary + ":eventos");
        var sos = mapper.readTree("{\"itineraryId\":\"" + itinerary + "\",\"tipo\":\"SOS\"}");
        assertThat(Topics.forEvent("TripEventReported", sos)).isEmpty();
    }
}
