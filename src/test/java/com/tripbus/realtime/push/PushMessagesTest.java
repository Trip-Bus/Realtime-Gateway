package com.tripbus.realtime.push;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/** US-185: qué eventos se avisan, a quién, y que el texto sea corto, en español con tuteo y sin datos personales. */
class PushMessagesTest {

    private static final List<String> PUSH_EVENTS = List.of("BookingAffected", "BookingRelocated",
            "WaitlistSeatOffered", "OfferAlert", "OfferQueueAdmitted", "DriverAssigned", "DriverAssignmentConflict");
    /** Un uuid, un correo o un número largo (documento, monto) no deben aparecer nunca. */
    private static final Pattern PERSONAL = Pattern.compile("[0-9a-f]{8}-|@|\\d{4,}|\\$");

    private final ObjectMapper mapper = new ObjectMapper();

    private JsonNode json(String value) throws Exception {
        return mapper.readTree(value);
    }

    @Test
    void cadaEventoTieneTituloYTextoCortosSinDatosPersonales() throws Exception {
        JsonNode payload = json("{\"userId\":\"6f1c2a1e-0000-4000-8000-000000000001\",\"gravedad\":\"CRITICA\"}");
        for (String type : PUSH_EVENTS) {
            PushMessages.Rule rule = PushMessages.ruleFor(type, payload);
            assertThat(rule).as(type).isNotNull();
            assertThat(rule.message().title()).hasSizeBetween(5, 40).doesNotContainPattern(PERSONAL);
            assertThat(rule.message().body()).hasSizeBetween(10, 120).doesNotContainPattern(PERSONAL);
        }
        // Tuteo: "tu", "tienes", "puedes", "revisa"... nunca "usted" ni "su reserva".
        assertThat(PushMessages.ruleFor("BookingAffected", payload).message().title()).startsWith("Tu viaje");
        PUSH_EVENTS.forEach(type -> assertThat(PushMessages.ruleFor(type, payload).message().body())
                .doesNotContainIgnoringCase("usted").doesNotContain("su reserva"));
    }

    @Test
    void elDestinatarioSaleDelUserIdDelConductorIdOLosOperadores() throws Exception {
        JsonNode payload = json("{\"userId\":\"u-1\",\"conductorId\":\"c-1\",\"companyId\":\"e-1\","
                + "\"gravedad\":\"CRITICA\"}");

        assertThat(PushMessages.directRecipient(PushMessages.ruleFor("OfferAlert", payload), payload))
                .containsExactly("u-1");
        assertThat(PushMessages.directRecipient(PushMessages.ruleFor("DriverAssigned", payload), payload))
                .containsExactly("c-1");
        assertThat(PushMessages.ruleFor("TripAlertRaised", payload).recipient())
                .isEqualTo(PushMessages.Recipient.COMPANY_OPERATORS);
        assertThat(PushMessages.directRecipient(PushMessages.ruleFor("BookingRelocated", json("{}")), json("{}")))
                .isEmpty();
    }

    @Test
    void soloLasAlertasCriticasYLosEventosDeLaListaSeAvisan() throws Exception {
        assertThat(PushMessages.ruleFor("TripAlertRaised", json("{\"gravedad\":\"ALTA\"}"))).isNull();
        assertThat(PushMessages.ruleFor("TripAlertRaised", json("{}"))).isNull();
        assertThat(PushMessages.ruleFor("RefundProcessed", json("{}"))).isNull();
        assertThat(PushMessages.ruleFor("SeatHeld", json("{}"))).isNull();
    }
}
