package com.tripbus.realtime.push;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;
import java.util.Map;

/**
 * US-185: qué evento se avisa por Web Push, a quién y con qué texto. El texto es corto, en español con tuteo y
 * sin datos personales (ni nombres, ni documentos, ni montos, ni ids): la app abre el detalle al tocarlo.
 */
final class PushMessages {

    /** Título y texto de la notificación. */
    record Message(String title, String body) {
    }

    /** Quién recibe el aviso: el userId del payload, el conductorId o los operadores de la empresa. */
    enum Recipient {
        PASSENGER, DRIVER, COMPANY_OPERATORS
    }

    record Rule(Recipient recipient, Message message) {
    }

    private static final Map<String, Rule> RULES = Map.of(
            "BookingAffected", passenger("Tu viaje tuvo un cambio",
                    "Abre TripBus para ver qué pasó y elegir qué hacer con tu reserva."),
            "BookingRelocated", passenger("Te reubicamos en otro viaje",
                    "Revisa en TripBus los detalles de tu nueva reserva."),
            "WaitlistSeatOffered", passenger("¡Hay un puesto para ti!",
                    "Se liberó un puesto en la lista de espera. Confírmalo antes de que venza."),
            "OfferAlert", passenger("Nueva oferta en tu ruta",
                    "Entra a TripBus para verla antes de que se agote."),
            "OfferQueueAdmitted", passenger("¡Es tu turno!",
                    "Ya puedes comprar en la oferta. Entra antes de que se acabe tu tiempo."),
            "DriverAssigned", new Rule(Recipient.DRIVER, new Message("Tienes un viaje asignado",
                    "Revisa en la app los detalles de tu próximo viaje.")),
            "DriverAssignmentConflict", new Rule(Recipient.DRIVER, new Message("Hay un conflicto en tu agenda",
                    "Una de tus asignaciones se cruza con otra. Revísala en la app.")),
            "TripAlertRaised", new Rule(Recipient.COMPANY_OPERATORS, new Message("Alerta crítica en un viaje",
                    "Abre el panel de alertas de TripBus para atenderla.")));

    private PushMessages() {
    }

    private static Rule passenger(String title, String body) {
        return new Rule(Recipient.PASSENGER, new Message(title, body));
    }

    /** La regla del evento, o null si no se avisa por push (TripAlertRaised solo con gravedad CRITICA). */
    static Rule ruleFor(String type, JsonNode payload) {
        Rule rule = RULES.get(type);
        if (rule != null && rule.recipient() == Recipient.COMPANY_OPERATORS
                && !"CRITICA".equals(payload.path("gravedad").asText())) {
            return null;
        }
        return rule;
    }

    /** El id del destinatario directo (userId o conductorId), si viene en el payload. */
    static List<String> directRecipient(Rule rule, JsonNode payload) {
        String field = rule.recipient() == Recipient.DRIVER ? "conductorId" : "userId";
        String id = payload.path(field).asText("");
        return id.isBlank() ? List.of() : List.of(id);
    }
}
