package com.tripbus.realtime.ws;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Tópicos del doc:
 * <ul>
 *   <li>{@code itinerario:{id}:asientos} — cambios de estado de asientos</li>
 *   <li>{@code oferta:{id}:cupo} — cupo restante de una oferta</li>
 *   <li>{@code viaje:{id}:eventos} — novedades del viaje</li>
 *   <li>{@code bus:{id}:posicion} — posición GPS del bus</li>
 * </ul>
 */
public final class Topics {

    private static final String UUID_RE = "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}";
    private static final Pattern TOPIC = Pattern.compile("^(itinerario|oferta|viaje|bus):(" + UUID_RE + "):(asientos|cupo|eventos|posicion)$");

    public enum Kind { ASIENTOS, CUPO, EVENTOS, POSICION }

    public record Parsed(Kind kind, UUID id) {
    }

    /**
     * Tipos de tópico que puede seguir una sesión anónima (US-039, US-047, US-056): quien espera
     * el bus en la vía no tiene cuenta. Sus payloads no llevan datos personales (EventFanout
     * quita los identificadores de pasajeros y de reservas).
     */
    private static final Set<Kind> PUBLIC_KINDS = EnumSet.of(Kind.POSICION, Kind.EVENTOS, Kind.CUPO, Kind.ASIENTOS);

    private Topics() {
    }

    /** Devuelve null si el tópico no es válido. */
    public static Parsed parse(String topic) {
        if (topic == null) {
            return null;
        }
        Matcher m = TOPIC.matcher(topic);
        if (!m.matches()) {
            return null;
        }
        String entity = m.group(1);
        String suffix = m.group(3);
        Kind kind = switch (entity + ":" + suffix) {
            case "itinerario:asientos" -> Kind.ASIENTOS;
            case "oferta:cupo" -> Kind.CUPO;
            case "viaje:eventos" -> Kind.EVENTOS;
            case "bus:posicion" -> Kind.POSICION;
            default -> null;
        };
        return kind == null ? null : new Parsed(kind, UUID.fromString(m.group(2)));
    }

    /** true si el tópico es válido y lo puede seguir una sesión anónima. */
    public static boolean isPublic(String topic) {
        Parsed parsed = parse(topic);
        return parsed != null && PUBLIC_KINDS.contains(parsed.kind());
    }

    /** Tópicos a los que se difunde un evento de dominio. */
    public static List<String> forEvent(String type, JsonNode payload) {
        return switch (type) {
            case "SeatHeld", "SeatReleased", "SeatSold", "HoldExpired", "SeatStateChanged" ->
                    single("itinerario:", payload, "itineraryId", ":asientos");
            case "OfferQuotaUpdated", "OfferClosed", "OfferSoldOut", "OfferActivated" ->
                    single("oferta:", payload, "offerId", ":cupo");
            case "TripEventReported" -> single("viaje:", payload, "itineraryId", ":eventos");
            case "PositionUpdated" -> single("bus:", payload, "busId", ":posicion");
            default -> List.of();
        };
    }

    private static List<String> single(String prefix, JsonNode payload, String field, String suffix) {
        String id = payload.path(field).asText(null);
        return id == null || id.isBlank() ? List.of() : List.of(prefix + id.toLowerCase() + suffix);
    }
}
