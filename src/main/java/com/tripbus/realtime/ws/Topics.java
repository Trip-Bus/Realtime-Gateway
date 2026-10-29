package com.tripbus.realtime.ws;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Tópicos del doc:
 * <ul>
 *   <li>{@code itinerario:{id}:asientos} — cambios de estado de asientos</li>
 *   <li>{@code oferta:{id}:cupo} — cupo restante de una oferta</li>
 *   <li>{@code viaje:{id}:eventos} — novedades del viaje y reprogramaciones</li>
 *   <li>{@code viaje:{id}:posicion} — posición GPS, estado de rastreo y ETA del bus que cubre el viaje
 *       (US-173/US-174); {@code id} es el itinerario</li>
 *   <li>{@code bus:{id}:posicion} — DEPRECADO: posición GPS del bus; se mantiene para el frontend
 *       que aún lo sigue. Usar {@code viaje:{id}:posicion}</li>
 *   <li>{@code usuario:{id}:tiquetes} — PRIVADO: aviso de tiquete emitido (US-033); solo lo sigue la
 *       sesión autenticada de ese usuario</li>
 *   <li>{@code usuario:{id}:avisos} — PRIVADO: avisos de sus reservas (US-183); solo ese usuario</li>
 *   <li>{@code conductor:{id}:viaje} — PRIVADO: asignaciones del conductor (US-186); solo ese
 *       usuario y con rol CONDUCTOR</li>
 *   <li>{@code empresa:{id}:alertas} — PRIVADO: alertas de los viajes de la empresa; OPERADOR de esa
 *       empresa (claim companyId) o ADMIN</li>
 * </ul>
 */
public final class Topics {

    private static final String UUID_RE = "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}";
    private static final Pattern TOPIC = Pattern.compile("^(itinerario|oferta|viaje|bus|usuario|conductor|empresa):("
            + UUID_RE + "):(asientos|cupo|eventos|posicion|tiquetes|avisos|viaje|alertas)$");

    private static final String USUARIO = "usuario:";
    private static final String VIAJE = "viaje:";
    private static final String CONDUCTOR = "conductor:";
    private static final String EMPRESA = "empresa:";
    private static final String ITINERARY_ID = "itineraryId";
    private static final String USER_ID = "userId";
    private static final String COMPANY_ID = "companyId";
    private static final String CONDUCTOR_ID = "conductorId";
    private static final String TO_POSICION = ":posicion";
    private static final String TO_EVENTOS = ":eventos";
    private static final String TO_AVISOS = ":avisos";
    private static final String TO_VIAJE = ":viaje";
    private static final String TO_ALERTAS = ":alertas";

    public enum Kind {
        ASIENTOS, CUPO, EVENTOS,
        /** bus:{id}:posicion (deprecado). */
        POSICION,
        /** viaje:{id}:posicion: posición, estado de rastreo y ETA por itinerario. */
        POSICION_VIAJE,
        /** Sin snapshot: el aviso solo trae ids y la app pide el tiquete por GET /tickets/{id}. */
        TIQUETES(false),
        /** usuario:{id}:avisos. Sin snapshot: la app pide GET /bookings/affected al suscribirse. */
        AVISOS(false),
        /** conductor:{id}:viaje. Sin snapshot: la app pide sus viajes asignados a fleet. */
        VIAJE_CONDUCTOR(false),
        /** empresa:{id}:alertas. Sin snapshot: la app pide GET /alerts?estado=ABIERTA al suscribirse. */
        ALERTAS(false);

        private final boolean snapshot;

        Kind() {
            this(true);
        }

        Kind(boolean snapshot) {
            this.snapshot = snapshot;
        }

        public boolean hasSnapshot() {
            return snapshot;
        }
    }

    public record Parsed(Kind kind, UUID id) {
    }

    /**
     * Tipos de tópico que puede seguir una sesión anónima (US-039, US-047, US-056): quien espera
     * el bus en la vía no tiene cuenta. Sus payloads no llevan datos personales (EventFanout
     * quita los identificadores de pasajeros, conductores y reservas).
     */
    private static final Set<Kind> PUBLIC_KINDS = EnumSet.of(Kind.POSICION, Kind.POSICION_VIAJE, Kind.EVENTOS, Kind.CUPO, Kind.ASIENTOS);

    /** Avisos al pasajero (US-033, US-183): se enrutan por el userId del payload; sin él no se difunden. */
    private static final Set<String> USER_NOTICES = Set.of("TicketIssued", "BookingAffected", "BookingRelocated",
            "RefundProcessed", "TicketRescheduled", "TicketUpdated");

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
            case "viaje:posicion" -> Kind.POSICION_VIAJE;
            case "usuario:tiquetes" -> Kind.TIQUETES;
            case "usuario:avisos" -> Kind.AVISOS;
            case "conductor:viaje" -> Kind.VIAJE_CONDUCTOR;
            case "empresa:alertas" -> Kind.ALERTAS;
            default -> null;
        };
        return kind == null ? null : new Parsed(kind, UUID.fromString(m.group(2)));
    }

    /** true si el tópico es válido y lo puede seguir una sesión anónima. */
    public static boolean isPublic(String topic) {
        Parsed parsed = parse(topic);
        return parsed != null && PUBLIC_KINDS.contains(parsed.kind());
    }

    /**
     * true si el {uuid} del tópico es el {@code userId} de la sesión (sin importar mayúsculas).
     * Anónimo (userId null) ⇒ false.
     */
    public static boolean isOwnedBy(Parsed parsed, String userId) {
        return parsed != null && sameId(parsed.id(), userId);
    }

    /**
     * Autorización de un tópico para una sesión con {@code userId}, {@code role} y {@code companyId}
     * (claims del JWT; null en una anónima). Los públicos los sigue cualquiera; los privados:
     * <ul>
     *   <li>{@code usuario:{uuid}:tiquetes|avisos}: solo el usuario {uuid}, sin excepción por rol (ni ADMIN)</li>
     *   <li>{@code conductor:{uuid}:viaje}: solo el usuario {uuid} y con rol CONDUCTOR</li>
     *   <li>{@code empresa:{uuid}:alertas}: OPERADOR cuyo companyId es {uuid}, o ADMIN para cualquiera</li>
     * </ul>
     */
    public static boolean canSubscribe(Parsed parsed, String userId, String role, String companyId) {
        if (parsed == null) {
            return false;
        }
        return switch (parsed.kind()) {
            case TIQUETES, AVISOS -> isOwnedBy(parsed, userId);
            case VIAJE_CONDUCTOR -> "CONDUCTOR".equals(role) && isOwnedBy(parsed, userId);
            case ALERTAS -> userId != null
                    && ("ADMIN".equals(role) || ("OPERADOR".equals(role) && sameId(parsed.id(), companyId)));
            default -> true;
        };
    }

    private static boolean sameId(UUID id, String value) {
        if (value == null) {
            return false;
        }
        try {
            return id.equals(UUID.fromString(value));
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    /** true si el evento es un aviso al pasajero, que solo se difunde con el userId del payload. */
    public static boolean isUserNotice(String type) {
        return USER_NOTICES.contains(type);
    }

    /** Tópicos a los que se difunde un evento de dominio. */
    public static List<String> forEvent(String type, JsonNode payload) {
        return switch (type) {
            case "SeatHeld", "SeatReleased", "SeatSold", "HoldExpired", "SeatStateChanged" ->
                    single("itinerario:", payload, ITINERARY_ID, ":asientos");
            case "OfferQuotaUpdated", "OfferClosed", "OfferSoldOut", "OfferActivated" ->
                    single("oferta:", payload, "offerId", ":cupo");
            // El SOS nunca va a la línea de tiempo pública (fleet no lo publica; aquí, por defensa).
            case "TripEventReported" -> "SOS".equals(payload.path("tipo").asText())
                    ? List.of() : single(VIAJE, payload, ITINERARY_ID, TO_EVENTOS);
            case "ItineraryRescheduled" -> single(VIAJE, payload, ITINERARY_ID, TO_EVENTOS);
            // bus:{id}:posicion queda deprecado, pero se sigue difundiendo para no romper al frontend actual.
            case "PositionUpdated" -> concat(
                    single("bus:", payload, "busId", TO_POSICION),
                    single(VIAJE, payload, ITINERARY_ID, TO_POSICION));
            case "EtaUpdated" -> single(VIAJE, payload, ITINERARY_ID, TO_POSICION);
            // estado (o estadoRastreo): ACTIVO, EN_PAUSA, SIN_SENAL o FINALIZADO. SIN_SENAL avisa además a la empresa.
            case "TrackingStatusChanged" -> concat(
                    single(VIAJE, payload, ITINERARY_ID, TO_POSICION),
                    isSinSenal(payload) ? single(EMPRESA, payload, COMPANY_ID, TO_ALERTAS) : List.of());
            case "TicketIssued" -> concat(
                    single(USUARIO, payload, USER_ID, ":tiquetes"),
                    single(USUARIO, payload, USER_ID, TO_AVISOS));
            case "BookingAffected", "BookingRelocated", "RefundProcessed", "TicketRescheduled", "TicketUpdated" ->
                    single(USUARIO, payload, USER_ID, TO_AVISOS);
            case "DriverAssigned", "DriverUnassigned" -> single(CONDUCTOR, payload, CONDUCTOR_ID, TO_VIAJE);
            case "DriverAssignmentConflict" -> concat(
                    single(CONDUCTOR, payload, CONDUCTOR_ID, TO_VIAJE),
                    single(EMPRESA, payload, COMPANY_ID, TO_ALERTAS));
            // Nunca a un tópico público: el SOS y sus datos solo los ve la empresa.
            case "TripAlertRaised", "TripAlertResolved", "TripInterrupted" ->
                    single(EMPRESA, payload, COMPANY_ID, TO_ALERTAS);
            default -> List.of();
        };
    }

    private static boolean isSinSenal(JsonNode payload) {
        return "SIN_SENAL".equals(payload.path("estado").asText(payload.path("estadoRastreo").asText()));
    }

    private static List<String> concat(List<String> first, List<String> second) {
        return Stream.concat(first.stream(), second.stream()).toList();
    }

    private static List<String> single(String prefix, JsonNode payload, String field, String suffix) {
        String id = payload.path(field).asText(null);
        return id == null || id.isBlank() ? List.of() : List.of(prefix + id.toLowerCase() + suffix);
    }
}
