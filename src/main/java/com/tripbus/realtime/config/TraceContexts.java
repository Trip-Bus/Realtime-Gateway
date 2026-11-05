package com.tripbus.realtime.config;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.regex.Pattern;

/**
 * Trazas (HT-10) del trabajo que no nace de una petición HTTP: la difusión de un evento del stream y el
 * snapshot pedido por el WebSocket. Si el evento (o el mensaje del cliente) trae un {@code traceparent} W3C
 * válido, el span es su hijo y la traza sigue la del servicio que lo publicó; si no, empieza una nueva. Las
 * llamadas HTTP del snapshot (RestClient con observaciones) heredan el span y propagan su {@code traceparent}.
 */
@Component
public class TraceContexts {

    public static final String TRACEPARENT = "traceparent";
    /** version-traceid-spanid-flags; se rechaza cualquier otra cosa (no se confía en lo que manda el cliente). */
    private static final Pattern VALID = Pattern.compile("00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}");

    private final Tracer tracer;
    private final Propagator propagator;

    @Autowired
    public TraceContexts(ObjectProvider<Tracer> tracer, ObjectProvider<Propagator> propagator) {
        this(tracer.getIfAvailable(() -> Tracer.NOOP), propagator.getIfAvailable(() -> Propagator.NOOP));
    }

    public TraceContexts(Tracer tracer, Propagator propagator) {
        this.tracer = tracer;
        this.propagator = propagator;
    }

    /** Sin trazas (pruebas unitarias). */
    public static TraceContexts noop() {
        return new TraceContexts(Tracer.NOOP, Propagator.NOOP);
    }

    /** Corre {@code work} dentro de un span {@code name}, hijo de {@code traceparent} si es válido. */
    public void run(String name, String traceparent, Runnable work) {
        Span.Builder builder = isValid(traceparent)
                ? propagator.extract(Map.of(TRACEPARENT, traceparent), Map::get)
                : tracer.spanBuilder();
        Span span = builder.name(name).start();
        try (Tracer.SpanInScope ignored = tracer.withSpan(span)) {
            work.run();
        } finally {
            span.end();
        }
    }

    static boolean isValid(String traceparent) {
        return traceparent != null && VALID.matcher(traceparent).matches();
    }
}
