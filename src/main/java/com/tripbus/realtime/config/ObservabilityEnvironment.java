package com.tripbus.realtime.config;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.util.StringUtils;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Observabilidad (HT-10) a partir de variables de entorno que Spring Boot no entiende solo:
 * <ul>
 *   <li>{@code OTEL_EXPORTER_OTLP_ENDPOINT}: URL base del colector OTLP (p. ej. {@code http://otel:4318}); se le
 *       agrega {@code /v1/traces} como hace OpenTelemetry. Vacía = no se exporta ninguna traza (las trazas se
 *       siguen creando para el {@code traceId} de los logs y el {@code traceparent} de las llamadas).</li>
 *   <li>{@code tripbus.log-format} ({@code LOG_FORMAT}): {@code json} ⇒ logs estructurados ECS en la consola
 *       ({@code logging.structured.format.console=ecs}).</li>
 * </ul>
 * Corre después de cargar los application-*.yml y antes de iniciar el sistema de logs. Sus valores van al
 * final: una propiedad puesta explícitamente (yml, variable de entorno) gana. Se registra en
 * {@code META-INF/spring.factories}.
 */
public class ObservabilityEnvironment implements EnvironmentPostProcessor {

    static final String SOURCE = "tripbusObservability";
    static final String OTLP_ENDPOINT_ENV = "OTEL_EXPORTER_OTLP_ENDPOINT";
    static final String LOG_FORMAT = "tripbus.log-format";
    private static final String TRACES_PATH = "/v1/traces";

    @Override
    public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
        Map<String, Object> values = new HashMap<>();
        String endpoint = environment.getProperty(OTLP_ENDPOINT_ENV);
        if (StringUtils.hasText(endpoint)) {
            values.put("management.otlp.tracing.endpoint", tracesUrl(endpoint));
        } else {
            values.put("management.otlp.tracing.export.enabled", "false");
        }
        String format = environment.getProperty(LOG_FORMAT, "");
        if ("json".equals(format.trim().toLowerCase(Locale.ROOT))) {
            values.put("logging.structured.format.console", "ecs");
        }
        environment.getPropertySources().addLast(new MapPropertySource(SOURCE, values));
    }

    /** URL de trazas OTLP/HTTP a partir de la URL base del colector. */
    static String tracesUrl(String endpoint) {
        String base = endpoint.trim();
        while (base.endsWith("/")) {
            base = base.substring(0, base.length() - 1);
        }
        return base.endsWith(TRACES_PATH) ? base : base + TRACES_PATH;
    }
}
