package com.tripbus.realtime.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.mock.env.MockEnvironment;

import static org.assertj.core.api.Assertions.assertThat;

/** HT-10: OTEL_EXPORTER_OTLP_ENDPOINT vacía apaga la exportación; LOG_FORMAT=json activa ECS. */
class ObservabilityEnvironmentTest {

    private final ObservabilityEnvironment processor = new ObservabilityEnvironment();

    private MockEnvironment process(MockEnvironment environment) {
        processor.postProcessEnvironment(environment, new SpringApplication());
        return environment;
    }

    @Test
    void sinEndpointNoSeExportaNingunaTraza() {
        MockEnvironment environment = process(new MockEnvironment());
        assertThat(environment.getProperty("management.otlp.tracing.export.enabled")).isEqualTo("false");
        assertThat(environment.getProperty("management.otlp.tracing.endpoint")).isNull();

        MockEnvironment blank = process(new MockEnvironment().withProperty("OTEL_EXPORTER_OTLP_ENDPOINT", "  "));
        assertThat(blank.getProperty("management.otlp.tracing.export.enabled")).isEqualTo("false");
    }

    @Test
    void conEndpointSeExportaASuRutaDeTrazas() {
        MockEnvironment environment = process(new MockEnvironment()
                .withProperty("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel:4318/"));
        assertThat(environment.getProperty("management.otlp.tracing.endpoint")).isEqualTo("http://otel:4318/v1/traces");
        assertThat(environment.getProperty("management.otlp.tracing.export.enabled")).isNull();
        assertThat(ObservabilityEnvironment.tracesUrl(" http://otel:4318/v1/traces ")).isEqualTo("http://otel:4318/v1/traces");
    }

    @Test
    void logFormatJsonActivaLosLogsEcs() {
        assertThat(process(new MockEnvironment().withProperty("tripbus.log-format", " JSON "))
                .getProperty("logging.structured.format.console")).isEqualTo("ecs");
        assertThat(process(new MockEnvironment().withProperty("tripbus.log-format", "text"))
                .getProperty("logging.structured.format.console")).isNull();
    }

    @Test
    void unaPropiedadExplicitaGanaSobreLaCalculada() {
        MockEnvironment environment = new MockEnvironment()
                .withProperty("management.otlp.tracing.export.enabled", "true");
        process(environment);
        assertThat(environment.getProperty("management.otlp.tracing.export.enabled")).isEqualTo("true");
        assertThat(environment.getPropertySources().contains(ObservabilityEnvironment.SOURCE)).isTrue();
    }
}
