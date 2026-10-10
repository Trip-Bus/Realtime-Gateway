package com.tripbus.realtime.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.core.env.MapPropertySource;
import org.springframework.mock.env.MockEnvironment;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RequiredSecretsCheckTest {

    private final RequiredSecretsCheck check = new RequiredSecretsCheck();
    private final SpringApplication application = new SpringApplication();

    @Test
    void sinPerfilDesplegadoNoHaceNada() {
        MockEnvironment environment = new MockEnvironment()
                .withProperty("tripbus.required-env", "REDIS_PASSWORD,WS_ALLOWED_ORIGINS");

        assertThatCode(() -> check.postProcessEnvironment(environment, application)).doesNotThrowAnyException();
    }

    @Test
    void conDesplegadoYTodoDefinidoPasa() {
        MockEnvironment environment = new MockEnvironment()
                .withProperty("tripbus.required-env", "REDIS_PASSWORD, WS_ALLOWED_ORIGINS")
                .withProperty("REDIS_PASSWORD", "secreto")
                .withProperty("WS_ALLOWED_ORIGINS", "otro");
        environment.setActiveProfiles("prod", "desplegado");

        assertThatCode(() -> check.postProcessEnvironment(environment, application)).doesNotThrowAnyException();
    }

    @Test
    void conVariablesFaltantesLanzaYLasNombra() {
        MockEnvironment environment = new MockEnvironment()
                .withProperty("tripbus.required-env", "REDIS_PASSWORD,WS_ALLOWED_ORIGINS,SEAT_INVENTORY_URL")
                .withProperty("REDIS_PASSWORD", "")
                .withProperty("SEAT_INVENTORY_URL", "http://seat-inventory:18003");
        environment.setActiveProfiles("prod", "desplegado");

        assertThatThrownBy(() -> check.postProcessEnvironment(environment, application))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Entorno prod,desplegado: faltan variables de entorno obligatorias "
                        + "[REDIS_PASSWORD, WS_ALLOWED_ORIGINS]");
    }

    @Test
    void uneLasListasDeTodosLosPerfiles() {
        // Si un perfil declara su propia lista, la unión de todas no debe perder ninguna variable.
        MockEnvironment environment = new MockEnvironment()
                .withProperty("tripbus.required-env", "REDIS_PASSWORD")
                .withProperty("REDIS_PASSWORD", "secreto");
        environment.getPropertySources().addLast(new MapPropertySource("application-qa.yml",
                Map.of("tripbus.required-env", "REDIS_PASSWORD,PUBLIC_API_URL")));
        environment.setActiveProfiles("qa", "desplegado");

        assertThatThrownBy(() -> check.postProcessEnvironment(environment, application))
                .hasMessageContaining("[PUBLIC_API_URL]");
    }
}
