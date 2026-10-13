package com.tripbus.realtime.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.PropertySource;
import org.springframework.core.env.PropertySourcesPropertyResolver;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * US-030: valor efectivo de allow-query-token por perfil, resolviendo SOLO los yml del repo (sin
 * variables de entorno de la máquina), con el yml del perfil por encima de application.yml.
 */
class PerfilesConfigTest {

    private static final String PROPIEDAD = "tripbus.realtime.allow-query-token";

    private static Boolean valorEfectivo(String perfil) throws IOException {
        YamlPropertySourceLoader loader = new YamlPropertySourceLoader();
        MutablePropertySources sources = new MutablePropertySources();
        if (perfil != null) {
            for (PropertySource<?> source : loader.load(perfil, new ClassPathResource("application-" + perfil + ".yml"))) {
                sources.addLast(source);
            }
        }
        for (PropertySource<?> source : loader.load("base", new ClassPathResource("application.yml"))) {
            sources.addLast(source);
        }
        return new PropertySourcesPropertyResolver(sources).getProperty(PROPIEDAD, Boolean.class);
    }

    @Test
    void prodNoAceptaElTokenEnLaUrl() throws IOException {
        assertThat(valorEfectivo("prod")).isFalse();
    }

    @Test
    void qaEIdeLoAceptanMientrasElFrontendMigra() throws IOException {
        assertThat(valorEfectivo("qa")).isTrue();
        assertThat(valorEfectivo(null)).isTrue();
    }
}
