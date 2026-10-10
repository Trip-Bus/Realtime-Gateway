package com.tripbus.realtime.config;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Profiles;
import org.springframework.core.env.PropertySource;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Verifica, en los entornos desplegados (perfiles "qa" y "prod", que activan el grupo
 * "desplegado"), que estén definidas todas las variables de {@code tripbus.required-env}.
 * Si falta alguna, el servicio no arranca y el mensaje dice exactamente cuáles faltan.
 *
 * <p>Es un {@link EnvironmentPostProcessor} y no un {@code @Component} a propósito: corre
 * cuando el {@code Environment} ya tiene cargados los application-*.yml pero ANTES de crear
 * cualquier bean. Como componente se ejecutaría tarde, y si antes fallaba otro bean (por ejemplo
 * la conexión a Redis sin contraseña) el error real quedaría escondido en una traza confusa.
 *
 * <p>Además, no basta con quitar el valor por defecto a los placeholders: propiedades como las
 * de {@code @ConfigurationProperties} NO fallan solas si falta la variable, sino que se quedan
 * con el texto literal {@code "${...}"} y el problema aparece mucho después (o nunca). Por eso
 * hace falta esta verificación explícita.
 *
 * <p>Cada perfil puede declarar su propia lista ({@code application-desplegado.yml},
 * {@code application-qa.yml}...). Como un perfil sobrescribe al otro, se toma la unión de los
 * valores de todas las fuentes de configuración para que ninguna variable se pierda.
 * Se registra en {@code META-INF/spring.factories}.
 */
public class RequiredSecretsCheck implements EnvironmentPostProcessor {

    static final String PERFIL = "desplegado";
    static final String PROPIEDAD = "tripbus.required-env";

    @Override
    public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
        if (!environment.acceptsProfiles(Profiles.of(PERFIL))) {
            return;
        }
        List<String> faltantes = new ArrayList<>();
        for (String variable : variablesObligatorias(environment)) {
            if (!StringUtils.hasText(environment.getProperty(variable))) {
                faltantes.add(variable);
            }
        }
        if (!faltantes.isEmpty()) {
            throw new IllegalStateException("Entorno " + String.join(",", environment.getActiveProfiles())
                    + ": faltan variables de entorno obligatorias " + faltantes);
        }
    }

    private static Set<String> variablesObligatorias(ConfigurableEnvironment environment) {
        Set<String> variables = new LinkedHashSet<>();
        for (PropertySource<?> fuente : environment.getPropertySources()) {
            Object valor = fuente.getProperty(PROPIEDAD);
            if (valor != null) {
                for (String variable : StringUtils.commaDelimitedListToStringArray(valor.toString())) {
                    if (StringUtils.hasText(variable)) {
                        variables.add(variable.trim());
                    }
                }
            }
        }
        return variables;
    }
}
