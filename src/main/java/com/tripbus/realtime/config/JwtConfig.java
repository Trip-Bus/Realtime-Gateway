package com.tripbus.realtime.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;

@Configuration
public class JwtConfig {

    /** Valida los JWT con las llaves públicas (JWKS) que publica identity. Las llaves se cachean. */
    @Bean
    public JwtDecoder jwtDecoder(RealtimeProperties properties) {
        return NimbusJwtDecoder.withJwkSetUri(properties.jwksUri()).build();
    }
}
