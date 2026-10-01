package com.tripbus.realtime.config;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/** Configuración para la página de demostración (src/main/resources/static/index.html). */
@RestController
public class DemoConfigController {

    private final RealtimeProperties properties;

    public DemoConfigController(RealtimeProperties properties) {
        this.properties = properties;
    }

    @GetMapping("/demo-config.json")
    public Map<String, String> config() {
        return Map.of("apiUrl", properties.publicApiUrl());
    }
}
