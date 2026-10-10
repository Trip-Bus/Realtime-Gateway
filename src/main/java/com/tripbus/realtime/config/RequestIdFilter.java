package com.tripbus.realtime.config;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Correlación de logs: cada petición lleva un requestId en el MDC (lo imprimen todos los logs de
 * esa petición) y en la respuesta. Si el API Gateway ya mandó X-Request-Id se reutiliza, así la
 * misma petición se puede seguir entre servicios. Corre primero que todo, también en el handshake
 * del WebSocket (GET /ws), para que hasta un 401 de token inválido quede correlacionado, y al
 * terminar escribe una línea de acceso (método, ruta, estado, ms).
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class RequestIdFilter extends OncePerRequestFilter {

    private static final Logger log = LoggerFactory.getLogger(RequestIdFilter.class);

    public static final String HEADER = "X-Request-Id";
    public static final String MDC_KEY = "requestId";

    /** Solo valores cortos y seguros: evita que un cliente inyecte saltos de línea en los logs. */
    private static final Pattern VALIDO = Pattern.compile("[A-Za-z0-9._-]{1,64}");

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String requestId = request.getHeader(HEADER);
        if (requestId == null || !VALIDO.matcher(requestId).matches()) {
            requestId = UUID.randomUUID().toString();
        }
        MDC.put(MDC_KEY, requestId);
        response.setHeader(HEADER, requestId);
        long inicio = System.nanoTime();
        try {
            chain.doFilter(request, response);
        } finally {
            // Log de acceso (una línea por petición) para poder buscar cualquier petición por su
            // requestId. Se omite /actuator: el healthcheck de Docker llama cada 10 s.
            if (!request.getRequestURI().startsWith("/actuator")) {
                log.info("{} {} -> {} ({} ms)", request.getMethod(), request.getRequestURI(),
                        response.getStatus(), (System.nanoTime() - inicio) / 1_000_000);
            }
            MDC.remove(MDC_KEY);
        }
    }
}
