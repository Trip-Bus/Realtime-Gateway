package com.tripbus.realtime.config;

import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class RequestIdFilterTest {

    private final RequestIdFilter filter = new RequestIdFilter();

    /** Ejecuta el filtro y devuelve el requestId que vio la petición en el MDC. */
    private String filtrar(MockHttpServletRequest request, MockHttpServletResponse response) throws Exception {
        AtomicReference<String> enMdc = new AtomicReference<>();
        filter.doFilter(request, response, new MockFilterChain() {
            @Override
            public void doFilter(jakarta.servlet.ServletRequest req, jakarta.servlet.ServletResponse res) {
                enMdc.set(MDC.get(RequestIdFilter.MDC_KEY));
            }
        });
        return enMdc.get();
    }

    @Test
    void reutilizaElRequestIdQueMandaElGateway() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader(RequestIdFilter.HEADER, "prueba-123");
        MockHttpServletResponse response = new MockHttpServletResponse();

        assertThat(filtrar(request, response)).isEqualTo("prueba-123");
        assertThat(response.getHeader(RequestIdFilter.HEADER)).isEqualTo("prueba-123");
        assertThat(MDC.get(RequestIdFilter.MDC_KEY)).isNull();
    }

    @Test
    void sinHeaderGeneraUnUuid() throws Exception {
        MockHttpServletResponse response = new MockHttpServletResponse();

        // /actuator no deja log de acceso, pero igual recibe requestId.
        String requestId = filtrar(new MockHttpServletRequest("GET", "/actuator/health"), response);

        assertThat(UUID.fromString(requestId)).isNotNull();
        assertThat(response.getHeader(RequestIdFilter.HEADER)).isEqualTo(requestId);
    }

    @Test
    void reemplazaValoresQuePodrianEnsuciarLosLogs() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader(RequestIdFilter.HEADER, "falso\nINFO usuario=admin");

        String requestId = filtrar(request, new MockHttpServletResponse());

        assertThat(UUID.fromString(requestId)).isNotNull();
    }
}
