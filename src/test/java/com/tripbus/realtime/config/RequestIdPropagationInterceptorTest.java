package com.tripbus.realtime.config;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.http.HttpMethod;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.mock.http.client.MockClientHttpRequest;
import org.springframework.mock.http.client.MockClientHttpResponse;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;

class RequestIdPropagationInterceptorTest {

    private final RequestIdPropagationInterceptor interceptor = new RequestIdPropagationInterceptor();
    private final ClientHttpRequestExecution execution = (request, body) -> new MockClientHttpResponse();

    @AfterEach
    void limpiarMdc() {
        MDC.clear();
    }

    private MockClientHttpRequest peticion() {
        return new MockClientHttpRequest(HttpMethod.GET, URI.create("http://seat-inventory/seats/itineraries/1"));
    }

    @Test
    void propagaElRequestIdDeLaPeticionEnCurso() throws Exception {
        MDC.put(RequestIdFilter.MDC_KEY, "prueba-realtime-1");
        MockClientHttpRequest request = peticion();

        interceptor.intercept(request, new byte[0], execution);

        assertThat(request.getHeaders().getFirst(RequestIdFilter.HEADER)).isEqualTo("prueba-realtime-1");
    }

    @Test
    void sinRequestIdEnCursoNoAgregaElHeader() throws Exception {
        MockClientHttpRequest request = peticion();

        interceptor.intercept(request, new byte[0], execution);

        assertThat(request.getHeaders().containsKey(RequestIdFilter.HEADER)).isFalse();
    }

    @Test
    void respetaUnHeaderPuestoExplicitamente() throws Exception {
        MDC.put(RequestIdFilter.MDC_KEY, "del-mdc");
        MockClientHttpRequest request = peticion();
        request.getHeaders().set(RequestIdFilter.HEADER, "explicito");

        interceptor.intercept(request, new byte[0], execution);

        assertThat(request.getHeaders().get(RequestIdFilter.HEADER)).containsExactly("explicito");
    }
}
