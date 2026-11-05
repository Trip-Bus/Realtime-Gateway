package com.tripbus.realtime.config;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** HT-10: el span de la difusión y del snapshot continúa el traceparent recibido si es válido. */
class TraceContextsTest {

    private static final String TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    private final Tracer tracer = mock(Tracer.class);
    private final Propagator propagator = mock(Propagator.class);
    private final Span.Builder builder = mock(Span.Builder.class);
    private final Span span = mock(Span.class);
    private final TraceContexts tracing = new TraceContexts(tracer, propagator);

    private void stubSpan() {
        when(builder.name(anyString())).thenReturn(builder);
        when(builder.start()).thenReturn(span);
        when(tracer.withSpan(span)).thenReturn(mock(Tracer.SpanInScope.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void conTraceparentValidoElSpanEsHijoDelRecibido() {
        when(propagator.extract(any(Map.class), any(Propagator.Getter.class))).thenAnswer(invocation -> {
            Map<String, String> carrier = invocation.getArgument(0);
            Propagator.Getter<Map<String, String>> getter = invocation.getArgument(1);
            assertThat(getter.get(carrier, "traceparent")).isEqualTo(TRACEPARENT);
            return builder;
        });
        stubSpan();
        boolean[] ran = {false};

        tracing.run("tripbus.ws.difundir", TRACEPARENT, () -> ran[0] = true);

        assertThat(ran[0]).isTrue();
        verify(builder).name("tripbus.ws.difundir");
        verify(span).end();
        verify(tracer, never()).spanBuilder();
    }

    @Test
    void sinTraceparentOConUnoInvalidoEmpiezaUnaTrazaNuevaYTerminaElSpanAunqueFalle() {
        when(tracer.spanBuilder()).thenReturn(builder);
        stubSpan();

        tracing.run("a", null, () -> { });
        assertThatThrownBy(() -> tracing.run("b", "00-xyz\n-inyectado", () -> {
            throw new IllegalStateException("falla");
        })).isInstanceOf(IllegalStateException.class);

        verify(propagator, never()).extract(any(), any());
        verify(span, org.mockito.Mockito.times(2)).end();
        assertThat(TraceContexts.isValid("00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01")).isFalse();
        assertThat(TraceContexts.isValid(TRACEPARENT)).isTrue();
    }

    @Test
    void sinTracerEnElContextoNoHaceNadaPeroCorreElTrabajo() {
        DefaultListableBeanFactory beans = new DefaultListableBeanFactory();
        TraceContexts sinTrazas = new TraceContexts(beans.getBeanProvider(Tracer.class),
                beans.getBeanProvider(Propagator.class));
        boolean[] ran = {false, false};

        sinTrazas.run("x", TRACEPARENT, () -> ran[0] = true);
        TraceContexts.noop().run("y", null, () -> ran[1] = true);

        assertThat(ran).containsExactly(true, true);
        verify(tracer, never()).withSpan(eq(span));
    }
}
