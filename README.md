# Realtime Gateway

Mantiene las conexiones WebSocket con los clientes: autentica el canal, gestiona suscripciones,
envía el snapshot inicial y difunde los eventos. **No tiene lógica de negocio ni base de datos.**

**HU:** US-020, US-021, US-047 (parte), US-056 (parte) · **Puerto:** 16007

## Protocolo

Conexión: `ws://host:16007/ws?token=<JWT>` (o conectar sin token y enviar `{"type":"connect","token":"..."}` en ≤ 10 s).

| Cliente → servidor | Servidor → cliente |
|---|---|
| `{"type":"subscribe","topic":"itinerario:{id}:asientos"}` | `subscribed`, luego `snapshot` con el estado actual |
| `{"type":"unsubscribe","topic":"..."}` | `event` con `{topic, event, eventId, data}` |
| `{"type":"ping"}` / `{"type":"pong"}` | `ping` cada 20 s (sin respuesta en 45 s ⇒ se cierra con 4408) |

Tópicos: `itinerario:{id}:asientos`, `oferta:{id}:cupo`, `viaje:{id}:eventos`, `bus:{id}:posicion`.
Token inválido ⇒ 401 en el handshake; token vencido ⇒ cierre 4401 (el cliente reconecta con uno nuevo).
Nunca se reenvía el `userId` de otros pasajeros.

Cada réplica lee **todos** los eventos de `tripbus:events` (Redis Streams) y los entrega a sus
propias conexiones, así que escala horizontalmente sin sticky sessions.

## Página de demostración

`http://localhost:16007/` — cliente de prueba del MVP: login, búsqueda, mapa de asientos en vivo,
retención con cuenta regresiva, pago y tiquete QR, posición del bus y novedades en vivo.

## Ejecutar y probar

```bash
mvn spring-boot:run          # requiere Redis e identity
TRIPBUS_IT=true mvn test     # prueba WebSocket de extremo a extremo con Redis real
```
