# Realtime Gateway

[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=Trip-Bus_Realtime-Gateway&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=Trip-Bus_Realtime-Gateway)

Mantiene las conexiones WebSocket con los clientes: autentica el canal, gestiona suscripciones,
envía el snapshot inicial y difunde los eventos. **No tiene lógica de negocio ni base de datos.**

**Stack:** Java 21 · Spring Boot 3.5.16 · Redis 7

**HU:** US-020, US-021, US-033 (aviso de tiquete), US-039 (seguimiento público), US-047 (parte), US-056 (parte), US-183 (avisos del pasajero), US-186 (canal del conductor), US-184 (reconexión), US-185 (Web Push), US-163 (personas mirando), US-162 (eventos de la sala de espera), HT-10 (observabilidad), HT-11 (límites y métricas) · **Puerto:** 18007 · **BD:** ninguna (solo Redis)

## Protocolo

### Conectarse y autenticarse en `/ws`

La URL pública es la del **API Gateway** (NGINX), no la de este servicio: `ws(s)://<gateway>:18000/ws`
(en local `ws://localhost:18000/ws`). El 18007 solo se usa directo en el IDE y en el compose de QA de este repo.

**Forma recomendada (la única en PROD):** abrir `/ws` **sin token** y, apenas abra, enviar el JWT
en el primer mensaje, antes de 10 s:

```js
const ws = new WebSocket("wss://<gateway>:18000/ws");
ws.onopen = () => ws.send(JSON.stringify({ type: "connect", token: jwt }));
// servidor → {"type":"connected","userId":...,"role":...,"heartbeatSeconds":20,"tokenExpiresAt":...}
```

Si no llega un `connect` válido en 10 s, el servidor envía `{"type":"error","code":"AUTH_TIMEOUT"}`
y cierra con 4401; cualquier `subscribe` antes del `connect` responde `UNAUTHENTICATED`. Sin cuenta
se manda `connect` sin token: ver [Seguimiento público](#seguimiento-público).

**`?token=<JWT>` en la URL (heredado, solo IDE y QA):** un token en la URL queda en logs, historial
del navegador y proxies, así que se controla con `WS_ALLOW_QUERY_TOKEN`
(`tripbus.realtime.allow-query-token`):

| Entorno | `WS_ALLOW_QUERY_TOKEN` | Efecto de `?token=` |
|---|---|---|
| IDE / QA | `true` | Se valida en el handshake (401 si es inválido) y la conexión queda autenticada. |
| PROD | `false` | Se **ignora** (el handshake no falla, pero no autentica): hay que mandar `connect`. |

El servicio nunca escribe la query string ni el token en los logs (el log de acceso solo lleva la
ruta). Detrás del gateway, la autenticación depende solo del JWT y el `Origin` se valida contra
`WS_ALLOWED_ORIGINS`; los headers `X-Forwarded-*` no se usan para nada
(`server.forward-headers-strategy: none`).

### Mensajes

| Cliente → servidor | Servidor → cliente |
|---|---|
| `{"type":"subscribe","topic":"itinerario:{id}:asientos"}` | `subscribed`, luego `snapshot` con el estado actual |
| `{"type":"resume","topic":"...","lastSeq":"<ms>-<n>"}` (al reconectar, US-184) | `subscribed`, los `event` perdidos y `resumed`; o `subscribed` y `snapshot` si ya no están (ver [Reconexión](#reconexión-resume)) |
| `{"type":"unsubscribe","topic":"..."}` | `event` con `{topic, event, seq, eventId, data}` (`snapshot` lleva `asOfSeq`) |
| `{"type":"ping"}` / `{"type":"pong"}` | `ping` cada 20 s (sin respuesta en 45 s ⇒ se cierra con 4408) |

`subscribe` y `resume` aceptan además `"traceparent":"00-…"` (W3C, opcional): el snapshot que piden continúa
esa traza (ver [Observabilidad](#observabilidad)).

#### Tópicos

| Tópico | Acceso | Eventos | Snapshot |
|---|---|---|---|
| `itinerario:{id}:asientos` | Público | `SeatHeld`, `SeatReleased`, `SeatSold`, `HoldExpired`, `SeatStateChanged` | Mapa de asientos (seat-inventory) |
| `oferta:{id}:cupo` | Público | `OfferQuotaUpdated` (todos), `OfferClosed`, `OfferSoldOut`, `OfferActivated`, `OfferQueueUpdated {offerId, enFila, admitidos}` (sala de espera, US-162), `OfferViewers {offerId, personas}` (US-163, lo genera el gateway; sin `seq`) | Cupo (flash-offers) |
| `ofertas:activas` | Público (único tópico **sin id**) | `OfferCreated`, `OfferActivated`, `OfferUpdated`, `OfferClosed`, `OfferSoldOut` (con `origen` y `destino`), `OfferQuotaUpdated` **como máximo uno por segundo por oferta** (US-160) | `GET {flash-offers}/offers/showcase`: primera página de la vitrina |
| `viaje:{id}:eventos` | Público | `TripEventReported` (incluye `AVERIA`, `ACCIDENTE`, `VIA_CERRADA`; nunca `SOS`), `ItineraryRescheduled` | Línea de tiempo (fleet) |
| `viaje:{id}:posicion` | Público | `PositionUpdated` (con `itineraryId`), `TrackingStatusChanged` (`ACTIVO`, `EN_PAUSA`, `SIN_SENAL`, `FINALIZADO`), `EtaUpdated` | `GET {fleet}/itineraries/{id}/bus-position`: posición, estado, paradas, `estadoRastreo` y `ultimaPosicionEn` (US-173/US-174) |
| `bus:{id}:posicion` | Público — **deprecado** | `PositionUpdated` | Última posición (fleet) |
| `usuario:{uuid}:tiquetes` | **Privado**: solo la sesión autenticada cuyo `userId` es `{uuid}` | `TicketIssued` (US-033) | Ninguno: solo `subscribed` |
| `usuario:{uuid}:avisos` | **Privado**: solo la sesión cuyo `userId` es `{uuid}` (ni ADMIN) | `BookingAffected`, `BookingRelocated`, `RefundProcessed`, `TicketRescheduled`, `TicketUpdated`, `TicketIssued` (US-183), `WaitlistSeatOffered` y `WaitlistClosed` (US-140), `TicketNoShow` (US-153), `OfferAlert` (US-161), `OfferQueueAdmitted` (sala de espera, US-162) | Ninguno: la app pide `GET /bookings/affected` |
| `conductor:{uuid}:viaje` | **Privado**: solo la sesión cuyo `userId` es `{uuid}` y con rol `CONDUCTOR` | `DriverAssigned`, `DriverUnassigned`, `DriverAssignmentConflict` (US-186) | Ninguno: solo `subscribed` |
| `empresa:{uuid}:alertas` | **Privado**: `OPERADOR` cuyo claim `companyId` es `{uuid}`, o `ADMIN` (cualquier empresa) | `TripAlertRaised`, `TripAlertResolved`, `TripInterrupted`, `DriverAssignmentConflict`, `TrackingStatusChanged` solo con estado `SIN_SENAL` | Ninguno: la app pide `GET /alerts?estado=ABIERTA` |
| `empresa:{uuid}:flota` | **Privado**: igual que `empresa:{uuid}:alertas` | `PositionUpdated` **como máximo una cada 3 s por viaje**, `EtaUpdated`, `TrackingStatusChanged` (todos los estados), por `companyId` (US-178) | Ninguno: la app pide `GET /fleet/live` |

- **`viaje:{id}:posicion`** (`{id}` = itinerario) es el tópico para seguir el bus de un viaje: trae la
  posición y el estado del rastreo. **`bus:{id}:posicion` está deprecado:** `PositionUpdated` se sigue
  difundiendo ahí para no romper al frontend actual, pero los clientes nuevos deben usar
  `viaje:{id}:posicion`; se quitará cuando el frontend haya migrado.
- **Públicos:** los sigue cualquier sesión, también la anónima. Nunca llevan datos de personas:
  `userId`, `holdId`, `bookingId`, `conductorId`, `email`, `numeroDocumento`, `nombre`, `qrCode` y
  `mensaje` se quitan de los eventos a cualquier profundidad (`EventFanout`), y el `motivo` de un
  objeto con `tipo: SOS`. Excepción: `nombre` se conserva dentro de `paradas` (es el nombre de la
  parada en `EtaUpdated`). `TripAlertRaised` nunca se difunde a un tópico público.
- **Límites de frecuencia** (por réplica, en memoria): en `ofertas:activas` pasa como máximo un
  `OfferQuotaUpdated` por segundo por oferta y en `empresa:{uuid}:flota` una `PositionUpdated` cada
  3 s por viaje (`itineraryId`); los demás dentro de esa ventana **se descartan**, no se encolan.
  `oferta:{id}:cupo` y `viaje:{id}:posicion` siguen recibiéndolos todos: para el valor exacto, ese tópico.
  Los demás eventos de la vitrina y de la flota no tienen límite.

```js
ws.send(JSON.stringify({ type: "connect" }));                         // también sin cuenta
ws.send(JSON.stringify({ type: "subscribe", topic: "ofertas:activas" }));
// servidor → {"type":"snapshot","topic":"ofertas:activas","asOfSeq":…,"data":{…primera página de /offers/showcase…}}
// servidor → {"type":"event","topic":"ofertas:activas","event":"OfferActivated",
//             "data":{"offerId":…,"origen":"Bogotá","destino":"Tunja",…}}
```
- **Privado `usuario:{uuid}:tiquetes`:** aviso en vivo al comprador cuando booking emite su tiquete.
  Un anónimo u otro usuario (ADMIN incluido) ⇒ `FORBIDDEN_TOPIC` sin cerrar la conexión. El `data`
  lleva **solo** `bookingId`, `ticketId`, `itineraryId` y `seatNumbers` (nunca `qrCode` ni `userId`);
  la app pide el tiquete completo con `GET /tickets/{ticketId}`.

```js
ws.send(JSON.stringify({ type: "subscribe", topic: `usuario:${userId}:tiquetes` }));
// servidor → {"type":"event","topic":"usuario:…:tiquetes","event":"TicketIssued",
//             "data":{"bookingId":…,"ticketId":…,"itineraryId":…,"seatNumbers":["5"]}}
```

- **Privado `usuario:{uuid}:avisos` (US-183):** avisos de las reservas del pasajero. Booking los
  manda con el `userId` del dueño; **sin `userId` no se difunden** (queda un WARN en el log). El `data`
  lleva solo identificadores y datos de la decisión (`bookingId`, `ticketId`, `itineraryId` y sus
  variantes `…Anterior`/`…Nuevo`, `refundId`, `seatNumbers`, `cambio`, `causa`, `plazoDecision`,
  `opciones`); nunca `qrCode`, `email`, documento, nombre ni montos.
  - `WaitlistSeatOffered` (US-140, de seat-inventory) lleva `itineraryId`, `seatNumbers` y `expiraEn`;
    el `holdId` no sale (es dato de la retención: la app lo consulta por la lista de espera).
  - `WaitlistClosed {itineraryId, userIds}` se envía al tópico de **cada** `userId` de la lista, con
    `data` = solo `{itineraryId}`: ningún pasajero ve los ids de los demás turnos.
  - `TicketNoShow` (US-153, de booking) lleva `bookingId`, `ticketId` e `itineraryId`.
  - `OfferAlert` (US-161) lleva solo datos de la oferta: `offerId`, `itineraryId`, `origen`, `destino`,
    `precioOferta`, `precioBase`, `inicio`, `fin` y `salida` (los que vengan).
  - `OfferQueueAdmitted {userId, …}` (sala de espera, US-162, de flash-offers): al pasajero admitido a
    comprar. Lleva solo `offerId`, `itineraryId`, `turno`, `admitidoEn` y `expiraEn` (los que vengan).

```js
ws.send(JSON.stringify({ type: "subscribe", topic: `usuario:${userId}:avisos` }));
// servidor → {"type":"subscribed",...}   (sin snapshot: la app llama GET /bookings/affected)
// servidor → {"type":"event","topic":"usuario:…:avisos","event":"BookingAffected",
//             "data":{"bookingId":…,"ticketId":…,"itineraryId":…,"causa":"CANCELACION",
//                     "plazoDecision":"…","opciones":["REUBICAR","REEMBOLSO"]}}
```

- **Privado `conductor:{uuid}:viaje` (US-186):** asignaciones del conductor. Solo el propio usuario
  y con rol `CONDUCTOR` (otro rol con el mismo uuid, otro conductor o un ADMIN ⇒ `FORBIDDEN_TOPIC`).

```js
ws.send(JSON.stringify({ type: "subscribe", topic: `conductor:${userId}:viaje` }));
// servidor → {"type":"event","topic":"conductor:…:viaje","event":"DriverAssigned",
//             "data":{"itineraryId":…,"companyId":…,"conductorId":…,"rol":"PRINCIPAL"}}
```

- **Privado `empresa:{uuid}:alertas`:** alertas de los viajes de la empresa para el operador. Aquí
  sí van `conductorId` y el `mensaje` del SOS; nunca datos de pasajeros. `TripAlertRaised` de gravedad
  `CRITICA` (SOS, ACCIDENTE) se envía **en el orden normal** del stream: el envío va directo al buffer
  de cada sesión (`ConcurrentWebSocketSessionDecorator`) y no hay una cola propia que se pueda
  reordenar sin reescribirlo.

```js
ws.send(JSON.stringify({ type: "subscribe", topic: `empresa:${companyId}:alertas` }));
// servidor → {"type":"subscribed",...}   (sin snapshot: la app llama GET /alerts?estado=ABIERTA)
// servidor → {"type":"event","topic":"empresa:…:alertas","event":"TripAlertRaised",
//             "data":{"alertId":…,"itineraryId":…,"tipo":"SOS","gravedad":"CRITICA","mensaje":…}}
```

- **Privado `empresa:{uuid}:flota` (US-178):** el mapa de la flota en vivo. Mismo acceso que las
  alertas (otra empresa, otro rol o un anónimo ⇒ `FORBIDDEN_TOPIC`) y, como ellas, conserva
  `conductorId` sin datos de pasajeros. Sin snapshot: al suscribirse la app pide `GET /fleet/live` y
  luego aplica los eventos (que fleet manda con `companyId`).

```js
ws.send(JSON.stringify({ type: "subscribe", topic: `empresa:${companyId}:flota` }));
// servidor → {"type":"subscribed",...}   (sin snapshot: la app llama GET /fleet/live)
// servidor → {"type":"event","topic":"empresa:…:flota","event":"PositionUpdated",
//             "data":{"busId":…,"itineraryId":…,"companyId":…,"lat":…,"lng":…}}
```

- **Personas mirando una oferta (US-163):** el gateway cuenta las sesiones suscritas a `oferta:{id}:cupo`
  en **todas** las réplicas (hash `tripbus:ws:viewers:{offerId}` en Redis: un campo por réplica que vence a
  los 15 s si la réplica deja de renovarlo) y cada 5 s (`tripbus.realtime.viewers-interval-ms`), **si el
  total cambió**, envía al mismo tópico `{"type":"event","event":"OfferViewers","data":{"offerId":…,"personas":3}}`.
  Solo es un número: nunca dice quién mira. No viene del stream, así que no lleva `seq` (no se compara con
  `asOfSeq`) ni entra al búfer de reconexión.
- **Sala de espera (US-162):** `OfferQueueUpdated {offerId, enFila, admitidos}` va a `oferta:{offerId}:cupo`
  (no a la vitrina) y `OfferQueueAdmitted {userId, …}` a `usuario:{userId}:avisos` (también por Web Push).

### Límites

| Límite | Valor | Dónde se cuenta | Al pasarse |
|---|---|---|---|
| Suscripciones de una sesión autenticada | `max-subscriptions` = 20 | en la sesión | `TOO_MANY_SUBSCRIPTIONS`, sin cerrar |
| Suscripciones de `OPERADOR` y `ADMIN` | `max-staff-subscriptions` = 20 (sin valor: `max-subscriptions`) | en la sesión | `TOO_MANY_SUBSCRIPTIONS`, sin cerrar |
| Suscripciones de una sesión anónima | `max-anonymous-subscriptions` = 5 | en la sesión | `TOO_MANY_SUBSCRIPTIONS`, sin cerrar |
| Conexiones abiertas por IP (HT-11) | `max-connections-per-ip` = 20 (`WS_MAX_CONNECTIONS_PER_IP`) | Redis, entre réplicas | `TOO_MANY_CONNECTIONS` y cierre **4429** al abrir |
| Conexiones autenticadas por usuario (HT-11) | `max-connections-per-user` = 5 (`WS_MAX_CONNECTIONS_PER_USER`) | Redis, entre réplicas | `TOO_MANY_CONNECTIONS` y cierre **4429** al autenticar |
| Mensajes del cliente (HT-11) | 30 por 10 s por sesión (`max-client-messages`, `client-message-window`) | en la sesión | `RATE_LIMITED` y el mensaje se descarta, **sin cerrar** |

El `connected` de una sesión autenticada trae su `maxSubscriptions`.

- Las conexiones se cuentan en sorted sets de Redis (`tripbus:ws:conn:ip:{ip}` y
  `tripbus:ws:conn:user:{userId}`): un miembro por conexión que el heartbeat renueva cada 20 s y que vence a
  los 60 s (`presence-ttl`) si la réplica muere. Al cerrarse la conexión se descuenta en el acto. Las
  anónimas cuentan solo por IP. Si Redis no responde se deja conectar (con WARN), como con la revocación.
- **IP del cliente:** por defecto la de la conexión TCP. Detrás del API Gateway todas llegarían con la IP
  del gateway, así que ahí se configura `WS_CLIENT_IP_HEADER=X-Forwarded-For` (o `X-Real-IP`): se toma el
  **último** valor del header (el que agregó el proxy de confianza; lo anterior lo pudo escribir el cliente)
  y solo si es una IP. Es solo para contar: la autenticación sigue sin usar ningún header `X-Forwarded-*`.

Token inválido en `connect` ⇒ `INVALID_TOKEN` y cierre 4401 (en `?token=`, si está permitido, ⇒ 401
en el handshake); token vencido ⇒ cierre 4401 (el cliente reconecta con uno nuevo), también con
temas privados suscritos.

**Tokens revocados (US-006):** la firma no basta. Al autenticar (`connect` o `?token=` si está
permitido) el gateway consulta `tripbus:revoked:{jti}` en Redis (la escribe identity al cerrar sesión,
con TTL); si existe ⇒ `{"type":"error","code":"UNAUTHENTICATED"}` y cierre 4401. Si Redis no responde
se permite la conexión con un WARN en el log (la expiración del token sigue protegiendo). Con la
sesión abierta, el evento `SessionRevoked {jti, userId}` cierra con 4401 las conexiones de ese `jti`
(las de otros tokens del mismo usuario siguen abiertas) y `UserSuspended {userId}` todas las de ese
usuario. Estos eventos no se reenvían a ningún tema y no afectan a las sesiones anónimas.

**Cuentas suspendidas (US-104):** en la misma autenticación (`connect` o `?token=`), después del
`jti`, el gateway consulta `tripbus:suspended:{sub}` en Redis (la escribe identity al suspender la
cuenta). Si existe ⇒ `{"type":"error","code":"ACCOUNT_SUSPENDED"}` y cierre 4401, igual que un token
revocado. Si Redis no responde se aplica el mismo criterio: se permite la conexión con un WARN. Las
conexiones ya abiertas las cierra el evento `UserSuspended` (arriba).

**Cambio de contraseña (US-129):** identity guarda `tripbus:pwd-changed:{sub}` = epoch en **segundos**
del cambio (TTL = vida máxima de un token) y devuelve un token nuevo. Los tokens con `iat` anterior a
ese valor dejan de valer:

- **Al conectar** (`connect` o `?token=`), después de la suspensión: si la clave existe y el `iat` es
  menor ⇒ `{"type":"error","code":"UNAUTHENTICATED"}` y cierre 4401, igual que un token revocado. El
  token emitido en el mismo segundo del cambio (el que entrega identity) sí vale; un token **sin `iat`**
  con la clave presente se rechaza (como hace identity).
- **Con la sesión abierta:** en cada heartbeat (20 s) el gateway hace **una** consulta (`GET`) por
  sesión autenticada y la cierra igual si su token quedó viejo. Las anónimas no consultan nada.
- Identity no publica un evento `PasswordChanged`, así que no hay cierre inmediato por evento: el
  máximo para cerrar una sesión abierta es un intervalo de heartbeat.
- Redis caído o valor ilegible ⇒ mismo criterio que la revocación: se permite, con un WARN en el log.

### Orden de los eventos (`seq` y `asOfSeq`)

Cada `event` lleva `seq`: el id de su entrada en el stream de Redis (`"<ms>-<n>"`, p. ej.
`"1760280000000-3"`), monótono en todo el bus. Cada `snapshot` lleva `asOfSeq`: el último `seq` que
el gateway había difundido cuando pidió ese snapshot (`"0-0"` si aún no había difundido nada). Para
compararlos se separan por `-` y se comparan numéricamente primero los milisegundos y luego `n`
(no como texto).

El cliente debe:

1. **Ignorar todo `event` con `seq <= asOfSeq`:** ese cambio ya está incluido en el snapshot (el tema
   se suscribe antes de pedirlo, así que puede llegar un evento antes que el snapshot o repetido).
2. **Si detecta un salto** (un `seq` que no es mayor que el último aplicado, o la conexión se cortó y
   pudo perder eventos), **volver a pedir el snapshot**: `unsubscribe` y `subscribe` del tema, y aplicar
   de nuevo la regla 1 con el `asOfSeq` nuevo.

```js
const after = (a, b) => { const [am, an] = a.split("-").map(Number), [bm, bn] = b.split("-").map(Number);
                          return am !== bm ? am > bm : an > bn; };
// snapshot → estado = data; ultimo = asOfSeq
// event    → if (!after(seq, ultimo)) return; aplicar(data); ultimo = seq;
```

### Reconexión (`resume`)

US-184: al reconectar, en vez de `subscribe` el cliente manda `resume` con el `seq` del último evento que
aplicó de ese tópico, y recupera lo que se perdió sin pedir el snapshot completo:

```js
ws.send(JSON.stringify({ type: "resume", topic: `usuario:${userId}:avisos`, lastSeq: ultimo }));
// servidor → {"type":"subscribed","topic":…}
// servidor → {"type":"event",…,"seq":"1760280000000-4"}     (los perdidos, en orden)
// servidor → {"type":"event",…,"seq":"1760280000000-7"}
// servidor → {"type":"resumed","topic":…,"lastSeq":"1760280000000-7"}
```

- Exige **la misma autorización que `subscribe`** (temas privados, anónimos, máximo de suscripciones): si no
  pasa, el mismo error (`FORBIDDEN_TOPIC`, `UNAUTHENTICATED`…) y no queda suscrito. Un `lastSeq` que no tiene
  la forma `<ms>-<n>` ⇒ `INVALID_SEQ`.
- El gateway guarda en Redis los últimos **200** eventos de **cada tópico** (`tripbus.realtime.replay.size`)
  durante **10 minutos** (`tripbus.realtime.replay.ttl`), tal como se enviaron (ya sin datos privados), en
  un stream por tópico (`tripbus:replay:{topic}`, con el mismo id del bus). Lo comparten todas las réplicas:
  se puede reconectar a cualquiera.
- **Si siguen en el búfer** todos los eventos posteriores a `lastSeq`, se reenvían en orden y la respuesta
  termina con `resumed` (`lastSeq` = el último enviado). Los que llegan en vivo mientras tanto se retienen y
  salen después de los del búfer, sin repetirse ni desordenarse.
- **Si ya no están** (pasaron más de 10 minutos, más de 200 eventos o el tópico no tiene búfer) se manda el
  `snapshot` con `asOfSeq`, exactamente como un `subscribe`, y no llega `resumed`. En los tópicos sin
  snapshot (avisos, conductor, alertas, flota) ese `snapshot` trae `"data": null`: la app vuelve a pedir su
  `GET` (p. ej. `/bookings/affected`).

### Notificaciones push (Web Push)

US-185: avisos con la app cerrada. El navegador se suscribe con la llave VAPID pública y registra su
suscripción en el gateway (a través del API Gateway, con el mismo JWT):

| Método y ruta | Autenticación | Cuerpo | Respuesta |
|---|---|---|---|
| `GET /push/public-key` | ninguna | — | `200 {"publicKey":"…"}` (base64url) |
| `POST /push/subscriptions` | `Authorization: Bearer <JWT>` | `{endpoint, keys:{p256dh, auth}}` (`PushSubscription.toJSON()`) | `201`; `400 INVALID_SUBSCRIPTION`; `401 UNAUTHENTICATED` |
| `DELETE /push/subscriptions` | `Authorization: Bearer <JWT>` | `{endpoint}` (sin cuerpo: todas las del usuario) | `204`; `401` |

```js
const { publicKey } = await (await fetch("/push/public-key")).json();
const sub = await registration.pushManager.subscribe({ userVisibleOnly: true, applicationServerKey: publicKey });
await fetch("/push/subscriptions", { method: "POST", body: JSON.stringify(sub),
  headers: { "Content-Type": "application/json", Authorization: `Bearer ${jwt}` } });
```

- El token se rechaza (401) igual que en el WebSocket: revocado, cuenta suspendida o anterior a un cambio de
  contraseña. El endpoint debe ser `https` y las llaves válidas (p256dh de 65 bytes, auth de 16).
- Se guardan en Redis **hasta 5 suscripciones por usuario** (`tripbus:push:subs:{userId}`); al registrar la
  sexta se borra la más vieja. Un `OPERADOR` queda además en `tripbus:push:operadores:{companyId}`.
- Se envía push **solo si el destinatario no tiene ninguna conexión abierta en ninguna réplica** (los mismos
  contadores de Redis de los [límites](#límites)); con la app abierta le llega el `event` por el WebSocket.
  La primera réplica que reclama `tripbus:push:enviado:{eventId}:{userId}` es la única que envía.

| Evento | Destinatario | Título |
|---|---|---|
| `BookingAffected` | pasajero (`userId`) | Tu viaje tuvo un cambio |
| `BookingRelocated` | pasajero | Te reubicamos en otro viaje |
| `WaitlistSeatOffered` | pasajero | ¡Hay un puesto para ti! |
| `OfferAlert` | pasajero | Nueva oferta en tu ruta |
| `OfferQueueAdmitted` | pasajero | ¡Es tu turno! |
| `DriverAssigned` | conductor (`conductorId`) | Tienes un viaje asignado |
| `DriverAssignmentConflict` | conductor | Hay un conflicto en tu agenda |
| `TripAlertRaised` con `gravedad: CRITICA` | operadores de la empresa (`companyId`) | Alerta crítica en un viaje |

- El contenido es `{"title","body","tipo"}`: un título y un texto corto en español con tuteo, **sin datos
  personales** (ni nombres, ni ids, ni montos); la app abre el detalle al tocarlo. Va cifrado (`aes128gcm`)
  y firmado con VAPID (librería `nl.martijndwars:web-push`, MIT), con TTL de 1 h y urgencia alta.
- Una suscripción que responde **404 o 410** ya no existe y se borra.
- **Sin `VAPID_PUBLIC_KEY`, `VAPID_PRIVATE_KEY` y `VAPID_SUBJECT`** (o con llaves inválidas) el push queda
  apagado: `GET /push/public-key` y `POST` responden `503 PUSH_DISABLED`, no se envía nada y todo lo demás
  funciona igual. Las llaves se generan una vez, p. ej. con `npx web-push generate-vapid-keys`.

### Seguimiento público

Decisión de producto (US-039, US-047, US-056): seguir el bus no exige cuenta, porque sirve a quien lo
espera en la vía. Se abre `/ws` y se manda `connect` **sin token** (o con `"token":""`):

```js
ws.onopen = () => ws.send(JSON.stringify({ type: "connect" }));
// servidor → {"type":"connected","anonymous":true,"heartbeatSeconds":20,"maxSubscriptions":5}
ws.send(JSON.stringify({ type: "subscribe", topic: `viaje:${itineraryId}:posicion` }));
```

- La sesión anónima no se cierra por `AUTH_TIMEOUT` ni por vencimiento de token (no tiene); sí por
  falta de heartbeat (4408), como cualquier otra.
- Solo puede suscribirse a **temas públicos** (`Topics.isPublic`): `viaje:{id}:posicion`,
  `bus:{id}:posicion` (deprecado), `viaje:{id}:eventos`, `oferta:{id}:cupo`, `itinerario:{id}:asientos`. Otro tema ⇒ error
  `FORBIDDEN_TOPIC` sin cerrar la conexión.
- Máximo **5 suscripciones** a la vez (`tripbus.realtime.max-anonymous-subscriptions`); la sexta ⇒
  `TOO_MANY_SUBSCRIPTIONS`. Las sesiones autenticadas siguen con su propio límite (arriba), los temas
  públicos y los privados que les correspondan.
- Recibe el mismo snapshot y los mismos eventos que una sesión autenticada. Los snapshots se piden sin
  usuario (el mapa de asientos solo dice el estado de cada puesto, `propio` siempre `false`) y los
  eventos van sin ids de pasajeros ni de reservas.
- Un `connect` con token inválido sigue respondiendo `INVALID_TOKEN` y cerrando con 4401. Un anónimo
  puede autenticarse después enviando `connect` con token; una sesión ya autenticada no se degrada
  (`connect` sin token ⇒ `ALREADY_AUTHENTICATED`). `allow-query-token` no cambia: `?token=` sigue
  igual que arriba.

Cada réplica lee **todos** los eventos de `tripbus:events` (Redis Streams) y los entrega a sus
propias conexiones, así que escala horizontalmente sin sticky sessions.

## Página de demostración

`http://localhost:18007/` — cliente de prueba del MVP: login, búsqueda, mapa de asientos en vivo,
retención con cuenta regresiva, pago y tiquete QR, posición del bus y novedades en vivo.
La página pide `/demo-config.json` para saber a qué API llamar: devuelve `PUBLIC_API_URL`
(por defecto el api-gateway en `http://localhost:18000`).

## Flujo de ramas y entornos

```
rama de trabajo --PR--> develop (QA) --PR de release--> main (PROD)
```

- `main` sigue siendo la rama por defecto en GitHub, pero **los PR de trabajo van con base `develop`**.
- `develop` es QA y `main` es PROD: a `main` solo llega código por un PR de release desde `develop`.

| Evento | Pruebas + JaCoCo + Trivy | SonarQube Cloud | Imagen publicada en GHCR |
|---|---|---|---|
| PR con base `develop` | ✔ | ✔ (quality gate) | no |
| push a `develop` (merge de un PR) | ✔ | ✔ (quality gate) | `:qa`, `:develop`, `:sha-<corto>` |
| PR con base `main` (release) | ✔ | — | no |
| push a `main` (merge del release) | ✔ | — | `:prod`, `:latest`, `:sha-<corto>` |
| `workflow_dispatch` | ✔ | solo si se lanza sobre `develop` | no |

**Por qué Sonar solo corre en `develop`:** el plan Free de SonarQube Cloud analiza una única rama,
y la rama principal del proyecto en Sonar es `develop`. Lo que llega a `main` es exactamente el
código que ya pasó el quality gate en `develop`, así que en `main` el CI hace `mvn verify` sin Sonar
y lo indica con un aviso (`::notice::`). El control de cobertura de JaCoCo (`jacoco:check`) sí corre
en todas las ramas.

**Cómo hacer un release:**

1. Asegúrate de que el último CI de `develop` esté en verde (pruebas, cobertura, quality gate y
   Trivy) y de haber probado la imagen `:qa`.
2. Abre un PR con base `main` y rama de origen `develop` (título p. ej. `release: <resumen>`).
3. Cuando el CI del PR pase, haz merge (sin squash, para que `main` y `develop` compartan historial).
   El push a `main` publica la imagen `:prod` / `:latest`.

## Entornos (IDE / QA / PROD)

| Entorno | Cómo corre | Perfil Spring | Secretos |
|---|---|---|---|
| IDE | `mvn spring-boot:run` contra los contenedores de QA | (ninguno) | valores por defecto de QA en `application.yml` |
| QA | Docker Compose en tu computador | `qa` (+ `desplegado`) | `.env` (copia de `.env.example`) |
| PROD | Docker Compose en el servidor | `prod` (+ `desplegado`) | solo en el servidor |

Puertos de este repo (el contenedor escucha en el mismo puerto que publica):

| Servicio | QA (publicado) | Dentro de Docker | PROD |
|---|---|---|---|
| realtime-gateway | 18007 | 18007 | no se publica: los navegadores entran por el API Gateway (`ws(s)://<gateway>:18000/ws`) |
| Redis (compartido, con contraseña) | 18100 | 6379 | nunca se publica |

Este servicio no tiene base de datos. Otros servicios que usa: identity (JWKS, `IDENTITY_JWKS_URI`,
por defecto `http://localhost:18001/.well-known/jwks.json`) para validar el JWT, y seat-inventory
(`SEAT_INVENTORY_URL`, 18003), fleet (`FLEET_URL`, 18005) y flash-offers (`FLASH_OFFERS_URL`, 18006)
para los snapshots. En `qa`/`prod` no tienen valor por defecto y son obligatorias.

### Levantar QA

```bash
cp .env.example .env && docker compose up -d --build
docker compose ps                                   # esperar a "healthy"
curl http://localhost:18007/actuator/health
curl http://localhost:18007/demo-config.json        # {"apiUrl":"http://localhost:18000"}
```

Con este compose solo funcionan el actuator, la página de demostración, `/demo-config.json` y el
handshake del WebSocket (sin token abre y espera `connect`; con un token inválido responde 401).
Conectarse con un JWT real, los snapshots y los eventos en vivo necesitan identity, seat-inventory,
fleet, flash-offers y el api-gateway: pruébalo con el compose general de **API-gateway**. El servicio
llega a los servicios de tu PC por `host.docker.internal`.

No lo corras a la vez que el compose general de **API-gateway** (usan los mismos puertos).
`docker compose down` detiene conservando los datos; `down -v` los borra.

### Imágenes Docker (GHCR)

El CI publica `ghcr.io/trip-bus/realtime-gateway` solo en push (nunca desde un PR):

| Etiqueta | Rama | Uso |
|---|---|---|
| `:qa` (y `:develop`) | `develop` | Última versión de QA. |
| `:prod` (y `:latest`) | `main` | Última versión de PROD; es la que se despliega en el servidor. |
| `:sha-<corto>` | ambas | Versión inmutable de un commit concreto (para fijar o hacer rollback). |

`docker compose up --build` en tu computador construye la imagen local `ghcr.io/trip-bus/realtime-gateway:qa-local`;
para usar la de QA ya publicada: `docker pull ghcr.io/trip-bus/realtime-gateway:qa`.

### Correr desde el IDE contra QA

Con el compose de QA arriba, `docker compose stop realtime-gateway` (libera el 18007) y luego
`mvn spring-boot:run` sin perfil: `application.yml` ya apunta a `localhost:18100` (Redis). Define
`REDIS_PASSWORD=tripbus_redis_qa_pw` (el compose de QA lo exige con contraseña).

### Variables

En `qa` y `prod` se activa el perfil `desplegado`: si falta alguna variable obligatoria el servicio
no arranca y dice cuáles faltan (`RequiredSecretsCheck`).

| Variable | Obligatoria en qa/prod | Por defecto (IDE) |
|---|---|---|
| `REDIS_PASSWORD` | ✔ | (vacía) |
| `IDENTITY_JWKS_URI` | ✔ | `http://localhost:18001/.well-known/jwks.json` |
| `SEAT_INVENTORY_URL` | ✔ | `http://localhost:18003` |
| `FLASH_OFFERS_URL` | ✔ | `http://localhost:18006` |
| `FLEET_URL` | ✔ | `http://localhost:18005` |
| `PUBLIC_API_URL` | ✔ | `http://localhost:18000` |
| `WS_ALLOWED_ORIGINS` | ✔ | `*` (QA: `http://localhost:5173,http://localhost:18007`) |

Opcionales con valor por defecto: `SERVER_PORT` (18007), `REDIS_HOST` (localhost), `REDIS_PORT` (18100),
`EVENTS_STREAM` (`tripbus:events`), `WS_ALLOW_QUERY_TOKEN` (IDE y QA `true`, PROD `false`; ver
[Conectarse y autenticarse](#conectarse-y-autenticarse-en-ws)), `WS_MAX_CONNECTIONS_PER_IP` (20),
`WS_MAX_CONNECTIONS_PER_USER` (5), `WS_CLIENT_IP_HEADER` (vacía; ver [Límites](#límites)),
`OTEL_EXPORTER_OTLP_ENDPOINT` (vacía = sin exportar trazas), `TRACING_SAMPLING` (0.1), `LOG_FORMAT`
(`text`; PROD `json`) y `VAPID_PUBLIC_KEY`, `VAPID_PRIVATE_KEY`, `VAPID_SUBJECT` (vacías = push apagado). Este servicio no usa `DB_*` (no tiene base de datos; `DB_HOST`
reemplaza a la antigua `DB_URL` solo en los servicios que la tienen) ni `INTERNAL_API_TOKEN`.

## Swagger

Este servicio **no tiene Swagger/OpenAPI** en ningún entorno (por eso no usa `OPENAPI_SERVER_URL`): no
expone una API REST de negocio, solo el WebSocket `/ws`, los tres endpoints de
[Web Push](#notificaciones-push-web-push) y `/demo-config.json`, todos descritos en este README. La autenticación es el JWT del
mensaje `connect` (o de `?token=` en el handshake, `HandshakeAuthInterceptor`, solo si está permitido), no Spring Security. La documentación de las APIs REST de
TripBus está en el Swagger de cada servicio (ver el README de **API-gateway**).

## Observabilidad

- `/actuator/health` (con `liveness` y `readiness`), `/actuator/prometheus`. Métricas de las conexiones
  (HT-11, de cada réplica; Prometheus las suma):

  | Métrica | Tipo | Etiquetas |
  |---|---|---|
  | `tripbus_ws_conexiones` | gauge | `autenticada` (`true`/`false`) |
  | `tripbus_ws_suscripciones` | gauge | `tipo_topico` (`asientos`, `cupo`, `avisos`, `alertas`, `flota`, `ofertas_activas`…) |
  | `tripbus_ws_mensajes_enviados_total` | counter | `tipo_topico` |
  | `tripbus_ws_cierres_total` | counter | `codigo` (1000, 4401, 4408, 4429…) |
  | `tripbus_ws_replay_total` | counter | `resultado` (`completo`, `snapshot`, `rechazado`) |

  Se mantienen las anteriores (`tripbus_realtime_connections`, `tripbus_realtime_topics`,
  `tripbus_realtime_messages_delivered_total`).
- **Trazas (HT-10):** Micrometer Tracing con OpenTelemetry (`micrometer-tracing-bridge-otel`) y exportador
  OTLP (`opentelemetry-exporter-otlp`), versiones de Spring Boot. Se exportan por OTLP/HTTP a
  `${OTEL_EXPORTER_OTLP_ENDPOINT}/v1/traces` (vacía = no se exporta nada, `ObservabilityEnvironment`) con
  muestreo `${TRACING_SAMPLING:0.1}`. La difusión de un evento **continúa el `traceparent`** que traiga la
  entrada del stream, y el snapshot pedido por el WebSocket continúa el `traceparent` del mensaje del cliente
  (o empieza una traza nueva) y lo **propaga** al servicio dueño del dato (`RestClient` con observaciones).
- **Logs:** con `LOG_FORMAT=json` (por defecto en PROD) cada línea es JSON **ECS**
  (`logging.structured.format.console=ecs`, `service.name=realtime-gateway`) con el MDC (`requestId`,
  `traceId`, `spanId`); si no, texto con `[requestId] [traceId]`. Nunca se escriben tokens, la query string
  ni el endpoint completo de una suscripción push.
- `/actuator/info`: versión y fecha del build, y `app.commit` (el commit con el que se construyó la imagen).
- Cada petición HTTP (incluido el handshake de `/ws`) lleva un `requestId`: se reutiliza el header
  `X-Request-Id` o se genera uno, y se devuelve en la respuesta. Las llamadas salientes de snapshot
  (seat-inventory, flash-offers, fleet) lo **propagan** cuando hay uno en curso; los snapshots que se
  piden desde un mensaje del WebSocket corren fuera de una petición HTTP, así que el servicio destino
  genera el suyo (pero sí recibe el `traceparent`).

## Pruebas

```bash
mvn -B verify      # unitarias + integración con Redis real (Testcontainers: Docker abierto)
```

- `*Test` / `*Tests`: unitarias (`TopicsTest`, `RequiredSecretsCheckTest`, `RequestIdFilterTest`,
  `RequestIdPropagationInterceptorTest`, `ClientSessionTest`, `EventFanoutTest`,
  `HandshakeAuthInterceptorTest`, `TokenRevocationsTest`, `PerfilesConfigTest`, `SnapshotServiceTest`,
  `SeqTest`, `SessionRegistryTest`, `RealtimeHandlerLimitesTest`, `ObservabilityEnvironmentTest`,
  `TraceContextsTest`, `RealtimeLimitsTest`, `PushMessagesTest`, `PushNotifierTest`, `PushControllerTest`).
- `RealtimeIT`: levanta Redis 7 con Testcontainers y prueba de extremo a extremo la conexión
  autenticada con `?token=` (`allow-query-token=true`), el snapshot inicial, la difusión desde Redis
  Streams (sin exponer `userId`), el ping y el rechazo de tokens y tópicos inválidos.
- `RealtimeSinQueryTokenIT`: como PROD (`allow-query-token=false`): `?token=` sin `connect` se cierra
  al vencer el plazo, y `connect` sin token en la URL queda autenticado.
- `RealtimePublicoIT`: seguimiento público. Un anónimo (`connect` sin token) recibe snapshot y eventos
  de `bus:{id}:posicion`, sigue abierto pasados los 10 s del plazo de autenticación, no pasa de 5
  suscripciones y puede autenticarse después; un token inválido sigue cerrando con 4401.
- `RealtimeTiquetesIT`: aviso privado de tiquete (US-033). El dueño recibe `TicketIssued` en
  `usuario:{uuid}:tiquetes` sin `qrCode` ni `userId` y sin snapshot; otro usuario, un ADMIN y un
  anónimo reciben `FORBIDDEN_TOPIC` sin que se cierre la conexión.
- `RealtimeRevocacionIT`: tokens revocados y orden (US-006). Un token con `tripbus:revoked:{jti}` se
  cierra con 4401 al conectar; `SessionRevoked` cierra solo la sesión de ese `jti` (otro token del
  mismo usuario y los anónimos siguen abiertos) y `UserSuspended` todas las del usuario; los eventos
  llevan `seq` creciente y el snapshot `asOfSeq`. Redis caído al consultar la revocación (permite la
  conexión con WARN) está en `TokenRevocationsTest`.
- `RealtimePosicionViajeIT`: posición por viaje (US-173/US-174) y cuentas suspendidas (US-104). Un
  anónimo sigue `viaje:{id}:posicion` con el snapshot de un fleet simulado (servidor HTTP local) y recibe
  `PositionUpdated` (sin `conductorId`) y `TrackingStatusChanged`; `PositionUpdated` sigue llegando a
  `bus:{id}:posicion`; con `tripbus:suspended:{sub}` la conexión se rechaza con `ACCOUNT_SUSPENDED` y
  4401 tanto por `connect` como por `?token=`.
- `RealtimeReconexionIT`: `resume` (US-184) con los eventos en el búfer (los reenvía en orden y termina con
  `resumed`), fuera de él (búfer de 3: manda el `snapshot` con `asOfSeq`) y sin permiso (`FORBIDDEN_TOPIC`,
  `UNAUTHENTICATED`, `INVALID_SEQ`, sin quedar suscrito).
- `RealtimeLimitesIT`: límite de conexiones por IP (con `client-ip-header`) y por usuario (cierre 4429), 30
  mensajes por 10 s (`RATE_LIMITED` sin cerrar) y las métricas `tripbus_ws_*` en `/actuator/prometheus`.
- `RealtimeTrazasIT`: el snapshot propaga el `traceparent` del cliente (o uno nuevo) a un fleet simulado y la
  difusión de un evento deja en los logs el `traceId` de su `traceparent`.
- `RealtimePushIT`: registro de suscripciones (401, 400, máximo 5), push a un servicio de push simulado
  **solo sin conexión abierta**, suscripción que responde 410/404 borrada, conductor y operadores (solo alertas
  `CRITICA`) y un solo push con dos réplicas. `RealtimePushApagadoIT`: sin llaves VAPID, 503 y el WebSocket
  igual.
- `RealtimeSalaEsperaIT`: `OfferQueueUpdated` en `oferta:{id}:cupo`, `OfferQueueAdmitted` solo al admitido y
  `OfferViewers` sumando este gateway y otra réplica simulada.
- Contra Redis real sin Spring: `ReplayBufferIT`, `ConnectionPresenceIT`, `OfferViewersIT` (dos réplicas y
  una que deja de reportar) y `PushSubscriptionsIT`.

Toda la plataforma: ver el README de **API-gateway**.

## Calidad

| Herramienta | Qué revisa | Dónde |
|---|---|---|
| JaCoCo | Cobertura de las pruebas y **mínimo obligatorio** (`jacoco:check`) | local: `target/site/jacoco/index.html` (tras `mvn verify`); CI: tabla en el resumen de cada ejecución y artifact `jacoco` |
| SonarQube Cloud | Bugs, code smells y cobertura; quality gate "Sonar way" (código nuevo ≥ 80 %) | proyecto `Trip-Bus_Realtime-Gateway` de la organización `trip-bus` |
| Gitleaks | Secretos en todo el historial git | job `secretos` del CI |
| Trivy | Vulnerabilidades CRITICAL/HIGH con corrección disponible en la imagen Docker | job `docker` del CI |

### Cobertura mínima (JaCoCo)

`mvn verify` falla si la cobertura total baja de los mínimos del `pom.xml`, en **todas** las ramas
(también en `main`, donde Sonar no corre):

| Métrica | Medido (2026-11-04) | Mínimo |
|---|---|---|
| Líneas | 99,6 % | `jacoco.minimo.lineas` = 0.95 |
| Ramas | 98,4 % | `jacoco.minimo.ramas` = 0.95 |

**La cobertura nunca puede bajar: si agregas pruebas, sube el mínimo.** Corre `mvn verify`, mira
el total en `target/site/jacoco/index.html` (o la tabla del CI), redondéalo hacia abajo al 5 % y
actualiza la propiedad en el pom en el mismo PR. Las mejores candidatas para subirla son
`RealtimeHandler` (suscripción y snapshot) y `EventFanout` (arranque del stream).

Fuera de la cobertura (misma en `sonar.coverage.exclusions` y en los `<excludes>` de JaCoCo) solo
está `RealtimeGatewayApplication` (el arranque de Spring). `RealtimeProperties` es un record de
configuración pero su constructor tiene lógica (valores por defecto), así que **sí** cuenta.

### Vulnerabilidades aceptadas (`.trivyignore.yaml`)

Lista **temporal** de CVE que Trivy no debe tratar como error. Solo entran las que **no tienen
arreglo en Spring Boot 3.x** y aparecen realmente en esta imagen; cada una explica en `statement`
por qué no aplica a este servicio. Todas **vencen el 2027-01-31** (`expired_at`): desde esa fecha
Trivy vuelve a fallar si no se migró a Spring Boot 4.1. Lo que sí tiene parche nunca se agrega aquí:
se corrige subiendo la versión (ver las propiedades `*.version` de parches de seguridad en `pom.xml`).

El análisis de Sonar solo corre en `develop` (push y PR con base `develop`, ver
[Flujo de ramas y entornos](#flujo-de-ramas-y-entornos)) y si el repo tiene el secreto `SONAR_TOKEN`;
en otro caso el CI hace `mvn verify` y deja un aviso.

### Secretos del CI (`SONAR_TOKEN`, `DOCKERHUB_USERNAME`, `DOCKERHUB_TOKEN`)

`SONAR_TOKEN` es el token de análisis de SonarQube Cloud (My Account → Security).

Los runners de GitHub comparten IP y agotan el límite de descargas anónimas de Docker Hub
(`toomanyrequests: You have reached your unauthenticated pull rate limit`), lo que rompe
Testcontainers (`redis:7-alpine`) y el build de la imagen (`maven`, `eclipse-temurin`).
Con estos secretos los jobs `secretos`, `build-test` y `docker` inician sesión en Docker Hub antes de
descargar nada y usan el límite de una cuenta autenticada. Gitleaks se descarga desde GHCR
(`ghcr.io/gitleaks/gitleaks`), que no tiene ese límite.

| Secreto | Valor |
|---|---|
| `SONAR_TOKEN` | Token de SonarQube Cloud de la organización `trip-bus` |
| `DOCKERHUB_USERNAME` | Usuario de Docker Hub |
| `DOCKERHUB_TOKEN` | Personal access token con permiso **Public Repo Read-only** (solo lectura; nunca la contraseña) |

Los de Docker Hub son **opcionales pero recomendados**: si faltan, el CI descarga sin autenticar,
deja un aviso y puede fallar por el límite. Se crean en Settings → Secrets and variables → Actions.

Escaneos locales con Docker:

```bash
docker run --rm -v "$PWD:/repo" ghcr.io/gitleaks/gitleaks:v8.30.1 git /repo --redact
docker build -t realtime-gateway:local . && \
  docker run --rm -v /var/run/docker.sock:/var/run/docker.sock -v "$PWD:/src" aquasec/trivy:0.70.0 \
  image --severity CRITICAL,HIGH --ignore-unfixed --ignorefile /src/.trivyignore.yaml realtime-gateway:local
```
