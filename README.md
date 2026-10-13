# Realtime Gateway

[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=Trip-Bus_Realtime-Gateway&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=Trip-Bus_Realtime-Gateway)

Mantiene las conexiones WebSocket con los clientes: autentica el canal, gestiona suscripciones,
envía el snapshot inicial y difunde los eventos. **No tiene lógica de negocio ni base de datos.**

**Stack:** Java 21 · Spring Boot 3.5.16 · Redis 7

**HU:** US-020, US-021, US-033 (aviso de tiquete), US-039 (seguimiento público), US-047 (parte), US-056 (parte) · **Puerto:** 18007 · **BD:** ninguna (solo Redis)

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
| `{"type":"unsubscribe","topic":"..."}` | `event` con `{topic, event, eventId, data}` |
| `{"type":"ping"}` / `{"type":"pong"}` | `ping` cada 20 s (sin respuesta en 45 s ⇒ se cierra con 4408) |

#### Tópicos

| Tópico | Acceso | Eventos | Snapshot |
|---|---|---|---|
| `itinerario:{id}:asientos` | Público | `SeatHeld`, `SeatReleased`, `SeatSold`, `HoldExpired`, `SeatStateChanged` | Mapa de asientos (seat-inventory) |
| `oferta:{id}:cupo` | Público | `OfferQuotaUpdated`, `OfferClosed`, `OfferSoldOut`, `OfferActivated` | Cupo (flash-offers) |
| `viaje:{id}:eventos` | Público | `TripEventReported` | Línea de tiempo (fleet) |
| `bus:{id}:posicion` | Público | `PositionUpdated` | Última posición (fleet) |
| `usuario:{uuid}:tiquetes` | **Privado**: solo la sesión autenticada cuyo `userId` es `{uuid}` | `TicketIssued` (US-033) | Ninguno: solo `subscribed` |

- **Públicos:** los sigue cualquier sesión, también la anónima. Nunca llevan datos de pasajeros:
  `userId`, `holdId` y `bookingId` se quitan de los eventos a cualquier profundidad (`EventFanout`).
- **Privado `usuario:{uuid}:tiquetes`:** aviso en vivo al comprador cuando booking emite su tiquete.
  Un anónimo u otro usuario (ADMIN incluido) ⇒ `FORBIDDEN_TOPIC` sin cerrar la conexión. El `data`
  lleva **solo** `bookingId`, `ticketId`, `itineraryId` y `seatNumbers` (nunca `qrCode` ni `userId`);
  la app pide el tiquete completo con `GET /tickets/{ticketId}`.

```js
ws.send(JSON.stringify({ type: "subscribe", topic: `usuario:${userId}:tiquetes` }));
// servidor → {"type":"event","topic":"usuario:…:tiquetes","event":"TicketIssued",
//             "data":{"bookingId":…,"ticketId":…,"itineraryId":…,"seatNumbers":["5"]}}
```

Token inválido en `connect` ⇒ `INVALID_TOKEN` y cierre 4401 (en `?token=`, si está permitido, ⇒ 401
en el handshake); token vencido ⇒ cierre 4401 (el cliente reconecta con uno nuevo), también con
temas privados suscritos.

### Seguimiento público

Decisión de producto (US-039, US-047, US-056): seguir el bus no exige cuenta, porque sirve a quien lo
espera en la vía. Se abre `/ws` y se manda `connect` **sin token** (o con `"token":""`):

```js
ws.onopen = () => ws.send(JSON.stringify({ type: "connect" }));
// servidor → {"type":"connected","anonymous":true,"heartbeatSeconds":20,"maxSubscriptions":5}
ws.send(JSON.stringify({ type: "subscribe", topic: `bus:${busId}:posicion` }));
```

- La sesión anónima no se cierra por `AUTH_TIMEOUT` ni por vencimiento de token (no tiene); sí por
  falta de heartbeat (4408), como cualquier otra.
- Solo puede suscribirse a **temas públicos** (`Topics.isPublic`): `bus:{id}:posicion`,
  `viaje:{id}:eventos`, `oferta:{id}:cupo`, `itinerario:{id}:asientos`. Otro tema ⇒ error
  `FORBIDDEN_TOPIC` sin cerrar la conexión.
- Máximo **5 suscripciones** a la vez (`tripbus.realtime.max-anonymous-subscriptions`); la sexta ⇒
  `TOO_MANY_SUBSCRIPTIONS`. Las sesiones autenticadas siguen con `max-subscriptions` (20), los temas
  públicos y su propio `usuario:{uuid}:tiquetes`.
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
[Conectarse y autenticarse](#conectarse-y-autenticarse-en-ws)). Este servicio no usa `DB_*` (no tiene base de datos; `DB_HOST`
reemplaza a la antigua `DB_URL` solo en los servicios que la tienen) ni `INTERNAL_API_TOKEN`.

## Swagger

Este servicio **no tiene Swagger/OpenAPI** en ningún entorno: no expone una API REST de negocio, solo
el WebSocket `/ws` (protocolo descrito arriba) y `/demo-config.json`. La autenticación es el JWT del
mensaje `connect` (o de `?token=` en el handshake, `HandshakeAuthInterceptor`, solo si está permitido), no Spring Security. La documentación de las APIs REST de
TripBus está en el Swagger de cada servicio (ver el README de **API-gateway**).

## Observabilidad

- `/actuator/health` (con `liveness` y `readiness`), `/actuator/prometheus` (conexiones y tópicos
  activos, mensajes entregados).
- `/actuator/info`: versión y fecha del build, y `app.commit` (el commit con el que se construyó la imagen).
- Cada petición HTTP (incluido el handshake de `/ws`) lleva un `requestId`: se reutiliza el header
  `X-Request-Id` o se genera uno, y se devuelve en la respuesta. Las llamadas salientes de snapshot
  (seat-inventory, flash-offers, fleet) lo **propagan** cuando hay uno en curso; los snapshots que se
  piden desde un mensaje del WebSocket corren fuera de una petición HTTP, así que el servicio destino
  genera el suyo. Logs legibles con `[requestId]` en IDE/QA y JSON (con `requestId` y
  `"service":"realtime-gateway"`) en PROD.

## Pruebas

```bash
mvn -B verify      # unitarias + integración con Redis real (Testcontainers: Docker abierto)
```

- `*Test` / `*Tests`: unitarias (`TopicsTest`, `RequiredSecretsCheckTest`, `RequestIdFilterTest`,
  `RequestIdPropagationInterceptorTest`, `ClientSessionTest`, `EventFanoutTest`,
  `HandshakeAuthInterceptorTest`, `PerfilesConfigTest`).
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

| Métrica | Medido (2026-10-12) | Mínimo |
|---|---|---|
| Líneas | 85,6 % | `jacoco.minimo.lineas` = 0.85 |
| Ramas | 84,0 % | `jacoco.minimo.ramas` = 0.80 |

**La cobertura nunca puede bajar: si agregas pruebas, sube el mínimo.** Corre `mvn verify`, mira
el total en `target/site/jacoco/index.html` (o la tabla del CI), redondéalo hacia abajo al 5 % y
actualiza la propiedad en el pom en el mismo PR. Las mejores candidatas para subirla son
`SnapshotService`, `RealtimeHandler` (heartbeat, `connect` y límites de suscripción) y `ClientSession`.

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
