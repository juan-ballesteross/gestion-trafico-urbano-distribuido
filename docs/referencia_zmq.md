# Referencia ZeroMQ — Proyecto Tráfico Urbano


| Puerto | Patrón      | Quién hace `bind`      | Quién hace `connect`        | Archivo principal        |
|--------|-------------|------------------------|-----------------------------|--------------------------|
| 5555   | PUB → XSUB  | `broker` (XSUB)        | sensores (PUB)              | `broker.c`, `sensor_*.c` |
| 5556   | XPUB → SUB  | `broker` (XPUB)        | `analitica` (SUB)           | `broker.c`, `analitica.c`|
| 5557   | PUSH → PULL | `control_semaforos`    | `analitica`                 | `control_semaforos.c`    |
| 5558   | PUSH → PULL | `db_principal`         | `analitica`                 | `db_principal.c`         |
| 5559   | PUSH → PULL | `db_replica`           | `analitica`                 | `db_replica.c`           |
| 5560   | REQ → REP   | `analitica` (REP)      | `monitor` (REQ)             | `analitica.c`, `monitor.c`|
| 5562   | PUSH → PULL | `semaforo`             | `control_semaforos`         | `semaforo.c`             |
| 5563   | REQ → REP   | `db_principal` (REP)   | `monitor` (REQ)             | `db_principal.c`         |
| 5564   | REQ → REP   | `db_replica` (REP)     | `monitor` (REQ, failover)   | `db_replica.c`           |

**Regla general del proyecto:** el proceso que **espera** conexiones hace `zmq_bind`; el que **inicia** la conexión hace `zmq_connect`.

---

## 2. API común (todos los procesos)

```c
#include <zmq.h>

/* Crear contexto (uno por proceso) */
void *ctx = zmq_ctx_new();

/* Crear socket según el patrón */
void *socket = zmq_socket(ctx, ZMQ_PUB);   /* o ZMQ_SUB, ZMQ_PUSH, etc. */

/* Conectar o enlazar */
zmq_connect(socket, "tcp://host:puerto");   /* cliente / emisor que busca al servidor */
zmq_bind(socket,    "tcp://*:puerto");      /* servidor que escucha en todas las interfaces */

/* Enviar y recibir (retornan bytes o -1 en error) */
zmq_send(socket, datos, longitud, flags);
zmq_recv(socket, buffer, tam_buffer, flags);

/* Cerrar al terminar (en este proyecto casi nunca se alcanza por bucles infinitos) */
zmq_close(socket);
zmq_ctx_destroy(ctx);

/* Mensaje de error */
zmq_strerror(zmq_errno());
```

### Flags usados en el proyecto

| Flag           | Valor | Uso en el proyecto                                      |
|----------------|-------|---------------------------------------------------------|
| `ZMQ_SNDMORE`  | 1     | Primera parte de mensaje **multipart** (tópico + JSON)  |
| `0`            | 0     | Última parte multipart, o mensaje de una sola parte     |

---

## 3. Patrón PUB / SUB (sensores → broker → analítica)

### Idea

- **PUB** publica sin saber si hay receptores (fire-and-forget).
- **SUB** se suscribe por **prefijo de tópico** (`ZMQ_SUBSCRIBE`).
- En mensajes **multipart**, la primera parte es el tópico y la segunda el JSON.

### Publicador — sensores (`sensor_camara.c`, `sensor_gps.c`, `sensor_espira.c`)

```c
void *pub = zmq_socket(ctx, ZMQ_PUB);
zmq_connect(pub, "tcp://broker_host:5555");   /* PORT_BROKER_XSUB */

/* PUB pierde mensajes si envía antes de que exista suscriptor: pausa inicial */
dormir_ms(1000);

char topico[] = "EVENT_LONGITUD_COLA";   /* o EVENT_DENSIDAD_DE_TRAFICO, EVENT_CONTEO_VEHICULAR */
char json[] = "{ ... }";

/* Multipart: [tópico][JSON] */
zmq_send(pub, topico, strlen(topico), ZMQ_SNDMORE);  /* parte 1, hay más */
zmq_send(pub, json,   strlen(json),   0);            /* parte 2, fin del mensaje */
```

### Broker proxy — `broker.c`

El broker no interpreta JSON; solo reenvía bytes de XSUB a XPUB.

```c
void *xsub = zmq_socket(ctx, ZMQ_XSUB);
void *xpub = zmq_socket(ctx, ZMQ_XPUB);

zmq_bind(xsub, "tcp://*:5555");   /* sensores PUB → connect aquí */
zmq_bind(xpub, "tcp://*:5556");   /* analítica SUB → connect aquí */

/* Bloquea y reenvía todo automáticamente */
zmq_proxy(xsub, xpub, NULL);
```

### Suscriptor — `analitica.c`

```c
void *sub = zmq_socket(ctx, ZMQ_SUB);
zmq_connect(sub, "tcp://pc1_host:5556");   /* PORT_BROKER_XPUB */

/* Solo recibe mensajes cuyo tópico empiece por "EVENT_" */
zmq_setsockopt(sub, ZMQ_SUBSCRIBE, "EVENT_", 6);

/* Recibir multipart (mismo orden que envió el PUB) */
char topico[256], json[BUFFER_SIZE];

int t_size = zmq_recv(sub, topico, sizeof(topico) - 1, 0);
topico[t_size] = '\0';

int j_size = zmq_recv(sub, json, sizeof(json) - 1, 0);
json[j_size] = '\0';
```

---

## 4. Patrón PUSH / PULL (colas asíncronas)

### Idea

- **PUSH** reparte mensajes entre varios PULL (round-robin) si hubiera varios.
- Un mensaje = **una sola parte** (JSON en texto plano, sin tópico).
- Ideal para pipelines: analítica → control → semáforo, analítica → bases de datos.

### Emisor PUSH — `analitica.c`

```c
void *push = zmq_socket(ctx, ZMQ_PUSH);
zmq_connect(push, "tcp://host:5557");   /* control, BD principal o réplica */

char cmd[] = "{\"comando_id\":\"CMD001\", ...}";
zmq_send(push, cmd, strlen(cmd), 0);
```

### Receptor PULL — `control_semaforos.c`, `db_principal.c`, `semaforo.c`

```c
void *pull = zmq_socket(ctx, ZMQ_PULL);
zmq_bind(pull, "tcp://*:5557");   /* puerto según el servicio */

char buffer[BUFFER_SIZE];
int size = zmq_recv(pull, buffer, sizeof(buffer) - 1, 0);
buffer[size] = '\0';
/* procesar buffer (JSON) */
```

### Relay PUSH→PULL — `control_semaforos.c`

Recibe de analítica y reenvía a semáforos sin transformar el mensaje:

```c
int size = zmq_recv(pull_socket, buffer, sizeof(buffer) - 1, 0);
zmq_send(push_socket, buffer, size, 0);   /* mismo payload, mismo tamaño */
```

---

## 5. Patrón REQ / REP (petición-respuesta síncrona)

### Idea

- **REQ** envía una petición y **debe** recibir exactamente una respuesta antes de volver a enviar.
- **REP** recibe una petición, procesa y **debe** responder siempre (aunque sea un JSON de error).
- Usado entre **Monitor** (REQ) y **Analítica** o **BD** (REP).

### Servidor REP — `analitica.c`, `db_principal.c`

```c
void *rep = zmq_socket(ctx, ZMQ_REP);
zmq_bind(rep, "tcp://*:5560");   /* analítica; BD usa 5563 */

char buffer[BUFFER_SIZE];
int size = zmq_recv(rep, buffer, sizeof(buffer) - 1, 0);
buffer[size] = '\0';

/* Parsear JSON, ejecutar lógica... */

const char *resp = "{\"status\":\"OK\"}";
zmq_send(rep, resp, strlen(resp), 0);   /* obligatorio: una respuesta por cada recv */
```

### Cliente REQ — `monitor.c`

```c
void *req = zmq_socket(ctx, ZMQ_REQ);
zmq_connect(req, "tcp://pc2_host:5560");

const char *peticion = "{\"tipo\":\"CONSULTAR_ESTADO\",\"interseccion\":\"INT_C3\"}";
zmq_send(req, peticion, strlen(peticion), 0);

char respuesta[BUFFER_SIZE];
int size = zmq_recv(req, respuesta, sizeof(respuesta) - 1, 0);
respuesta[size] = '\0';
```

### REQ con timeout (failover del monitor)

Si no hay respuesta a tiempo, se considera caído el servicio y se reconecta a otro host:

```c
zmq_send(req_socket, mensaje, strlen(mensaje), 0);

zmq_pollitem_t items[] = { { req_socket, 0, ZMQ_POLLIN, 0 } };
int rc = zmq_poll(items, 1, timeout_ms);   /* ej. 3000 ms */

if (rc > 0 && (items[0].revents & ZMQ_POLLIN)) {
    zmq_recv(req_socket, respuesta, resp_size - 1, 0);
} else {
    /* timeout → activar failover, cerrar socket y zmq_connect a réplica */
}
```

### Reconectar REQ (failover BD)

```c
zmq_close(*req_socket);
*req_socket = zmq_socket(ctx, ZMQ_REQ);
zmq_connect(*req_socket, "tcp://pc2_host:5564");   /* BD réplica */
```

> **Importante:** tras un timeout en REQ, el socket queda en estado inconsistente; en el proyecto se **cierra y crea uno nuevo** antes de seguir.

---

## 6. `zmq_poll` — varios sockets en un solo proceso

Usado en **`analitica.c`** (SUB + REP) y **`db_principal.c`** (PULL + REP).

```c
zmq_pollitem_t items[] = {
    { sub_socket, 0, ZMQ_POLLIN, 0 },   /* o pull_socket */
    { rep_socket, 0, ZMQ_POLLIN, 0 }
};

while (1) {
    int rc = zmq_poll(items, 2, 1000);   /* esperar hasta 1000 ms */
    if (rc < 0) break;                   /* error */
    if (rc == 0) continue;               /* timeout, ningún dato */

    if (items[0].revents & ZMQ_POLLIN) {
        /* hay datos en SUB/PULL */
    }
    if (items[1].revents & ZMQ_POLLIN) {
        /* hay petición REQ→REP pendiente */
    }
}
```

---

## 7. Tipos de socket usados (resumen)

| Constante ZMQ | Rol en el proyecto                          |
|---------------|---------------------------------------------|
| `ZMQ_PUB`     | Sensores publican eventos                   |
| `ZMQ_XSUB`    | Broker recibe de sensores                   |
| `ZMQ_XPUB`    | Broker reenvía a suscriptores               |
| `ZMQ_SUB`     | Analítica recibe eventos del broker         |
| `ZMQ_PUSH`    | Analítica, control — envían JSON            |
| `ZMQ_PULL`    | Control, BD, semáforo — reciben JSON        |
| `ZMQ_REQ`     | Monitor — consultas al operador             |
| `ZMQ_REP`     | Analítica, BD — responden al monitor        |

---

## 8. Formato de mensajes en el proyecto

### Eventos de sensores (PUB multipart)

| Tópico                      | Origen        | Campos principales del JSON                          |
|----------------------------|---------------|------------------------------------------------------|
| `EVENT_CONTEO_VEHICULAR`   | `sensor_espira`  | `vehiculos_contados`, `timestamp_inicio/fin`      |
| `EVENT_LONGITUD_COLA`      | `sensor_camara`  | `volumen`, `velocidad_promedio`                   |
| `EVENT_DENSIDAD_DE_TRAFICO`| `sensor_gps`     | `nivel_congestion`, `velocidad_promedio`          |

### Comandos y persistencia (PUSH, una parte)

JSON de comando de semáforo u objeto de evento/acción enviado por analítica a control y BDs.

### Peticiones del monitor (REQ → REP)

Ejemplos de `tipo` en el JSON:

- `CONSULTAR_ESTADO` → analítica
- `FORZAR_CAMBIO` → analítica
- `CONSULTAR_INTERSECCION` → BD
- `CONSULTAR_HISTORICO` → BD
- `HEALTH_CHECK` → BD

---

## 9. Buenas prácticas ya aplicadas en el código

1. **PUB:** esperar ~1 s tras `connect` antes del primer `zmq_send` (slow joiner).
2. **SUB:** usar `ZMQ_SUBSCRIBE` con prefijo `"EVENT_"` para filtrar tópicos.
3. **REP:** siempre `zmq_send` después de cada `zmq_recv`, incluso ante error.
4. **REQ:** un send → un recv; si hay timeout, recrear el socket.
5. **Errores:** comprobar retorno de `connect`/`bind` y usar `zmq_strerror(zmq_errno())`.
6. **Buffers:** terminar en `'\0'` tras `zmq_recv` si se tratan como C-strings (`buffer[size] = '\0'`).

---

## 10. Dónde ver el código real

| Patrón              | Archivo a abrir              |
|---------------------|------------------------------|
| PUB + multipart     | `src/pc1/sensor_camara.c`    |
| XSUB/XPUB + proxy   | `src/pc1/broker.c`           |
| SUB + poll          | `src/pc2/analitica.c`        |
| PUSH (varios destinos) | `src/pc2/analitica.c`     |
| PULL → PUSH relay   | `src/pc2/control_semaforos.c`|
| PULL + REP + poll   | `src/pc3/db_principal.c`     |
| REQ + timeout       | `src/pc3/monitor.c`          |
| Puertos             | `src/config.h`               |

---

*Última revisión alineada con la estructura del repositorio (PC1/PC2/PC3).*
