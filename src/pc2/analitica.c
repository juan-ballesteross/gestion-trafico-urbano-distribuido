#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <zmq.h>
#include "../config.h"

/**
 * Servicio de Analítica (PC2).
 *
 * Sockets ZMQ:
 *   SUB  ← conecta a Broker XPUB (PC1:5556) — recibe eventos de sensores
 *   PUSH → conecta a Control (localhost:5557) — envía comandos de semáforo
 *   PUSH → conecta a BD Principal (PC3:5558) — persiste eventos
 *   PUSH → conecta a BD Réplica (localhost:5559) — réplica de eventos
 *   REP  ← bind en :5560 — responde consultas/forzar cambio del Monitor
 *
 * Lógica:
 *   1. Recibe evento del broker (SUB)
 *   2. Actualiza estado de la intersección
 *   3. Evalúa reglas de congestión
 *   4. Si hay cambio de estado → envía comando al control de semáforos
 *   5. Persiste evento + acción en BD Principal y BD Réplica
 *   6. Atiende peticiones REQ/REP del Monitor (zmq_poll)
 *
 * Uso: analitica [broker_host] [pc3_host]
 */

/* ── Enumeración de estados del tráfico ─────────── */
typedef enum {
    ESTADO_NORMAL,
    ESTADO_CONGESTION,
    ESTADO_OLA_VERDE,
    ESTADO_EMERGENCIA
} EstadoTrafico;

static const char *ESTADO_NOMBRES[] = {
    "NORMAL", "CONGESTION", "OLA_VERDE", "EMERGENCIA"
};

/* ── Estado por intersección ────────────────────── */
typedef struct {
    char id[16];
    int vehiculos_contados;
    int volumen_cola;
    double velocidad_promedio;
    char nivel_congestion[16];
    EstadoTrafico estado_actual;
    long ultimo_update;
    int tiene_semaforo;        /* 1 si esta interseccion tiene semaforo */
    int orientacion;           /* 0 = HORIZONTAL, 1 = VERTICAL */
} InterseccionState;

static InterseccionState grid[GRID_FILAS * GRID_COLUMNAS];
static int total_intersecciones = 0;

/* Forward declaration — necesaria porque inicializar_grid() usa buscar_interseccion() */
static InterseccionState *buscar_interseccion(const char *id);

/* Prototypes usados antes de la definición */
static int columna_de_id(const char *id);
static int orientacion_de_id(const char *id);

/**
 * Inicializa el grid completo y marca cuales intersecciones tienen semaforo.
 * @param lista_semaforos CSV con IDs de intersecciones con semaforo (ej: "INT_C3,INT_C4,INT_F3")
 */
static void inicializar_grid(const char *lista_semaforos) {
    total_intersecciones = 0;
    for (int f = 0; f < GRID_FILAS; f++) {
        for (int c = 0; c < GRID_COLUMNAS; c++) {
            /* Corregir formato de ID: incluir letra de fila A..D y número de columna 1..N
               para que coincida con los IDs generados por semaforo.c (INT_A1, INT_B3, ...). */
            char fila = 'A' + f;
            snprintf(grid[total_intersecciones].id, sizeof(grid[0].id), "INT_%c%d", fila, c + 1);
            grid[total_intersecciones].vehiculos_contados = 0;
            grid[total_intersecciones].volumen_cola = 0;
            grid[total_intersecciones].velocidad_promedio = 60.0;
            strncpy(grid[total_intersecciones].nivel_congestion, "BAJA", sizeof(grid[0].nivel_congestion));
            grid[total_intersecciones].estado_actual = ESTADO_NORMAL;
            grid[total_intersecciones].ultimo_update = 0;
            grid[total_intersecciones].tiene_semaforo = 0;
            /* Establecer orientación por defecto según la columna (paridad) */
            grid[total_intersecciones].orientacion = orientacion_de_id(grid[total_intersecciones].id);
            total_intersecciones++;
        }
    }

    /* Procesar lista_semaforos (CSV) */
    char semaforos_copia[256];
    strncpy(semaforos_copia, lista_semaforos, sizeof(semaforos_copia));
    char *token = strtok(semaforos_copia, ",");

    while (token) {
        /* Trim espacios en token */
        while (*token == ' ') token++;
        /* Eliminar espacios finales */
        char *fin = token + strlen(token) - 1;
        while (fin > token && *fin == ' ') { *fin = '\0'; fin--; }

        /* Permitir sufijo de orientación en el CSV (ej: INT_C4:V). Leer sufijo y aplicar */
        char token_clean[64];
        strncpy(token_clean, token, sizeof(token_clean) - 1);
        token_clean[sizeof(token_clean) - 1] = '\0';
        char *suf = strchr(token_clean, ':');
        int orient_spec = -1; /* -1 = no especificado, 0 = H, 1 = V */
        if (suf) {
            if (*(suf + 1) == 'H' || *(suf + 1) == 'h') orient_spec = 0;
            else if (*(suf + 1) == 'V' || *(suf + 1) == 'v') orient_spec = 1;
            *suf = '\0';
        }

        InterseccionState *inter = buscar_interseccion(token_clean);
        if (inter) {
            inter->tiene_semaforo = 1;
            if (orient_spec != -1) inter->orientacion = orient_spec;
            printf("[ANALITICA] Semaforo registrado en: %s\n", token_clean);
        } else {
            fprintf(stderr, "[ANALITICA] ADVERTENCIA: Interseccion con semaforo '%s' no existe en el grid\n", token_clean);
        }
        token = strtok(NULL, ",");
    }

    printf("[ANALITICA] Grid %dx%d inicializado — %d intersecciones\n",
           GRID_FILAS, GRID_COLUMNAS, total_intersecciones);
}

static InterseccionState *buscar_interseccion(const char *id) {
    for (int i = 0; i < total_intersecciones; i++) {
        if (strcmp(grid[i].id, id) == 0) {
            return &grid[i];
        }
    }
    return NULL;
}

/* ── Extracción de valores JSON (esquema conocido) ─ */

static int json_extraer_string(const char *json, const char *clave, char *destino, size_t dest_size) {
    char patron[64];
    snprintf(patron, sizeof(patron), "\"%s\":\"", clave);
    const char *inicio = strstr(json, patron);
    if (!inicio) return 0;
    inicio += strlen(patron);
    const char *fin = strchr(inicio, '"');
    if (!fin) return 0;
    size_t len = (size_t)(fin - inicio);
    if (len >= dest_size) len = dest_size - 1;
    strncpy(destino, inicio, len);
    destino[len] = '\0';
    return 1;
}

static int json_extraer_int(const char *json, const char *clave, int *destino) {
    char patron[64];
    snprintf(patron, sizeof(patron), "\"%s\":", clave);
    const char *inicio = strstr(json, patron);
    if (!inicio) return 0;
    inicio += strlen(patron);
    *destino = atoi(inicio);
    return 1;
}

static int json_extraer_double(const char *json, const char *clave, double *destino) {
    char patron[64];
    snprintf(patron, sizeof(patron), "\"%s\":", clave);
    const char *inicio = strstr(json, patron);
    if (!inicio) return 0;
    inicio += strlen(patron);
    *destino = atof(inicio);
    return 1;
}

/* ── Evaluación de reglas de congestión ─────────── */

static EstadoTrafico evaluar_reglas(InterseccionState *inter) {
    /*
     * Reglas basadas en el enunciado:
     *   Normal:     cola < 5  AND  velocidad > 35  AND  vehiculos < 15
     *   Congestión: cola >= UMBRAL_COLA  OR  velocidad <= UMBRAL_VELOCIDAD  OR  vehiculos >= UMBRAL_VEHICULOS
     *   OLA_VERDE y EMERGENCIA: solo por comando del operador
     */

    /* Si está en modo forzado (OLA_VERDE/EMERGENCIA), no evaluar automáticamente */
    if (inter->estado_actual == ESTADO_OLA_VERDE || inter->estado_actual == ESTADO_EMERGENCIA) {
        return inter->estado_actual;
    }

    if (inter->volumen_cola >= UMBRAL_COLA ||
        inter->velocidad_promedio <= UMBRAL_VELOCIDAD ||
        inter->vehiculos_contados >= UMBRAL_VEHICULOS) {
        return ESTADO_CONGESTION;
    }

    return ESTADO_NORMAL;
}

/* ── Actualizar estado de intersección con evento ── */

static void actualizar_con_evento(const char *json) {
    char tipo[64] = {0};
    char interseccion[16] = {0};

    json_extraer_string(json, "tipo", tipo, sizeof(tipo));
    json_extraer_string(json, "interseccion", interseccion, sizeof(interseccion));

    InterseccionState *inter = buscar_interseccion(interseccion);
    if (!inter) {
        fprintf(stderr, "[ANALITICA] Interseccion desconocida: %s\n", interseccion);
        return;
    }

    inter->ultimo_update = (long)time(NULL);

    if (strcmp(tipo, "EVENT_CONTEO_VEHICULAR") == 0) {
        json_extraer_int(json, "vehiculos_contados", &inter->vehiculos_contados);

    } else if (strcmp(tipo, "EVENT_LONGITUD_COLA") == 0) {
        json_extraer_int(json, "volumen", &inter->volumen_cola);
        json_extraer_double(json, "velocidad_promedio", &inter->velocidad_promedio);

    } else if (strcmp(tipo, "EVENT_DENSIDAD_DE_TRAFICO") == 0) {
        json_extraer_string(json, "nivel_congestion", inter->nivel_congestion, sizeof(inter->nivel_congestion));
        json_extraer_double(json, "velocidad_promedio", &inter->velocidad_promedio);
    }
}

/* ── Generar comando JSON de cambio de semáforo ──── */

static int columna_de_id(const char *id) {
    /* Extrae el número de columna del ID INT_<Letra><Número> */
    const char *p = id + strlen("INT_");
    /* saltar letra(s) de fila */
    while (*p && !(*p >= '0' && *p <= '9')) p++;
    if (!*p) return -1;
    return atoi(p);
}

/* 0 = HORIZONTAL (columnas impares), 1 = VERTICAL (columnas pares) */
static int orientacion_de_id(const char *id) {
    int col = columna_de_id(id);
    if (col <= 0) return 0;
    return (col % 2 == 0) ? 1 : 0;
}

static void generar_comando_json(char *buffer, size_t size,
                                 const char *interseccion,
                                 const char *nuevo_estado,
                                 const char *origen,
                                 const char *mensaje,
                                 int duracion_seg) {
    long timestamp = (long)time(NULL);
    /* Incluir campo "mensaje" si se proporciona (no vacío) */
    if (duracion_seg > 0) {
        if (mensaje && mensaje[0] != '\0') {
            snprintf(buffer, size,
                "{"
                "\"comando_id\":\"CMD_%s_%ld\","
                "\"interseccion\":\"%s\","
                "\"nuevo_estado\":\"%s\","
                "\"origen\":\"%s\","
                "\"mensaje\":\"%s\","
                "\"duracion\":%d,"
                "\"timestamp\":%ld"
                "}",
                interseccion, timestamp,
                interseccion, nuevo_estado, origen, mensaje, duracion_seg, timestamp);
        } else {
            snprintf(buffer, size,
                "{"
                "\"comando_id\":\"CMD_%s_%ld\","
                "\"interseccion\":\"%s\","
                "\"nuevo_estado\":\"%s\","
                "\"origen\":\"%s\","
                "\"duracion\":%d,"
                "\"timestamp\":%ld"
                "}",
                interseccion, timestamp,
                interseccion, nuevo_estado, origen, duracion_seg, timestamp);
        }
    } else {
        if (mensaje && mensaje[0] != '\0') {
            snprintf(buffer, size,
                "{"
                "\"comando_id\":\"CMD_%s_%ld\","
                "\"interseccion\":\"%s\","
                "\"nuevo_estado\":\"%s\","
                "\"origen\":\"%s\","
                "\"mensaje\":\"%s\","
                "\"timestamp\":%ld"
                "}",
                interseccion, timestamp,
                interseccion, nuevo_estado, origen, mensaje, timestamp);
        } else {
            snprintf(buffer, size,
                "{"
                "\"comando_id\":\"CMD_%s_%ld\","
                "\"interseccion\":\"%s\","
                "\"nuevo_estado\":\"%s\","
                "\"origen\":\"%s\","
                "\"timestamp\":%ld"
                "}",
                interseccion, timestamp,
                interseccion, nuevo_estado, origen, timestamp);
        }
    }
}

static void persistir_json_bd(const char *json,
                              void *push_bd_prin,
                              void *push_bd_rep) {
    int rc_prin = zmq_send(push_bd_prin, json, strlen(json), 0);
    if (rc_prin < 0) {
        fprintf(stderr, "[ANALITICA] Error enviando a BD Principal: %s\n",
                zmq_strerror(zmq_errno()));
    }

    int rc_rep = zmq_send(push_bd_rep, json, strlen(json), 0);
    if (rc_rep < 0) {
        fprintf(stderr, "[ANALITICA] Error enviando a BD Replica: %s\n",
                zmq_strerror(zmq_errno()));
    }
}

/* ── Procesar un evento completo ───────────────── */

static void procesar_evento(const char *json,
                            void *push_ctrl,
                            void *push_bd_prin,
                            void *push_bd_rep) {
    char interseccion[16] = {0};
    json_extraer_string(json, "interseccion", interseccion, sizeof(interseccion));

    InterseccionState *inter = buscar_interseccion(interseccion);
    if (!inter) return;

    /* Guardar estado anterior */
    EstadoTrafico estado_previo = inter->estado_actual;

    /* Actualizar datos con el evento recibido */
    actualizar_con_evento(json);

    /* Evaluar reglas */
    EstadoTrafico estado_nuevo = evaluar_reglas(inter);

    /* Si hay cambio de estado → actualizar */
    if (estado_nuevo != estado_previo) {
        inter->estado_actual = estado_nuevo;

        printf("[ANALITICA] %s: %s → %s\n",
               interseccion,
               ESTADO_NOMBRES[estado_previo],
               ESTADO_NOMBRES[estado_nuevo]);

        /* Solo enviar comando si esta interseccion tiene semaforo */
        if (inter->tiene_semaforo) {
            if (estado_nuevo == ESTADO_CONGESTION) {
                /* Congestión: aumentar verde en objetivo a 30s y detener "el otro" por 15s */
                int dur_obj = 30;
                int dur_otros = 15;
                /* Construir lista de semáforos afectados (opuestos) para el mensaje */
                char lista_detener[512] = "";
                int primero = 1;
                int orient_obj = inter->orientacion;
                for (int i = 0; i < total_intersecciones; i++) {
                    if (!grid[i].tiene_semaforo) continue;
                    if (strcmp(grid[i].id, interseccion) == 0) continue;
                    int orient_i = grid[i].orientacion;
                    if (orient_i != orient_obj) {
                        if (!primero) strncat(lista_detener, ", ", sizeof(lista_detener) - strlen(lista_detener) - 1);
                        strncat(lista_detener, grid[i].id, sizeof(lista_detener) - strlen(lista_detener) - 1);
                        primero = 0;
                    }
                }
                char mensaje[512];
                if (lista_detener[0] != '\0')
                    snprintf(mensaje, sizeof(mensaje), "Congestión en %s, detener %s", interseccion, lista_detener);
                else
                    snprintf(mensaje, sizeof(mensaje), "Congestión en %s", interseccion);

                /* Comando objetivo: VERDE con mensaje */
                char cmd_obj[BUFFER_SIZE];
                generar_comando_json(cmd_obj, sizeof(cmd_obj), interseccion, "VERDE", "ANALITICA", mensaje, dur_obj);
                zmq_send(push_ctrl, cmd_obj, strlen(cmd_obj), 0);
                persistir_json_bd(cmd_obj, push_bd_prin, push_bd_rep);
                printf("[ANALITICA] Comando enviado → %s = VERDE (dur=%d)\n", interseccion, dur_obj);

                /* Enviar ROJO a los opuestos con el mismo mensaje */
                for (int i = 0; i < total_intersecciones; i++) {
                    if (!grid[i].tiene_semaforo) continue;
                    if (strcmp(grid[i].id, interseccion) == 0) continue;
                    int orient_i = grid[i].orientacion;
                    if (orient_i != orient_obj) {
                        char cmd_other[BUFFER_SIZE];
                        generar_comando_json(cmd_other, sizeof(cmd_other), grid[i].id, "ROJO", "ANALITICA", mensaje, dur_otros);
                        zmq_send(push_ctrl, cmd_other, strlen(cmd_other), 0);
                        persistir_json_bd(cmd_other, push_bd_prin, push_bd_rep);
                        printf("[ANALITICA] Comando enviado → %s = ROJO (dur=%d)\n", grid[i].id, dur_otros);
                    }
                }
            } else {
                /* Normal: volver a comportamiento por defecto (ROJO por defecto en cambios)
                   Enviar ROJO simple (con mensaje vacío) para restablecer */
                char cmd[BUFFER_SIZE];
                generar_comando_json(cmd, sizeof(cmd), interseccion, "ROJO", "ANALITICA", "", TIEMPO_ROJO_DEFAULT);
                zmq_send(push_ctrl, cmd, strlen(cmd), 0);
                persistir_json_bd(cmd, push_bd_prin, push_bd_rep);
                printf("[ANALITICA] Comando enviado → %s = ROJO (dur=%d)\n", interseccion, TIEMPO_ROJO_DEFAULT);
            }
        } else {
            printf("[ANALITICA] %s sin semaforo — no se envia comando\n", interseccion);
        }
    }

    /* Persistir evento en BD Principal y BD Réplica */
    persistir_json_bd(json, push_bd_prin, push_bd_rep);
}

/* ── Imprimir las reglas en uso ─────────────────── */

static void imprimir_reglas(void) {
    printf("\n[ANALITICA] ===== REGLAS DE EVALUACION DE TRAFICO =====\n");
    printf("[ANALITICA] Regla TRAFICO NORMAL:\n");
    printf("[ANALITICA]   cola < %d  AND  velocidad > %.0f km/h  AND  vehiculos < %d\n",
           UMBRAL_COLA, UMBRAL_VELOCIDAD, UMBRAL_VEHICULOS);
    printf("[ANALITICA] Regla CONGESTION (cualquiera de):\n");
    printf("[ANALITICA]   cola >= %d  OR  velocidad <= %.0f km/h  OR  vehiculos >= %d\n",
           UMBRAL_COLA, UMBRAL_VELOCIDAD, UMBRAL_VEHICULOS);
    printf("[ANALITICA] Regla OLA_VERDE / EMERGENCIA: solo por comando del operador\n");
    printf("[ANALITICA] Tiempo semaforo verde normal: %d seg | rojo normal: %d seg\n",
           TIEMPO_VERDE_DEFAULT, TIEMPO_ROJO_DEFAULT);
    printf("[ANALITICA] =====================================================\n\n");
}

/* ── Procesar petición REQ/REP del Monitor ─────── */

static void procesar_peticion_monitor(void *rep_socket,
                                      void *push_ctrl,
                                      void *push_bd_prin,
                                      void *push_bd_rep) {
    char buffer[BUFFER_SIZE];
    int size = zmq_recv(rep_socket, buffer, sizeof(buffer) - 1, 0);
    if (size < 0) return;
    buffer[size] = '\0';

    char tipo_peticion[64] = {0};
    json_extraer_string(buffer, "tipo", tipo_peticion, sizeof(tipo_peticion));

    if (strcmp(tipo_peticion, "CONSULTAR_ESTADO") == 0) {
        /* Consultar estado de una intersección */
        char id[16] = {0};
        json_extraer_string(buffer, "interseccion", id, sizeof(id));

        InterseccionState *inter = buscar_interseccion(id);
        if (inter) {
            char resp[BUFFER_SIZE];
            snprintf(resp, sizeof(resp),
                "{"
                "\"interseccion\":\"%s\","
                "\"estado\":\"%s\","
                "\"vehiculos\":%d,"
                "\"cola\":%d,"
                "\"velocidad\":%.1f,"
                "\"nivel_congestion\":\"%s\","
                "\"tiene_semaforo\":%d,"
                "\"ultimo_update\":%ld"
                "}",
                inter->id, ESTADO_NOMBRES[inter->estado_actual],
                inter->vehiculos_contados, inter->volumen_cola,
                inter->velocidad_promedio, inter->nivel_congestion,
                inter->tiene_semaforo,
                inter->ultimo_update);
            zmq_send(rep_socket, resp, strlen(resp), 0);
        } else {
            const char *err = "{\"error\":\"Interseccion no encontrada\"}";
            zmq_send(rep_socket, err, strlen(err), 0);
        }

    } else if (strcmp(tipo_peticion, "FORZAR_CAMBIO") == 0) {
        /* Forzar cambio de semáforo por indicación directa del operador */
        char interseccion[16] = {0};
        char nuevo_estado[8]  = {0};

        json_extraer_string(buffer, "interseccion", interseccion, sizeof(interseccion));
        json_extraer_string(buffer, "nuevo_estado",  nuevo_estado,  sizeof(nuevo_estado));

        printf("[ANALITICA] Comando FORZADO recibido del Monitor: %s -> %s\n",
               interseccion, nuevo_estado);

        /* Extraer duracion opcional; si no viene, usar valor por defecto según nuevo_estado */
        int duracion = 0;
        if (!json_extraer_int(buffer, "duracion", &duracion) || duracion <= 0) {
            if (strcmp(nuevo_estado, "VERDE") == 0) duracion = TIEMPO_VERDE_DEFAULT;
            else duracion = TIEMPO_ROJO_DEFAULT;
        }

        /* Actualizar estado interno si la interseccion existe */
        InterseccionState *inter = buscar_interseccion(interseccion);
        if (inter) {
            inter->estado_actual = (strcmp(nuevo_estado, "VERDE") == 0)
                                   ? ESTADO_OLA_VERDE : ESTADO_NORMAL;
        }

        /* Extraer mensaje opcional de la petición del Monitor */
        char mensaje_monitor[256] = "";
        json_extraer_string(buffer, "mensaje", mensaje_monitor, sizeof(mensaje_monitor));

        /* Reenviar comando al control de semáforos: usar la duracion y mensaje determinados */
        char cmd[BUFFER_SIZE];
        generar_comando_json(cmd, sizeof(cmd), interseccion, nuevo_estado, "OPERADOR", mensaje_monitor, duracion);
        zmq_send(push_ctrl, cmd, strlen(cmd), 0);
        persistir_json_bd(cmd, push_bd_prin, push_bd_rep);
        printf("[ANALITICA] Comando forzado reenviado a Control: %s\n", cmd);

        /* Aplicar cambios a otras intersecciones: si forzaron VERDE, poner ROJO a la orientacion opuesta;
           si forzaron ROJO, poner VERDE a la orientacion opuesta. Usar la misma duracion. */
        if (strcmp(nuevo_estado, "VERDE") == 0 || strcmp(nuevo_estado, "ROJO") == 0) {
            int orient_obj;
            if (inter) orient_obj = inter->orientacion;
            else orient_obj = orientacion_de_id(interseccion);
             const char *estado_otros = (strcmp(nuevo_estado, "VERDE") == 0) ? "ROJO" : "VERDE";
             for (int i = 0; i < total_intersecciones; i++) {
                 if (!grid[i].tiene_semaforo) continue;
                 if (strcmp(grid[i].id, interseccion) == 0) continue;
                 int orient_i = grid[i].orientacion;
                 if (orient_i != orient_obj) {
                     char cmd_other[BUFFER_SIZE];
                     generar_comando_json(cmd_other, sizeof(cmd_other), grid[i].id, estado_otros, "ANALITICA", mensaje_monitor, duracion);
                     zmq_send(push_ctrl, cmd_other, strlen(cmd_other), 0);
                     persistir_json_bd(cmd_other, push_bd_prin, push_bd_rep);
                     printf("[ANALITICA] Comando enviado → %s = %s (dur=%d)\n", grid[i].id, estado_otros, duracion);
                 }
             }
         }

        const char *ok = "{\"status\":\"OK\",\"mensaje\":\"Comando forzado ejecutado\"}";
        zmq_send(rep_socket, ok, strlen(ok), 0);

    } else if (strcmp(tipo_peticion, "CONSULTAR_REGLAS") == 0) {
        imprimir_reglas();
        const char *ok = "{\"status\":\"OK\",\"mensaje\":\"Reglas impresas en consola\"}";
        zmq_send(rep_socket, ok, strlen(ok), 0);

    } else {
        const char *err = "{\"error\":\"Tipo de peticion desconocido\"}";
        zmq_send(rep_socket, err, strlen(err), 0);
    }
}

/* ── Main ──────────────────────────────────────── */

int main(int argc, char *argv[]) {
    const char *broker_host;
    const char *pc3_host;
    const char *lista_semaforos = DEFAULT_SEMAFOROS;

    if (argc >= 2) {
        broker_host = argv[1];
    } else {
        broker_host = PC1_HOST;
    }

    if (argc >= 3) {
        pc3_host = argv[2];
    } else {
        pc3_host = PC3_HOST;
    }

    if (argc >= 4) {
        lista_semaforos = argv[3];
    }

    inicializar_grid(lista_semaforos);
    imprimir_reglas();

    void *contexto = zmq_ctx_new();

    /* ── SUB socket: recibir eventos del Broker ── */
    void *sub_socket = zmq_socket(contexto, ZMQ_SUB);
    char addr_broker[128];
    snprintf(addr_broker, sizeof(addr_broker), "tcp://%s:%d", broker_host, PORT_BROKER_XPUB);
    if (zmq_connect(sub_socket, addr_broker) != 0) {
        fprintf(stderr, "[ANALITICA] Error conectando SUB a %s: %s\n", addr_broker, zmq_strerror(zmq_errno()));
        return 1;
    }
    /* Suscribirse a TODOS los tópicos de eventos */
    zmq_setsockopt(sub_socket, ZMQ_SUBSCRIBE, "EVENT_", 6);
    printf("[ANALITICA] SUB conectado a Broker en %s\n", addr_broker);

    /* ── PUSH socket: enviar comandos al Control ── */
    void *push_ctrl = zmq_socket(contexto, ZMQ_PUSH);
    char addr_ctrl[128];
    /* Control corre en la misma PC2 → localhost evita problemas si la IP cambia */
    snprintf(addr_ctrl, sizeof(addr_ctrl), "tcp://localhost:%d", PORT_CTRL_PULL);
    zmq_connect(push_ctrl, addr_ctrl);
    printf("[ANALITICA] PUSH conectado a Control en %s\n", addr_ctrl);

    /* ── PUSH socket: persistir en BD Principal ── */
    void *push_bd_prin = zmq_socket(contexto, ZMQ_PUSH);
    char addr_bd_prin[128];
    snprintf(addr_bd_prin, sizeof(addr_bd_prin), "tcp://%s:%d", pc3_host, PORT_BD_PRINCIPAL_PULL);
    if (zmq_connect(push_bd_prin, addr_bd_prin) != 0) {
        fprintf(stderr, "[ANALITICA] Error conectando PUSH a BD Principal %s: %s\n",
                addr_bd_prin, zmq_strerror(zmq_errno()));
    } else {
        printf("[ANALITICA] PUSH conectado a BD Principal en %s\n", addr_bd_prin);
    }

    /* ── PUSH socket: replicar en BD Réplica ──── */
    void *push_bd_rep = zmq_socket(contexto, ZMQ_PUSH);
    char addr_bd_rep[128];
    snprintf(addr_bd_rep, sizeof(addr_bd_rep), "tcp://localhost:%d", PORT_BD_REPLICA_PULL);
    zmq_connect(push_bd_rep, addr_bd_rep);
    printf("[ANALITICA] PUSH conectado a BD Replica en %s\n", addr_bd_rep);

    /* ── REP socket: responder al Monitor ──────── */
    void *rep_socket = zmq_socket(contexto, ZMQ_REP);
    char addr_rep[128];
    snprintf(addr_rep, sizeof(addr_rep), "tcp://*:%d", PORT_ANALITICA_REP);
    if (zmq_bind(rep_socket, addr_rep) != 0) {
        fprintf(stderr, "[ANALITICA] Error bind REP en %s: %s\n", addr_rep, zmq_strerror(zmq_errno()));
        return 1;
    }
    printf("[ANALITICA] REP escuchando en %s\n", addr_rep);

    printf("[ANALITICA] Servicio iniciado — esperando eventos...\n\n");

    /* ── Loop principal con zmq_poll ────────────── */
    zmq_pollitem_t items[] = {
        { sub_socket,  0, ZMQ_POLLIN, 0 },
        { rep_socket,  0, ZMQ_POLLIN, 0 }
    };

    char topico[256];
    char json[BUFFER_SIZE];

    while (1) {
        /* Esperar eventos en cualquiera de los dos sockets (timeout 1000ms) */
        int rc = zmq_poll(items, 2, 1000);
        if (rc < 0) break;

        /* Evento de sensor recibido vía SUB */
        if (items[0].revents & ZMQ_POLLIN) {
            /* Recibir mensaje multipart: [TOPICO][JSON] */
            int t_size = zmq_recv(sub_socket, topico, sizeof(topico) - 1, 0);
            if (t_size >= 0) {
                topico[t_size] = '\0';

                int j_size = zmq_recv(sub_socket, json, sizeof(json) - 1, 0);
                if (j_size >= 0) {
                    json[j_size] = '\0';
                    printf("[ANALITICA] Evento recibido [%s]: %s\n", topico, json);
                    procesar_evento(json, push_ctrl, push_bd_prin, push_bd_rep);
                }
            }
        }

        /* Petición del Monitor vía REP */
        if (items[1].revents & ZMQ_POLLIN) {
            procesar_peticion_monitor(rep_socket, push_ctrl, push_bd_prin, push_bd_rep);
        }
    }

    /* Cleanup */
    zmq_close(sub_socket);
    zmq_close(push_ctrl);
    zmq_close(push_bd_prin);
    zmq_close(push_bd_rep);
    zmq_close(rep_socket);
    zmq_ctx_destroy(contexto);

    return 0;
}