#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <zmq.h>
#include "../config.h"

/**
 * Semáforos Simulados (PC1).
 * Mantiene el estado (ROJO/VERDE) de cada intersección del grid.
 * Escucha comandos de cambio vía ZMQ PULL desde ControlSemaforos (PC2).
 *
 * PULL BIND en tcp:// *:5562 <- ControlSemaforos conecta aquí (PUSH→PULL)
 *
 * Formato del comando esperado (JSON):
 * {
 *   "comando_id": "CMD001",
 *   "interseccion": "INT_C5",
 *   "nuevo_estado": "VERDE",
 *   "origen": "ANALITICA"
 * }
 *
 * Uso: semaforo [puerto_pull]
 */

/* ── Estado de un semáforo ──────────────────────── */
typedef struct {
    char id[16];      /* ej: "INT_C5"  */
    char estado[8];   /* "ROJO" o "VERDE" */
    int orientacion;  /* 0 = HORIZONTAL, 1 = VERTICAL (indica qué dirección tiene prioridad cuando está VERDE) */
    int tiempo_verde; /* segundos actuales para verde */
    int tiempo_rojo;  /* segundos actuales para rojo */
    int tiempo_restante; /* segundos restantes en la fase actual */
} SemaforoState;

/* ── Grid completo de semáforos ─────────────────── */
static SemaforoState grid[MAX_SEMAFOROS];
static int total_semaforos = 0;

/**
 * Inicializa semáforos a partir de un string CSV de intersecciones.
 * Ejemplo: "INT_C3,INT_C4,INT_F3"
 */
static void inicializar_semaforos(const char *lista_csv) {
    char copia[512];
    strncpy(copia, lista_csv, sizeof(copia) - 1);
    copia[sizeof(copia) - 1] = '\0';

    total_semaforos = 0;
    char *token = strtok(copia, ",");
    while (token != NULL && total_semaforos < MAX_SEMAFOROS) {
        /* Quitar espacios en blanco al inicio */
        while (*token == ' ') token++;
        /* Permitir especificar orientación en el CSV con sintaxis: ID[:H|:V]
           Ejemplos válidos: "INT_C3", "INT_C4:V", "INT_B3:H".
           Si no se especifica, se aplica la regla por paridad de columna como antes. */
        char copia_token[64];
        strncpy(copia_token, token, sizeof(copia_token) - 1);
        copia_token[sizeof(copia_token) - 1] = '\0';

        /* Separar ID y sufijo de orientación si existe */
        char *suf = strchr(copia_token, ':');
        char id_only[16];
        int orient = -1; /* -1 = auto, 0 = H, 1 = V */
        if (suf) {
            *suf = '\0';
            suf++;
            if (suf[0] == 'H' || suf[0] == 'h') orient = 0;
            else if (suf[0] == 'V' || suf[0] == 'v') orient = 1;
        }
        /* copiar id limpio */
        strncpy(id_only, copia_token, sizeof(id_only) - 1);
        id_only[sizeof(id_only) - 1] = '\0';

        /* Copiar ID al grid */
        strncpy(grid[total_semaforos].id, id_only, sizeof(grid[0].id) - 1);
        grid[total_semaforos].id[sizeof(grid[0].id) - 1] = '\0';

        /* Si orient == -1 -> inferir por paridad de columna; si se especificó, usarla */
        const char *p = grid[total_semaforos].id + strlen("INT_");
        while (*p && !(*p >= '0' && *p <= '9')) p++;
        int col = (*p) ? atoi(p) : -1;
        if (orient == -1) {
            if (col > 0) orient = (col % 2 == 0) ? 1 : 0;
            else orient = 0; /* fallback HORIZONTAL */
        }

        if (orient == 0) {
            grid[total_semaforos].orientacion = 0;
            strncpy(grid[total_semaforos].estado, "VERDE", sizeof(grid[0].estado));
            grid[total_semaforos].tiempo_verde = TIEMPO_VERDE_DEFAULT;
            grid[total_semaforos].tiempo_rojo = TIEMPO_ROJO_DEFAULT;
            grid[total_semaforos].tiempo_restante = grid[total_semaforos].tiempo_verde;
        } else {
            grid[total_semaforos].orientacion = 1;
            strncpy(grid[total_semaforos].estado, "ROJO", sizeof(grid[0].estado));
            grid[total_semaforos].tiempo_verde = TIEMPO_VERDE_DEFAULT;
            grid[total_semaforos].tiempo_rojo = TIEMPO_ROJO_DEFAULT;
            grid[total_semaforos].tiempo_restante = grid[total_semaforos].tiempo_rojo;
        }
        total_semaforos++;
        token = strtok(NULL, ",");
    }
    printf("[SEMAFORO] %d semaforos inicializados:\n", total_semaforos);
    for (int i = 0; i < total_semaforos; i++) {
        const char *orient_s = (grid[i].orientacion == 0) ? "H" : "V";
        printf("  -> %s (orientacion=%s)\n", grid[i].id, orient_s);
    }
}

static SemaforoState *buscar_semaforo(const char *interseccion) {
    for (int i = 0; i < total_semaforos; i++) {
        if (strcmp(grid[i].id, interseccion) == 0) {
            return &grid[i];
        }
    }
    return NULL;
}

/* Buffer pequeño para evitar procesar comandos duplicados (por reenvíos) */
#define SEEN_IDS_MAX 64
static char seen_ids[SEEN_IDS_MAX][64];
static int seen_idx = 0;
static int seen_count = 0;

static int visto_recientemente(const char *id) {
    if (!id || id[0] == '\0') return 0;
    for (int i = 0; i < seen_count; i++) {
        if (strcmp(seen_ids[i], id) == 0) return 1;
    }
    return 0;
}

static void registrar_id(const char *id) {
    if (!id || id[0] == '\0') return;
    /* almacenar en buffer circular */
    strncpy(seen_ids[seen_idx], id, sizeof(seen_ids[0]) - 1);
    seen_ids[seen_idx][sizeof(seen_ids[0]) - 1] = '\0';
    seen_idx = (seen_idx + 1) % SEEN_IDS_MAX;
    if (seen_count < SEEN_IDS_MAX) seen_count++;
}

static void cambiar_estado(const char *interseccion, const char *nuevo_estado) {
    SemaforoState *sem = buscar_semaforo(interseccion);
    if (!sem) {
        fprintf(stderr, "[SEMAFORO] Interseccion desconocida: %s\n", interseccion);
        return;
    }
    printf("[SEMAFORO] %s: %s -> %s\n", interseccion, sem->estado, nuevo_estado);
    strncpy(sem->estado, nuevo_estado, sizeof(sem->estado) - 1);
    sem->estado[sizeof(sem->estado) - 1] = '\0';
}

static void imprimir_estados(void) {
    printf("\n====== ESTADO ACTUAL DE SEMAFOROS ======\n");
    for (int i = 0; i < total_semaforos; i++) {
        const char *icono = (strcmp(grid[i].estado, "VERDE") == 0) ? "[V]" : "[R]";
        printf("  %s : %s %s\n", grid[i].id, icono, grid[i].estado);
    }
    printf("========================================\n\n");
}

/**
 * Extrae un valor string de un JSON simple dado una clave.
 * Busca "clave":"valor" y copia valor al buffer destino.
 * Retorna 1 si encontró, 0 si no.
 */
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

static void inicializar_grid(void) {
    total_semaforos = 0;
    int flujo_filas[] = FLUJO_FILAS;
    int flujo_columnas[] = FLUJO_COLUMNAS;

    for (int f = 0; f < GRID_FILAS; f++) {
        if (!flujo_filas[f]) continue; // Saltar filas sin flujo

        char fila = 'A' + f;
        for (int c = 0; c < GRID_COLUMNAS; c++) {
            if (!flujo_columnas[c]) continue; // Saltar columnas sin flujo

            snprintf(grid[total_semaforos].id, sizeof(grid[0].id), "INT_%c%d", fila, c + 1);
            /* Inicialización de orientación y fase inicial.
               Regla simple: columnas impares -> prioridad HORIZONTAL (VERDE inicial),
               columnas pares -> prioridad VERTICAL (ROJO inicial). Esta regla es
               determinista y puede ajustarse según política del proyecto. */
            if (((c + 1) % 2) == 1) {
                grid[total_semaforos].orientacion = 0; /* HORIZONTAL */
                strncpy(grid[total_semaforos].estado, "VERDE", sizeof(grid[0].estado));
                grid[total_semaforos].tiempo_verde = TIEMPO_VERDE_DEFAULT;
                grid[total_semaforos].tiempo_rojo = TIEMPO_ROJO_DEFAULT;
                grid[total_semaforos].tiempo_restante = grid[total_semaforos].tiempo_verde;
            } else {
                grid[total_semaforos].orientacion = 1; /* VERTICAL */
                strncpy(grid[total_semaforos].estado, "ROJO", sizeof(grid[0].estado));
                grid[total_semaforos].tiempo_verde = TIEMPO_VERDE_DEFAULT;
                grid[total_semaforos].tiempo_rojo = TIEMPO_ROJO_DEFAULT;
                grid[total_semaforos].tiempo_restante = grid[total_semaforos].tiempo_rojo;
            }
            total_semaforos++;
        }
    }
    printf("[SEMAFORO] Grid inicializado con %d semáforos activos\n", total_semaforos);
}

static void aplicar_cambio_por_tiempo(SemaforoState *sem) {
    /* Alterna la fase cuando el tiempo restante llega a 0 */
    if (strcmp(sem->estado, "VERDE") == 0) {
        /* pasar a ROJO */
        strncpy(sem->estado, "ROJO", sizeof(sem->estado));
        sem->tiempo_restante = sem->tiempo_rojo;
        const char *orient = (sem->orientacion == 0) ? "HORIZONTAL" : "VERTICAL";
        printf("[SEMAFORO] %s: VERDE->ROJO (orientacion=%s)\n", sem->id, orient);
    } else {
        /* pasar a VERDE */
        strncpy(sem->estado, "VERDE", sizeof(sem->estado));
        sem->tiempo_restante = sem->tiempo_verde;
        const char *orient = (sem->orientacion == 0) ? "HORIZONTAL" : "VERTICAL";
        printf("[SEMAFORO] %s: ROJO->VERDE (orientacion=%s)\n", sem->id, orient);
    }
}

static void forzar_estado(SemaforoState *sem, const char *nuevo_estado, int duracion_seg) {
    if (strcmp(nuevo_estado, "VERDE") == 0) {
        strncpy(sem->estado, "VERDE", sizeof(sem->estado));
        sem->tiempo_restante = (duracion_seg > 0) ? duracion_seg : sem->tiempo_verde;
    } else if (strcmp(nuevo_estado, "ROJO") == 0) {
        strncpy(sem->estado, "ROJO", sizeof(sem->estado));
        sem->tiempo_restante = (duracion_seg > 0) ? duracion_seg : sem->tiempo_rojo;
    }
    printf("[SEMAFORO] %s: Forzado %s por %d seg\n", sem->id, sem->estado, sem->tiempo_restante);
}

int main(int argc, char *argv[]) {
    int puerto = PORT_SEMAFORO_PULL;
    const char *lista_semaforos = DEFAULT_SEMAFOROS;

    if (argc >= 2) {
        puerto = atoi(argv[1]);
    }
    if (argc >= 3) {
        lista_semaforos = argv[2];
    }

    /* Inicializar semáforos: prioridad a la lista CSV.
       1) Si se pasa lista por argumento (argv[2]) -> usarla.
       2) Si no se pasa, usar DEFAULT_SEMAFOROS si DEFAULT_NUM_SEMAFOROS>0.
       3) Si DEFAULT_NUM_SEMAFOROS == 0 -> inicializar todo el grid según FLUJO_FILAS/COLUMNAS.
       Esto evita que el programa cree semáforos derivados del grid por defecto
       cuando la intención es usar la lista de semáforos configurada en config.h. */
    if (argc >= 3) {
        inicializar_semaforos(lista_semaforos);
    } else if (DEFAULT_NUM_SEMAFOROS > 0) {
        inicializar_semaforos(DEFAULT_SEMAFOROS);
    } else {
        inicializar_grid();
    }
    imprimir_estados();

    void *contexto = zmq_ctx_new();
    void *pull = zmq_socket(contexto, ZMQ_PULL);

    char addr[64];
    snprintf(addr, sizeof(addr), "tcp://*:%d", puerto);

    if (zmq_bind(pull, addr) != 0) {
        fprintf(stderr, "[SEMAFORO] Error bind PULL en %s: %s\n", addr, zmq_strerror(zmq_errno()));
        return 1;
    }

    printf("[SEMAFORO] Escuchando comandos en PULL tcp://*:%d\n", puerto);

    char buffer[BUFFER_SIZE];
    zmq_pollitem_t items[] = { { pull, 0, ZMQ_POLLIN, 0 } };

    while (1) {
        /* Poll con timeout de 1 seg para actualizar temporizadores */
        int rc = zmq_poll(items, 1, 1000);
        if (rc < 0) break;

        /* Procesar comando si existe */
        if (items[0].revents & ZMQ_POLLIN) {
            int size = zmq_recv(pull, buffer, sizeof(buffer) - 1, 0);
            if (size >= 0) {
                buffer[size] = '\0';

                char interseccion[16] = {0};
                char nuevo_estado[8]  = {0};
                char origen[32]       = {0};
                char cmd_id[64]       = {0};
                int duracion = 0;
                char mensaje[128]     = {0};

                json_extraer_string(buffer, "interseccion", interseccion, sizeof(interseccion));
                json_extraer_string(buffer, "nuevo_estado", nuevo_estado, sizeof(nuevo_estado));
                json_extraer_string(buffer, "origen", origen, sizeof(origen));
                json_extraer_string(buffer, "comando_id", cmd_id, sizeof(cmd_id));
                /* Mensaje opcional: motivo/regla que originó el comando */
                json_extraer_string(buffer, "mensaje", mensaje, sizeof(mensaje));
                json_extraer_int(buffer, "duracion", &duracion);

                /* Ignorar reenvíos duplicados si comando_id ya fue visto */
                if (strlen(cmd_id) > 0) {
                    if (visto_recientemente(cmd_id)) {
                        printf("[SEMAFORO] Ignorado comando duplicado %s\n", cmd_id);
                        continue;
                    } else {
                        registrar_id(cmd_id);
                    }
                }

                if (strlen(interseccion) > 0 && strlen(nuevo_estado) > 0) {
                    SemaforoState *sem = buscar_semaforo(interseccion);
                    if (sem) {
                        /* Si viene un mensaje/motivo, mostrarlo explícitamente */
                        if (strlen(mensaje) > 0) {
                            printf("[SEMAFORO] Regla recibida: \"%s\"\n", mensaje);
                        }
                        /* Capturar estado previo real antes de cambiar */
                        char previo[16];
                        strncpy(previo, sem->estado, sizeof(previo) - 1);
                        previo[sizeof(previo) - 1] = '\0';
                        forzar_estado(sem, nuevo_estado, duracion);
                        /* Mostrar cambio con motivo si se proporcionó */
                        if (strlen(mensaje) > 0) {
                            printf("[SEMAFORO] %s: %s -> %s (por %s)\n",
                                   interseccion,
                                   previo,
                                   nuevo_estado,
                                   mensaje);
                        }
                        printf("[SEMAFORO] Comando %s ejecutado (origen: %s)\n", cmd_id, origen);
                    } else {
                        fprintf(stderr, "[SEMAFORO] Interseccion desconocida en comando: %s\n", interseccion);
                    }
                } else {
                    fprintf(stderr, "[SEMAFORO] Comando malformado: %s\n", buffer);
                }
            }
        }

        /* Actualizar temporizadores cada segundo: decrementar tiempo_restante y alternar si llega a 0 */
        for (int i = 0; i < total_semaforos; i++) {
            if (grid[i].tiempo_restante > 0) {
                grid[i].tiempo_restante -= 1;
                if (grid[i].tiempo_restante <= 0) {
                    aplicar_cambio_por_tiempo(&grid[i]);
                }
            }
        }
    }

    zmq_close(pull);
    zmq_ctx_destroy(contexto);
    return 0;
}
