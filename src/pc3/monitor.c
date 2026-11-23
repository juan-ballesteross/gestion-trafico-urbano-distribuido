#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <zmq.h>
#include "../config.h"

/**
 * Servicio de Monitoreo y Consulta (PC3).
 *
 * Sockets ZMQ:
 *   REQ  -> Analitica   (PC2:5560) — consultar estado / forzar cambios
 *   REQ  -> BD          (PC3:5563 o PC2:5564 en failover)
 *
 * Failover automatico:
 *   Si la BD Principal (PC3) no responde, el monitor recrea el
 *   socket REQ apuntando a la BD Replica en PC2 (PORT_BD_REPLICA_REP)
 *   y continua operando de forma transparente para el operador.
 *
 * Uso: monitor [pc2_host] [pc3_bd_host]
 */

/* ── Forward declarations ───────────────────────── */
static void mostrar_visualizacion(void);

/* ── Estado global de failover ──────────────────── */
static int bd_en_failover = 0;  /* 0=BD Principal, 1=BD Replica */

/* ── Extraer string de JSON ─────────────────────── */

/* ── Enviar REQ con timeout ──────────────────────── */
static int enviar_req_con_timeout(void *req_socket, const char *mensaje,
                                   char *respuesta, size_t resp_size,
                                   int timeout_ms) {
    zmq_send(req_socket, mensaje, strlen(mensaje), 0);

    zmq_pollitem_t items[] = { { req_socket, 0, ZMQ_POLLIN, 0 } };
    int rc = zmq_poll(items, 1, timeout_ms);

    if (rc > 0 && (items[0].revents & ZMQ_POLLIN)) {
        int size = zmq_recv(req_socket, respuesta, resp_size - 1, 0);
        if (size >= 0) {
            respuesta[size] = '\0';
            return 0;
        }
    }
    snprintf(respuesta, resp_size, "{\"error\":\"Timeout\"}");
    return -1;
}

/* ── Reconectar socket REQ ───────────────────────── */
static void reconectar_req(void *ctx, void **socket, const char *nueva_addr) {
    if (*socket) zmq_close(*socket);
    *socket = zmq_socket(ctx, ZMQ_REQ);
    zmq_connect(*socket, nueva_addr);
    printf("[MONITOR] Socket BD reconectado -> %s\n", nueva_addr);
}

/* ── Activar failover a BD Replica ──────────────── */
static void activar_failover(void *ctx, void **req_bd, const char *pc2_host) {
    if (bd_en_failover) return;
    printf("\n[MONITOR] *** FAILOVER ACTIVADO ***\n");
    printf("[MONITOR] BD Principal no responde. Usando BD Replica en PC2.\n\n");
    char addr[128];
    snprintf(addr, sizeof(addr), "tcp://%s:%d", pc2_host, PORT_BD_REPLICA_REP);
    reconectar_req(ctx, req_bd, addr);
    bd_en_failover = 1;
}

/* ── Menu interactivo ────────────────────────────── */
static void mostrar_menu(void) {
    printf("\n");
    printf("=============================================\n");
    printf("  SISTEMA DE MONITOREO DE TRAFICO URBANO\n");
    if (bd_en_failover)
        printf("  [!] MODO FAILOVER — usando BD Replica\n");
    printf("=============================================\n");
    printf("  1. Consultar estado de interseccion\n");
    printf("  2. Consultar historial de eventos\n");
    printf("  3. Forzar cambio de semaforo\n");
    printf("  4. Activar ola verde (fila completa)\n");
    printf("  5. Health check BD\n");
    printf("  6. Visualizar cuadricula ciudad\n");
    printf("  7. Consultar reglas de analitica\n");
    printf("  8. Salir\n");
    printf("=============================================\n");
    printf("Opcion: ");
}

/* ── Visualizacion ASCII de la cuadricula ────────── */
static void mostrar_visualizacion(void) {
    const int filas    = GRID_FILAS;
    const int columnas = GRID_COLUMNAS;
    int flujo_filas[]    = FLUJO_FILAS;
    int flujo_columnas[] = FLUJO_COLUMNAS;

    char celda[GRID_FILAS][GRID_COLUMNAS][8];
    for (int i = 0; i < filas; i++)
        for (int j = 0; j < columnas; j++)
            snprintf(celda[i][j], sizeof(celda[i][j]), "   ");

    char semaforos_str[] = DEFAULT_SEMAFOROS;
    char *token = strtok(semaforos_str, ",");
    while (token != NULL) {
        /* Eliminar sufijo de orientación si existe (ej: INT_C4:V) */
        char token_clean[64];
        strncpy(token_clean, token, sizeof(token_clean) - 1);
        token_clean[sizeof(token_clean) - 1] = '\0';
        char *suf = strchr(token_clean, ':');
        if (suf) *suf = '\0';
        /* Trim espacios */
        char *p = token_clean;
        while (*p == ' ') p++;
        /* Usar p para parsear fila/col */
        char fila_c; int col;
        if (sscanf(p, "INT_%c%d", &fila_c, &col) == 2) {
            int fi = fila_c - 'A';
            int ci = col - 1;
            if (fi >= 0 && fi < filas && ci >= 0 && ci < columnas)
                snprintf(celda[fi][ci], sizeof(celda[fi][ci]), "[S]");
        }
        token = strtok(NULL, ",");
    }

    printf("\n  Ciudad %dx%d  [S]=Semaforo  -->=Flujo fila  v=Flujo col\n\n",
           filas, columnas);
    printf("     ");
    for (int j = 0; j < columnas; j++) printf("  C%d  ", j + 1);
    printf("\n");

    for (int i = 0; i < filas; i++) {
        printf(" F%d  ", i + 1);
        for (int j = 0; j < columnas; j++) {
            printf("%s", celda[i][j]);
            if (j < columnas - 1)
                printf(flujo_filas[i] ? "-->" : "   ");
        }
        printf("\n");
        if (i < filas - 1) {
            printf("     ");
            for (int j = 0; j < columnas; j++)
                /* Mostrar flecha hacia abajo si hay flujo vertical en la columna */
                printf(flujo_columnas[j] ? "  v  " : "     ");
            printf("\n");
        }
    }
    printf("\nSemaforos: %s\n", DEFAULT_SEMAFOROS);
}

/* ══════════════ MAIN ══════════════════════════════ */
int main(int argc, char *argv[]) {
    const char *pc2_host    = (argc >= 2) ? argv[1] : PC2_HOST;
    const char *pc3_bd_host = (argc >= 3) ? argv[2] : "localhost";

    void *contexto = zmq_ctx_new();

    /* REQ -> Analitica */
    void *req_analitica = zmq_socket(contexto, ZMQ_REQ);
    char addr_analitica[128];
    snprintf(addr_analitica, sizeof(addr_analitica),
             "tcp://%s:%d", pc2_host, PORT_ANALITICA_REP);
    zmq_connect(req_analitica, addr_analitica);
    printf("[MONITOR] REQ -> Analitica: %s\n", addr_analitica);

    /* REQ -> BD Principal */
    void *req_bd = zmq_socket(contexto, ZMQ_REQ);
    char addr_bd[128];
    snprintf(addr_bd, sizeof(addr_bd),
             "tcp://%s:%d", pc3_bd_host, PORT_BD_PRINCIPAL_REP);
    zmq_connect(req_bd, addr_bd);
    printf("[MONITOR] REQ -> BD Principal: %s\n", addr_bd);

    printf("[MONITOR] Servicio de monitoreo iniciado.\n");
    printf("[MONITOR] Todas las operaciones se imprimen en tiempo real.\n\n");

    char respuesta[BUFFER_SIZE * 4];
    char input[256];

    while (1) {
        mostrar_menu();
        if (fgets(input, sizeof(input), stdin) == NULL) break;
        int opcion = atoi(input);

        switch (opcion) {

        /* 1. Consultar estado de interseccion */
        case 1: {
            printf("Interseccion (ej: INT_C3): ");
            if (!fgets(input, sizeof(input), stdin)) break;
            input[strcspn(input, "\n")] = '\0';

            char peticion[BUFFER_SIZE];
            snprintf(peticion, sizeof(peticion),
                "{\"tipo\":\"CONSULTAR_ESTADO\",\"interseccion\":\"%s\"}", input);

            printf("[MONITOR] Enviando CONSULTAR_ESTADO a Analitica...\n");
            int rc = enviar_req_con_timeout(req_analitica, peticion,
                                            respuesta, sizeof(respuesta),
                                            HEALTH_CHECK_TIMEOUT_MS);
            if (rc == 0) printf("[MONITOR] Estado: %s\n", respuesta);
            else         printf("[MONITOR] Analitica no responde (timeout)\n");
            break;
        }

        /* 2. Consultar historial */
        case 2: {
            long ahora = (long)time(NULL);
            char peticion[BUFFER_SIZE];
            snprintf(peticion, sizeof(peticion),
                "{\"tipo\":\"CONSULTAR_HISTORICO\",\"t1\":%ld,\"t2\":%ld}",
                ahora - 3600, ahora);

            printf("[MONITOR] Consultando historial ultima hora en BD%s...\n",
                   bd_en_failover ? " REPLICA" : " Principal");

            int rc = enviar_req_con_timeout(req_bd, peticion,
                                            respuesta, sizeof(respuesta),
                                            HEALTH_CHECK_TIMEOUT_MS);
            if (rc == 0) {
                printf("[MONITOR] Historico: %s\n", respuesta);
            } else {
                printf("[MONITOR] BD no responde — activando failover...\n");
                activar_failover(contexto, &req_bd, pc2_host);
                rc = enviar_req_con_timeout(req_bd, peticion,
                                            respuesta, sizeof(respuesta),
                                            HEALTH_CHECK_TIMEOUT_MS);
                if (rc == 0) printf("[MONITOR] Historico (REPLICA): %s\n", respuesta);
                else         printf("[MONITOR] BD Replica tampoco responde\n");
            }
            break;
        }

        /* 3. Forzar cambio de semaforo */
        case 3: {
            printf("Interseccion (ej: INT_C3): ");
            if (!fgets(input, sizeof(input), stdin)) break;
            input[strcspn(input, "\n")] = '\0';
            char interseccion[16];
            strncpy(interseccion, input, sizeof(interseccion) - 1);
            interseccion[sizeof(interseccion) - 1] = '\0';

            printf("Nuevo estado (VERDE/ROJO): ");
            if (!fgets(input, sizeof(input), stdin)) break;
            input[strcspn(input, "\n")] = '\0';

            char peticion[BUFFER_SIZE];
            snprintf(peticion, sizeof(peticion),
                "{\"tipo\":\"FORZAR_CAMBIO\","
                "\"interseccion\":\"%s\","
                "\"nuevo_estado\":\"%s\","
                "\"origen\":\"OPERADOR\"}",
                interseccion, input);

            printf("[MONITOR] Enviando FORZAR_CAMBIO: %s -> %s\n",
                   interseccion, input);
            int rc = enviar_req_con_timeout(req_analitica, peticion,
                                            respuesta, sizeof(respuesta),
                                            HEALTH_CHECK_TIMEOUT_MS);
            if (rc == 0) printf("[MONITOR] Confirmacion: %s\n", respuesta);
            else         printf("[MONITOR] Analitica no responde (timeout)\n");
            break;
        }

        /* 4. Ola verde (fila completa) */
        case 4: {
            printf("Letra de fila (A=F1, B=F2, C=F3, D=F4): ");
            if (!fgets(input, sizeof(input), stdin)) break;
            input[strcspn(input, "\n")] = '\0';
            char fila_c = (char)(input[0] >= 'a' ? input[0] - 32 : input[0]);

            printf("[MONITOR] Activando OLA VERDE en fila %c...\n", fila_c);
            int enviados = 0;

            for (int col = 1; col <= GRID_COLUMNAS; col++) {
                char interseccion[16];
                snprintf(interseccion, sizeof(interseccion), "INT_%c%d", fila_c, col);

                char peticion[BUFFER_SIZE];
                snprintf(peticion, sizeof(peticion),
                    "{\"tipo\":\"FORZAR_CAMBIO\","
                    "\"interseccion\":\"%s\","
                    "\"nuevo_estado\":\"VERDE\","
                    "\"origen\":\"OLA_VERDE\"}",
                    interseccion);

                /* Socket temporal para cada REQ independiente */
                void *req_tmp = zmq_socket(contexto, ZMQ_REQ);
                char addr_tmp[128];
                snprintf(addr_tmp, sizeof(addr_tmp),
                         "tcp://%s:%d", pc2_host, PORT_ANALITICA_REP);
                zmq_connect(req_tmp, addr_tmp);
                dormir_ms(150);

                int rc = enviar_req_con_timeout(req_tmp, peticion,
                                                respuesta, sizeof(respuesta),
                                                HEALTH_CHECK_TIMEOUT_MS);
                if (rc == 0) {
                    printf("[MONITOR]   -> %s: VERDE OK\n", interseccion);
                    enviados++;
                }
                zmq_close(req_tmp);
            }
            printf("[MONITOR] Ola verde completada: %d/%d semaforos\n",
                   enviados, GRID_COLUMNAS);
            break;
        }

        /* 5. Health check BD */
        case 5: {
            const char *peticion = "{\"tipo\":\"HEALTH_CHECK\"}";
            printf("[MONITOR] Health check a BD%s...\n",
                   bd_en_failover ? " REPLICA" : " Principal");
            int rc = enviar_req_con_timeout(req_bd, peticion,
                                            respuesta, sizeof(respuesta),
                                            HEALTH_CHECK_TIMEOUT_MS);
            if (rc == 0) {
                printf("[MONITOR] BD OK: %s\n", respuesta);
            } else {
                printf("[MONITOR] BD NO responde.\n");
                if (!bd_en_failover) {
                    activar_failover(contexto, &req_bd, pc2_host);
                    rc = enviar_req_con_timeout(req_bd, peticion,
                                                respuesta, sizeof(respuesta),
                                                HEALTH_CHECK_TIMEOUT_MS);
                    if (rc == 0) printf("[MONITOR] BD Replica OK: %s\n", respuesta);
                    else         printf("[MONITOR] BD Replica tampoco responde\n");
                }
            }
            break;
        }

        /* 6. Visualizar cuadricula */
        case 6:
            mostrar_visualizacion();
            break;

        /* 7. Consultar reglas de analitica */
        case 7: {
            const char *peticion = "{\"tipo\":\"CONSULTAR_REGLAS\"}";
            printf("[MONITOR] Solicitando reglas a Analitica...\n");
            int rc = enviar_req_con_timeout(req_analitica, peticion,
                                            respuesta, sizeof(respuesta),
                                            HEALTH_CHECK_TIMEOUT_MS);
            if (rc == 0) printf("[MONITOR] Reglas impresas en consola de Analitica. Resp: %s\n", respuesta);
            else         printf("[MONITOR] Analitica no responde\n");
            break;
        }

        /* 8. Salir */
        case 8:
            printf("[MONITOR] Cerrando servicio...\n");
            goto cleanup;

        default:
            printf("[MONITOR] Opcion no valida (1-8)\n");
        }
    }

cleanup:
    zmq_close(req_analitica);
    zmq_close(req_bd);
    zmq_ctx_destroy(contexto);
    return 0;
}
