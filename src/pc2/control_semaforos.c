#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <zmq.h>
#include "../config.h"

/**
 * Servicio de Control de Semáforos (PC2).
 *
 * Sockets ZMQ:
 *   PULL ← bind en :5557  — recibe comandos de Analítica
 *   PUSH → conecta a Semáforos (PC1:5562) — reenvía comandos
 *
 * Función:
 *   Actúa como relay asíncrono entre Analítica y los Semáforos.
 *   Recibe comandos de cambio de estado, los reenvía al proceso
 *   semaforo.c en PC1 sin bloquear a Analítica.
 *   Puede ejecutar lógica adicional como ola verde (cambio
 *   coordinado de múltiples semáforos en secuencia).
 *
 * Uso: control_semaforos [pc1_host]
 */

int main(int argc, char *argv[]) {
    const char *pc1_host;

    if (argc >= 2) {
        pc1_host = argv[1];
    } else {
        pc1_host = PC1_HOST;
    }

    void *contexto = zmq_ctx_new();

    /* ── PULL socket: recibir comandos de Analítica ── */
    void *pull_socket = zmq_socket(contexto, ZMQ_PULL);
    char addr_pull[128];
    snprintf(addr_pull, sizeof(addr_pull), "tcp://*:%d", PORT_CTRL_PULL);
    if (zmq_bind(pull_socket, addr_pull) != 0) {
        fprintf(stderr, "[CONTROL] Error bind PULL en %s: %s\n", addr_pull, zmq_strerror(zmq_errno()));
        return 1;
    }
    printf("[CONTROL] PULL escuchando en %s\n", addr_pull);

    /* ── PUSH socket: reenviar a Semáforos en PC1 ── */
    void *push_socket = zmq_socket(contexto, ZMQ_PUSH);
    char addr_push[128];
    snprintf(addr_push, sizeof(addr_push), "tcp://%s:%d", pc1_host, PORT_SEMAFORO_PULL);
    if (zmq_connect(push_socket, addr_push) != 0) {
        fprintf(stderr, "[CONTROL] Error conectando PUSH a %s: %s\n", addr_push, zmq_strerror(zmq_errno()));
        return 1;
    }
    printf("[CONTROL] PUSH conectado a Semaforos en %s\n", addr_push);

    printf("[CONTROL] Servicio iniciado — esperando comandos...\n\n");

    char buffer[BUFFER_SIZE];
    int comandos_reenviados = 0;

    while (1) {
        int size = zmq_recv(pull_socket, buffer, sizeof(buffer) - 1, 0);
        if (size < 0) {
            fprintf(stderr, "[CONTROL] Error recv: %s\n", zmq_strerror(zmq_errno()));
            continue;
        }
        buffer[size] = '\0';

        printf("[CONTROL] Comando recibido: %s\n", buffer);

        /* Reenviar al proceso semaforo en PC1 */
        int rc = zmq_send(push_socket, buffer, size, 0);
        if (rc < 0) {
            fprintf(stderr, "[CONTROL] Error reenviando a Semaforos: %s\n",
                    zmq_strerror(zmq_errno()));
        } else {
            comandos_reenviados++;
            printf("[CONTROL] Comando #%d reenviado a Semaforos\n", comandos_reenviados);
        }
    }

    /* Cleanup (inalcanzable en operacion normal) */
    zmq_close(pull_socket);
    zmq_close(push_socket);
    zmq_ctx_destroy(contexto);
    return 0;
}
