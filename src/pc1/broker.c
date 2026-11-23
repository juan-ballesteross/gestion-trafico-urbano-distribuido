#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <zmq.h>
#include "../config.h"

/**
 * broker.c — Broker de mensajes ZeroMQ (PC1)
 *
 * Rol en el sistema:
 *   Punto central que recibe eventos de todos los sensores (espira, cámara, GPS)
 *   y los reenvía sin modificar el contenido hacia la analítica en PC2.
 *   No interpreta JSON ni filtra por intersección: solo hace de "puente".
 *
 * Patrón ZMQ — proxy XSUB / XPUB:
 *   Los sensores usan PUB y se conectan al XSUB del broker.
 *   La analítica usa SUB y se conecta al XPUB del broker.
 *   zmq_proxy() copia en bloque todo lo que entra por XSUB hacia XPUB.
 *
 *   Sensor (PUB) ──connect──► XSUB :5555 ──proxy──► XPUB :5556 ──connect──► Analítica (SUB)
 *
 * Puertos (por defecto en config.h):
 *   PORT_BROKER_XSUB (5555) — entrada de publicaciones
 *   PORT_BROKER_XPUB (5556) — salida hacia suscriptores
 *
 * Por qué XSUB/XPUB y no PUB/SUB directo:
 *   Con PUB/SUB clásico el suscriptor debe conectarse antes que el publicador
 *   o se pierden mensajes. El proxy XSUB/XPUB permite que sensores y analítica
 *   arranquen en cualquier orden; el broker siempre está escuchando (bind).
 *
 * Uso:
 *   broker [puerto_xsub] [puerto_xpub]
 *   Sin argumentos usa 5555 y 5556. Ver también docs/referencia_zmq.md
 */

int main(int argc, char *argv[]) {
    int puerto_sub = PORT_BROKER_XSUB;
    int puerto_pub = PORT_BROKER_XPUB;

    /* ── 1. Parámetros de ejecución ───────────────────────────── */
    if (argc >= 3) {
        puerto_sub = atoi(argv[1]);
        puerto_pub = atoi(argv[2]);
    }

    /* ── 2. Contexto y socket de entrada (sensores) ───────────── */
    void *contexto = zmq_ctx_new();
    if (!contexto) {
        fprintf(stderr, "[BROKER] Error creando contexto ZMQ\n");
        return 1;
    }

    /*
     * ZMQ_XSUB: lado "suscriptor extendido" del proxy.
     * Los sensores hacen zmq_connect(PUB) → este socket hace zmq_bind(XSUB).
     */
    void *xsub = zmq_socket(contexto, ZMQ_XSUB);
    char addr_sub[64];
    snprintf(addr_sub, sizeof(addr_sub), "tcp://*:%d", puerto_sub);
    if (zmq_bind(xsub, addr_sub) != 0) {
        fprintf(stderr, "[BROKER] Error bind XSUB en %s: %s\n",
                addr_sub, zmq_strerror(zmq_errno()));
        return 1;
    }

    /* ── 3. Socket de salida (analítica y otros suscriptores) ─── */
    /*
     * ZMQ_XPUB: lado "publicador extendido" del proxy.
     * La analítica hace zmq_connect(SUB) → este socket hace zmq_bind(XPUB).
     */
    void *xpub = zmq_socket(contexto, ZMQ_XPUB);
    char addr_pub[64];
    snprintf(addr_pub, sizeof(addr_pub), "tcp://*:%d", puerto_pub);
    if (zmq_bind(xpub, addr_pub) != 0) {
        fprintf(stderr, "[BROKER] Error bind XPUB en %s: %s\n",
                addr_pub, zmq_strerror(zmq_errno()));
        return 1;
    }

    printf("[BROKER] Iniciado — XSUB:%d | XPUB:%d\n", puerto_sub, puerto_pub);
    printf("[BROKER] Listo y en ejecución — escuchando sensores y suscriptores\n");

    /* ── 4. Proxy (bucle bloqueante) ────────────────────────────── */
    /*
     * zmq_proxy reenvía mensajes de xsub → xpub hasta que el proceso termine.
     * El tercer argumento (NULL) es un socket de control opcional; no se usa aquí.
     * Los mensajes multipart (tópico + JSON de los sensores) se preservan tal cual.
     */
    zmq_proxy(xsub, xpub, NULL);

    /* Solo se ejecuta si zmq_proxy falla o el proceso recibe señal de cierre */
    zmq_close(xsub);
    zmq_close(xpub);
    zmq_ctx_destroy(contexto);

    return 0;
}
