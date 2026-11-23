#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <zmq.h>
#include "../config.h"

/**
 * sensor_gps.c — Simulador de sensor GPS / densidad de tráfico (PC1)
 *
 * Qué mide (simulado):
 *   Nivel de congestión (BAJA, NORMAL, ALTA) y velocidad promedio
 *   inferida del movimiento de vehículos en la intersección.
 *
 * Flujo en el sistema distribuido:
 *   [este proceso] --PUB--> Broker XSUB --XPUB--> Analítica / BD
 *
 * Comunicación ZMQ:
 *   - Socket: ZMQ_PUB conectado a tcp://<broker_host>:PORT_BROKER_XSUB
 *   - Cada envío es un mensaje de dos partes: tópico + JSON
 *   - Tópico: "EVENT_DENSIDAD_DE_TRAFICO"
 *   - Intervalo entre lecturas: SENSOR_GPS_INTERVALO_MS (config.h)
 *
 * Ejemplo de payload JSON:
 * {
 *   "tipo": "EVENT_DENSIDAD_DE_TRAFICO",
 *   "sensor_id": "GPS_INT_D2",
 *   "interseccion": "INT_D2",
 *   "nivel_congestion": "ALTA",
 *   "velocidad_promedio": 12.3,
 *   "timestamp": 1712170830
 * }
 *
 * Uso:
 *   sensor_gps [interseccion] [broker_host]
 *   interseccion  — ID de la intersección (default: INT_A1)
 *   broker_host   — IP o hostname del broker (default: localhost)
 */

/**
 * Arma el JSON de una lectura simulada de GPS.
 * El nivel de congestión y la velocidad se correlacionan: a mayor
 * congestión, menor velocidad (coherencia para la analítica).
 */
static void generar_evento_json(char *buffer, size_t size, const char *sensor_id, const char *interseccion) {
    /*
     * Simulación ponderada (dado 0–9):
     *   0–6  (70 %) → BAJA    → 40–60 km/h
     *   7–8  (20 %) → NORMAL  → 20–39 km/h
     *   9    (10 %) → ALTA    →  1–10 km/h
     */
    int dado = rand() % 10;
    const char *nivel;
    double velocidad;

    if (dado <= 6) {
        nivel     = "BAJA";
        velocidad = 40.0 + ((rand() % 200) / 10.0);
    } else if (dado <= 8) {
        nivel     = "NORMAL";
        velocidad = 20.0 + ((rand() % 190) / 10.0);
    } else {
        nivel     = "ALTA";
        velocidad = 1.0  + ((rand() % 90)  / 10.0);
    }

    long timestamp = (long)time(NULL);

    snprintf(buffer, size,
        "{"
        "\"tipo\":\"EVENT_DENSIDAD_DE_TRAFICO\","
        "\"sensor_id\":\"%s\","
        "\"interseccion\":\"%s\","
        "\"nivel_congestion\":\"%s\","
        "\"velocidad_promedio\":%.1f,"
        "\"timestamp\":%ld"
        "}",
        sensor_id, interseccion,
        nivel, velocidad,
        timestamp);
}

int main(int argc, char *argv[]) {
    const char *interseccion;
    const char *broker_host;

    /* ── 1. Parámetros de ejecución ───────────────────────────── */
    if (argc >= 2) {
        interseccion = argv[1];
    } else {
        interseccion = "INT_A1";
    }

    if (argc >= 3) {
        broker_host = argv[2];
    } else {
        broker_host = "localhost";
    }

    /* Identificador único del sensor, p. ej. GPS_INT_D2 */
    char sensor_id[64];
    snprintf(sensor_id, sizeof(sensor_id), "GPS_%s", interseccion);

    char broker_addr[128];
    snprintf(broker_addr, sizeof(broker_addr), "tcp://%s:%d", broker_host, PORT_BROKER_XSUB);

    srand((unsigned int)time(NULL) ^ (unsigned int)obtener_pid());

    /* ── 2. Conexión al broker ────────────────────────────────── */
    void *contexto = zmq_ctx_new();
    void *pub = zmq_socket(contexto, ZMQ_PUB);

    if (zmq_connect(pub, broker_addr) != 0) {
        fprintf(stderr, "[GPS:%s] Error conectando a %s: %s\n",
                sensor_id, broker_addr, zmq_strerror(zmq_errno()));
        return 1;
    }

    printf("[GPS:%s] Conectado a %s | Interseccion: %s\n",
           sensor_id, broker_addr, interseccion);

    dormir_ms(1000);

    /* ── 3. Bucle de publicación ──────────────────────────────── */
    char topico[] = "EVENT_DENSIDAD_DE_TRAFICO";
    char json[BUFFER_SIZE];

    while (1) {
        generar_evento_json(json, sizeof(json), sensor_id, interseccion);

        zmq_send(pub, topico, strlen(topico), ZMQ_SNDMORE);
        zmq_send(pub, json, strlen(json), 0);

        printf("[GPS:%s] %s -> %s\n", sensor_id, interseccion, json);

        dormir_ms(SENSOR_GPS_INTERVALO_MS);
    }

    zmq_close(pub);
    zmq_ctx_destroy(contexto);
    return 0;
}
