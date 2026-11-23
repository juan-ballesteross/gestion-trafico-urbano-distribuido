#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <zmq.h>
#include "../config.h"

/**
 * sensor_camara.c — Simulador de cámara de tráfico (PC1)
 *
 * Qué mide (simulado):
 *   Longitud de cola (vehículos esperando) y velocidad promedio del flujo
 *   en una intersección. En un despliegue real, estos datos vendrían de
 *   visión por computadora; aquí se generan aleatoriamente.
 *
 * Flujo en el sistema distribuido:
 *   [este proceso] --PUB--> Broker XSUB --XPUB--> Analítica / BD
 *
 * Comunicación ZMQ:
 *   - Socket: ZMQ_PUB conectado a tcp://<broker_host>:PORT_BROKER_XSUB
 *   - Cada envío es un mensaje de dos partes: tópico + JSON
 *   - Tópico: "EVENT_LONGITUD_COLA"
 *   - Intervalo entre lecturas: SENSOR_CAMARA_INTERVALO_MS (config.h)
 *
 * Ejemplo de payload JSON:
 * {
 *   "tipo": "EVENT_LONGITUD_COLA",
 *   "sensor_id": "CAM_INT_B3",
 *   "interseccion": "INT_B3",
 *   "volumen": 3,
 *   "velocidad_promedio": 52.4,
 *   "timestamp": 1712170830
 * }
 *
 * Uso:
 *   sensor_camara [interseccion] [broker_host]
 *   interseccion  — ID de la intersección (default: INT_A1)
 *   broker_host   — IP o hostname del broker (default: localhost)
 */

/**
 * Arma el JSON de una lectura simulada de cámara.
 * Escribe el resultado en buffer (máximo size bytes).
 */
static void generar_evento_json(char *buffer, size_t size, const char *sensor_id, const char *interseccion) {
    /* volumen: vehículos en cola (0–5, escenario poco congestionado) */
    int volumen = rand() % 6;
    /* velocidad: km/h en rango fluido 40–60 */
    double velocidad = 40.0 + ((rand() % 200) / 10.0);
    long timestamp = (long)time(NULL);

    snprintf(buffer, size,
        "{"
        "\"tipo\":\"EVENT_LONGITUD_COLA\","
        "\"sensor_id\":\"%s\","
        "\"interseccion\":\"%s\","
        "\"volumen\":%d,"
        "\"velocidad_promedio\":%.1f,"
        "\"timestamp\":%ld"
        "}",
        sensor_id, interseccion,
        volumen, velocidad,
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

    /* Identificador único del sensor, p. ej. CAM_INT_B3 */
    char sensor_id[64];
    snprintf(sensor_id, sizeof(sensor_id), "CAM_%s", interseccion);

    char broker_addr[128];
    snprintf(broker_addr, sizeof(broker_addr), "tcp://%s:%d", broker_host, PORT_BROKER_XSUB);

    /* Semilla distinta por proceso para que cada instancia simule distinto */
    srand((unsigned int)time(NULL) ^ (unsigned int)obtener_pid());

    /* ── 2. Conexión al broker ────────────────────────────────── */
    void *contexto = zmq_ctx_new();
    void *pub = zmq_socket(contexto, ZMQ_PUB);

    if (zmq_connect(pub, broker_addr) != 0) {
        fprintf(stderr, "[CAMARA:%s] Error conectando a %s: %s\n",
                sensor_id, broker_addr, zmq_strerror(zmq_errno()));
        return 1;
    }

    printf("[CAMARA:%s] Conectado a %s | Interseccion: %s\n",
           sensor_id, broker_addr, interseccion);

    /* Dar tiempo al broker/suscriptores antes del primer envío (patrón PUB) */
    dormir_ms(1000);

    /* ── 3. Bucle de publicación ──────────────────────────────── */
    char topico[] = "EVENT_LONGITUD_COLA";
    char json[BUFFER_SIZE];

    while (1) {
        generar_evento_json(json, sizeof(json), sensor_id, interseccion);

        /* Mensaje multipart ZMQ: parte 1 = tópico, parte 2 = cuerpo JSON */
        zmq_send(pub, topico, strlen(topico), ZMQ_SNDMORE);
        zmq_send(pub, json, strlen(json), 0);

        printf("[CAMARA:%s] %s -> %s\n", sensor_id, interseccion, json);

        dormir_ms(SENSOR_CAMARA_INTERVALO_MS);
    }

    zmq_close(pub);
    zmq_ctx_destroy(contexto);
    return 0;
}
