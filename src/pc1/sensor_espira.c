#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <zmq.h>
#include "../config.h"

/**
 * sensor_espira.c — Simulador de espira inductiva (PC1)
 *
 * Qué mide (simulado):
 *   Conteo de vehículos que pasan sobre la espira en un intervalo de
 *   tiempo (ventana de ~30 s simulada con timestamp_inicio / timestamp_fin).
 *
 * Flujo en el sistema distribuido:
 *   [este proceso] --PUB--> Broker XSUB --XPUB--> Analítica / BD
 *
 * Comunicación ZMQ:
 *   - Socket: ZMQ_PUB conectado a tcp://<broker_host>:PORT_BROKER_XSUB
 *   - Cada envío es un mensaje de dos partes: tópico + JSON
 *   - Tópico: "EVENT_CONTEO_VEHICULAR"
 *   - Intervalo entre lecturas: SENSOR_ESPIRA_INTERVALO_MS (config.h)
 *
 * Ejemplo de payload JSON:
 * {
 *   "tipo": "EVENT_CONTEO_VEHICULAR",
 *   "sensor_id": "ESP_INT_C5",
 *   "interseccion": "INT_C5",
 *   "vehiculos_contados": 7,
 *   "timestamp_inicio": 1712170800,
 *   "timestamp_fin": 1712170830,
 *   "timestamp": 1712170830
 * }
 *
 * Uso:
 *   sensor_espira [interseccion] [broker_host]
 *   interseccion  — ID de la intersección (default: INT_A1)
 *   broker_host   — IP o hostname del broker (default: localhost)
 */

/**
 * Arma el JSON de un conteo simulado en la ventana de medición.
 * timestamp_inicio y timestamp_fin delimitan el periodo contado;
 * timestamp es el instante de envío del evento.
 */
static void generar_evento_json(char *buffer, size_t size, const char *sensor_id, const char *interseccion) {
    /* vehículos detectados en la ventana (0–10, escenario poco congestionado) */
    int vehiculos = rand() % 11;
    long timestamp = (long)time(NULL);

    snprintf(buffer, size,
        "{"
        "\"tipo\":\"EVENT_CONTEO_VEHICULAR\","
        "\"sensor_id\":\"%s\","
        "\"interseccion\":\"%s\","
        "\"vehiculos_contados\":%d,"
        "\"timestamp_inicio\":%ld,"
        "\"timestamp_fin\":%ld,"
        "\"timestamp\":%ld"
        "}",
        sensor_id, interseccion,
        vehiculos,
        timestamp - 30, timestamp,
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

    /* Identificador único del sensor, p. ej. ESP_INT_C5 */
    char sensor_id[64];
    snprintf(sensor_id, sizeof(sensor_id), "ESP_%s", interseccion);

    char broker_addr[128];
    snprintf(broker_addr, sizeof(broker_addr), "tcp://%s:%d", broker_host, PORT_BROKER_XSUB);

    srand((unsigned int)time(NULL) ^ (unsigned int)obtener_pid());

    /* ── 2. Conexión al broker ────────────────────────────────── */
    void *contexto = zmq_ctx_new();
    void *pub = zmq_socket(contexto, ZMQ_PUB);

    if (zmq_connect(pub, broker_addr) != 0) {
        fprintf(stderr, "[ESPIRA:%s] Error conectando a %s: %s\n",
                sensor_id, broker_addr, zmq_strerror(zmq_errno()));
        return 1;
    }

    printf("[ESPIRA:%s] Conectado a %s | Interseccion: %s\n",
           sensor_id, broker_addr, interseccion);

    dormir_ms(1000);

    /* ── 3. Bucle de publicación ──────────────────────────────── */
    char topico[] = "EVENT_CONTEO_VEHICULAR";
    char json[BUFFER_SIZE];

    while (1) {
        generar_evento_json(json, sizeof(json), sensor_id, interseccion);

        zmq_send(pub, topico, strlen(topico), ZMQ_SNDMORE);
        zmq_send(pub, json, strlen(json), 0);

        printf("[ESPIRA:%s] %s -> %s\n", sensor_id, interseccion, json);

        dormir_ms(SENSOR_ESPIRA_INTERVALO_MS);
    }

    zmq_close(pub);
    zmq_ctx_destroy(contexto);
    return 0;
}
