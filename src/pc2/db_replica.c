#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <zmq.h>
#include <sqlite3.h>
#include "../config.h"

/**
 * Base de Datos Réplica (PC2).
 *
 * Sockets ZMQ:
 *   PULL ← bind en :5559 — recibe réplica de eventos desde Analítica
 *
 * Persistencia:
 *   SQLite local (trafico_replica.db) — backup de BD Principal (PC3)
 *
 * Failover:
 *   Si PC3 falla, este proceso se activa como primaria.
 *   En ese caso, el Monitor redirige consultas REQ/REP aquí.
 *
 * Uso: db_replica
 */

/* ── Inicializar tablas SQLite ──────────────────── */

static int inicializar_tablas(sqlite3 *db) {
    const char *sql_eventos =
        "CREATE TABLE IF NOT EXISTS eventos ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  tipo TEXT NOT NULL,"
        "  sensor_id TEXT,"
        "  interseccion TEXT NOT NULL,"
        "  datos_json TEXT NOT NULL,"
        "  timestamp INTEGER NOT NULL,"
        "  fecha_recepcion TEXT DEFAULT (datetime('now','localtime'))"
        ");";
    const char *sql_acciones =
        "CREATE TABLE IF NOT EXISTS acciones ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  comando_id TEXT,"
        "  interseccion TEXT NOT NULL,"
        "  nuevo_estado TEXT NOT NULL,"
        "  origen TEXT,"
        "  timestamp INTEGER NOT NULL,"
        "  fecha_recepcion TEXT DEFAULT (datetime('now','localtime'))"
        ");";

    char *err_msg = NULL;
    int rc = sqlite3_exec(db, sql_eventos, NULL, NULL, &err_msg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "[DB_REPLICA] Error creando tabla eventos: %s\n", err_msg);
        sqlite3_free(err_msg);
        return -1;
    }

    rc = sqlite3_exec(db, sql_acciones, NULL, NULL, &err_msg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "[DB_REPLICA] Error creando tabla acciones: %s\n", err_msg);
        sqlite3_free(err_msg);
        return -1;
    }

    printf("[DB_REPLICA] Tablas inicializadas correctamente\n");
    return 0;
}

/* ── Extraer string de JSON ─────────────────────── */

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

static int json_extraer_long(const char *json, const char *clave, long *destino) {
    char patron[64];
    snprintf(patron, sizeof(patron), "\"%s\":", clave);
    const char *inicio = strstr(json, patron);
    if (!inicio) return 0;
    inicio += strlen(patron);
    *destino = atol(inicio);
    return 1;
}

static int es_mensaje_accion(const char *json) {
    return strstr(json, "\"comando_id\"") != NULL &&
           strstr(json, "\"nuevo_estado\"") != NULL;
}

/* ── Guardar evento en SQLite ──────────────────── */

static int guardar_evento(sqlite3 *db, const char *json) {
    char tipo[64] = {0};
    char sensor_id[64] = {0};
    char interseccion[16] = {0};
    long timestamp = 0;

    json_extraer_string(json, "tipo", tipo, sizeof(tipo));
    json_extraer_string(json, "sensor_id", sensor_id, sizeof(sensor_id));
    json_extraer_string(json, "interseccion", interseccion, sizeof(interseccion));
    json_extraer_long(json, "timestamp", &timestamp);

    const char *sql = "INSERT INTO eventos (tipo, sensor_id, interseccion, datos_json, timestamp) VALUES (?, ?, ?, ?, ?)";
    sqlite3_stmt *stmt;

    int rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "[DB_REPLICA] Error preparando INSERT: %s\n", sqlite3_errmsg(db));
        return -1;
    }

    sqlite3_bind_text(stmt, 1, tipo, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, sensor_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 3, interseccion, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 4, json, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(stmt, 5, (sqlite3_int64)timestamp);

    rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) {
        fprintf(stderr, "[DB_REPLICA] Error ejecutando INSERT: %s\n", sqlite3_errmsg(db));
        sqlite3_finalize(stmt);
        return -1;
    }

    sqlite3_finalize(stmt);
    return 0;
}

static int guardar_accion(sqlite3 *db, const char *json) {
    char comando_id[64] = {0};
    char interseccion[16] = {0};
    char nuevo_estado[8] = {0};
    char origen[32] = {0};
    long timestamp = 0;

    json_extraer_string(json, "comando_id", comando_id, sizeof(comando_id));
    json_extraer_string(json, "interseccion", interseccion, sizeof(interseccion));
    json_extraer_string(json, "nuevo_estado", nuevo_estado, sizeof(nuevo_estado));
    json_extraer_string(json, "origen", origen, sizeof(origen));
    json_extraer_long(json, "timestamp", &timestamp);

    const char *sql =
        "INSERT INTO acciones (comando_id, interseccion, nuevo_estado, origen, timestamp) "
        "VALUES (?, ?, ?, ?, ?)";
    sqlite3_stmt *stmt;

    int rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "[DB_REPLICA] Error preparando INSERT accion: %s\n", sqlite3_errmsg(db));
        return -1;
    }

    sqlite3_bind_text(stmt, 1, comando_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, interseccion, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 3, nuevo_estado, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 4, origen, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(stmt, 5, (sqlite3_int64)timestamp);

    rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) {
        fprintf(stderr, "[DB_REPLICA] Error ejecutando INSERT accion: %s\n", sqlite3_errmsg(db));
        sqlite3_finalize(stmt);
        return -1;
    }

    sqlite3_finalize(stmt);
    return 0;
}

static int guardar_mensaje(sqlite3 *db, const char *json) {
    if (es_mensaje_accion(json)) {
        return guardar_accion(db, json);
    }
    return guardar_evento(db, json);
}

/* ── Consultar eventos por intersección ──────────── */

static void consultar_interseccion(sqlite3 *db, const char *interseccion, char *respuesta, size_t resp_size) {
    const char *sql_eventos =
        "SELECT datos_json, fecha_recepcion "
        "FROM eventos WHERE interseccion = ? ORDER BY timestamp DESC LIMIT 5";
    const char *sql_acciones =
        "SELECT comando_id, nuevo_estado, origen, timestamp, fecha_recepcion "
        "FROM acciones WHERE interseccion = ? ORDER BY timestamp DESC LIMIT 5";
    sqlite3_stmt *stmt = NULL;

    int offset = snprintf(respuesta, resp_size,
                          "{\"interseccion\":\"%s\",\"fuente\":\"REPLICA\",\"eventos\":[",
                          interseccion);
    int primero = 1;

    int rc = sqlite3_prepare_v2(db, sql_eventos, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        snprintf(respuesta, resp_size, "{\"error\":\"Error en consulta: %s\"}", sqlite3_errmsg(db));
        return;
    }

    sqlite3_bind_text(stmt, 1, interseccion, -1, SQLITE_TRANSIENT);
    while (sqlite3_step(stmt) == SQLITE_ROW && offset < (int)resp_size - 128) {
        const char *datos = (const char *)sqlite3_column_text(stmt, 0);
        const char *fecha = (const char *)sqlite3_column_text(stmt, 1);

        if (!primero) {
            offset += snprintf(respuesta + offset, resp_size - offset, ",");
        }
        offset += snprintf(respuesta + offset, resp_size - offset,
                           "{\"datos\":%s,\"recibido\":\"%s\"}", datos, fecha);
        primero = 0;
    }
    sqlite3_finalize(stmt);

    offset += snprintf(respuesta + offset, resp_size - offset, "],\"acciones\":[");
    primero = 1;

    rc = sqlite3_prepare_v2(db, sql_acciones, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        snprintf(respuesta, resp_size, "{\"error\":\"Error en consulta: %s\"}", sqlite3_errmsg(db));
        return;
    }

    sqlite3_bind_text(stmt, 1, interseccion, -1, SQLITE_TRANSIENT);
    while (sqlite3_step(stmt) == SQLITE_ROW && offset < (int)resp_size - 160) {
        const char *comando_id = (const char *)sqlite3_column_text(stmt, 0);
        const char *nuevo_estado = (const char *)sqlite3_column_text(stmt, 1);
        const char *origen = (const char *)sqlite3_column_text(stmt, 2);
        sqlite3_int64 timestamp = sqlite3_column_int64(stmt, 3);
        const char *fecha = (const char *)sqlite3_column_text(stmt, 4);

        if (!primero) {
            offset += snprintf(respuesta + offset, resp_size - offset, ",");
        }
        offset += snprintf(respuesta + offset, resp_size - offset,
                           "{\"comando_id\":\"%s\",\"nuevo_estado\":\"%s\","
                           "\"origen\":\"%s\",\"timestamp\":%lld,\"recibido\":\"%s\"}",
                           comando_id ? comando_id : "",
                           nuevo_estado ? nuevo_estado : "",
                           origen ? origen : "",
                           (long long)timestamp,
                           fecha ? fecha : "");
        primero = 0;
    }

    snprintf(respuesta + offset, resp_size - offset, "]}");
    sqlite3_finalize(stmt);
}

static void consultar_historico(sqlite3 *db, long t1, long t2, char *respuesta, size_t resp_size) {
    const char *sql_eventos =
        "SELECT tipo, interseccion, datos_json, fecha_recepcion "
        "FROM eventos WHERE timestamp >= ? AND timestamp <= ? "
        "ORDER BY timestamp DESC LIMIT 20";
    const char *sql_acciones =
        "SELECT comando_id, interseccion, nuevo_estado, origen, timestamp, fecha_recepcion "
        "FROM acciones WHERE timestamp >= ? AND timestamp <= ? "
        "ORDER BY timestamp DESC LIMIT 20";
    sqlite3_stmt *stmt = NULL;

    int offset = snprintf(respuesta, resp_size,
                          "{\"fuente\":\"REPLICA\",\"t1\":%ld,\"t2\":%ld,\"eventos\":[",
                          t1, t2);
    int primero = 1;

    int rc = sqlite3_prepare_v2(db, sql_eventos, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        snprintf(respuesta, resp_size, "{\"error\":\"Error en consulta: %s\"}", sqlite3_errmsg(db));
        return;
    }

    sqlite3_bind_int64(stmt, 1, (sqlite3_int64)t1);
    sqlite3_bind_int64(stmt, 2, (sqlite3_int64)t2);
    while (sqlite3_step(stmt) == SQLITE_ROW && offset < (int)resp_size - 220) {
        const char *tipo  = (const char *)sqlite3_column_text(stmt, 0);
        const char *inter = (const char *)sqlite3_column_text(stmt, 1);
        const char *datos = (const char *)sqlite3_column_text(stmt, 2);
        const char *fecha = (const char *)sqlite3_column_text(stmt, 3);

        if (!primero) {
            offset += snprintf(respuesta + offset, resp_size - offset, ",");
        }
        offset += snprintf(respuesta + offset, resp_size - offset,
                           "{\"tipo\":\"%s\",\"interseccion\":\"%s\","
                           "\"datos\":%s,\"recibido\":\"%s\"}",
                           tipo ? tipo : "",
                           inter ? inter : "",
                           datos ? datos : "{}",
                           fecha ? fecha : "");
        primero = 0;
    }
    sqlite3_finalize(stmt);

    offset += snprintf(respuesta + offset, resp_size - offset, "],\"acciones\":[");
    primero = 1;

    rc = sqlite3_prepare_v2(db, sql_acciones, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        snprintf(respuesta, resp_size, "{\"error\":\"Error en consulta: %s\"}", sqlite3_errmsg(db));
        return;
    }

    sqlite3_bind_int64(stmt, 1, (sqlite3_int64)t1);
    sqlite3_bind_int64(stmt, 2, (sqlite3_int64)t2);
    while (sqlite3_step(stmt) == SQLITE_ROW && offset < (int)resp_size - 220) {
        const char *comando_id = (const char *)sqlite3_column_text(stmt, 0);
        const char *inter = (const char *)sqlite3_column_text(stmt, 1);
        const char *nuevo_estado = (const char *)sqlite3_column_text(stmt, 2);
        const char *origen = (const char *)sqlite3_column_text(stmt, 3);
        sqlite3_int64 timestamp = sqlite3_column_int64(stmt, 4);
        const char *fecha = (const char *)sqlite3_column_text(stmt, 5);

        if (!primero) {
            offset += snprintf(respuesta + offset, resp_size - offset, ",");
        }
        offset += snprintf(respuesta + offset, resp_size - offset,
                           "{\"comando_id\":\"%s\",\"interseccion\":\"%s\","
                           "\"nuevo_estado\":\"%s\",\"origen\":\"%s\","
                           "\"timestamp\":%lld,\"recibido\":\"%s\"}",
                           comando_id ? comando_id : "",
                           inter ? inter : "",
                           nuevo_estado ? nuevo_estado : "",
                           origen ? origen : "",
                           (long long)timestamp,
                           fecha ? fecha : "");
        primero = 0;
    }

    snprintf(respuesta + offset, resp_size - offset, "]}");
    sqlite3_finalize(stmt);
}

/* ── Procesar petición REQ del Monitor (modo failover) ── */

static void procesar_peticion_failover(sqlite3 *db, void *rep_socket) {
    char buffer[BUFFER_SIZE];
    int size = zmq_recv(rep_socket, buffer, sizeof(buffer) - 1, 0);
    if (size < 0) return;
    buffer[size] = '\0';

    char tipo_peticion[64] = {0};

    /* Extraer tipo */
    char patron[] = "\"tipo\":\"";
    const char *inicio = strstr(buffer, patron);
    if (inicio) {
        inicio += strlen(patron);
        const char *fin = strchr(inicio, '"');
        if (fin) {
            size_t len = (size_t)(fin - inicio);
            if (len >= sizeof(tipo_peticion)) len = sizeof(tipo_peticion) - 1;
            strncpy(tipo_peticion, inicio, len);
            tipo_peticion[len] = '\0';
        }
    }

    char respuesta[BUFFER_SIZE * 2];

    if (strcmp(tipo_peticion, "CONSULTAR_INTERSECCION") == 0) {
        char id[16] = {0};
        json_extraer_string(buffer, "interseccion", id, sizeof(id));
        printf("[DB_REPLICA] (FAILOVER) Consulta interseccion: %s\n", id);
        consultar_interseccion(db, id, respuesta, sizeof(respuesta));
        zmq_send(rep_socket, respuesta, strlen(respuesta), 0);

    } else if (strcmp(tipo_peticion, "CONSULTAR_HISTORICO") == 0) {
        long t1 = 0, t2 = 0;
        json_extraer_long(buffer, "t1", &t1);
        json_extraer_long(buffer, "t2", &t2);
        printf("[DB_REPLICA] (FAILOVER) Consulta historico: %ld - %ld\n", t1, t2);
        consultar_historico(db, t1, t2, respuesta, sizeof(respuesta));
        zmq_send(rep_socket, respuesta, strlen(respuesta), 0);

    } else if (strcmp(tipo_peticion, "HEALTH_CHECK") == 0) {
        const char *ok = "{\"status\":\"OK\",\"fuente\":\"REPLICA\"}";
        zmq_send(rep_socket, ok, strlen(ok), 0);

    } else {
        const char *err = "{\"error\":\"Tipo de peticion desconocido\"}";
        zmq_send(rep_socket, err, strlen(err), 0);
    }
}

/* ── Main ──────────────────────────────────────── */

int main(int argc, char *argv[]) {
    (void)argc;
    (void)argv;

    /* Abrir/crear la base de datos SQLite */
    sqlite3 *db;
    int rc = sqlite3_open("trafico_replica.db", &db);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "[DB_REPLICA] Error abriendo SQLite: %s\n", sqlite3_errmsg(db));
        return 1;
    }
    printf("[DB_REPLICA] Base de datos trafico_replica.db abierta\n");

    if (inicializar_tablas(db) != 0) {
        sqlite3_close(db);
        return 1;
    }

    /* Crear socket PULL para recibir eventos de Analítica */
    void *contexto = zmq_ctx_new();
    void *pull_socket = zmq_socket(contexto, ZMQ_PULL);

    char addr_pull[128];
    snprintf(addr_pull, sizeof(addr_pull), "tcp://*:%d", PORT_BD_REPLICA_PULL);
    if (zmq_bind(pull_socket, addr_pull) != 0) {
        fprintf(stderr, "[DB_REPLICA] Error bind PULL en %s: %s\n", addr_pull, zmq_strerror(zmq_errno()));
        sqlite3_close(db);
        return 1;
    }
    printf("[DB_REPLICA] PULL escuchando en %s\n", addr_pull);

    /* ── REP socket: failover — mismo puerto que BD Principal ── */
    /* Puerto alternativo para no colisionar si la BD Principal sigue activa en PC3.
       El monitor cambia a PORT_BD_REPLICA_REP cuando detecta falla de PC3. */
    void *rep_socket = zmq_socket(contexto, ZMQ_REP);
    char addr_rep[128];
    snprintf(addr_rep, sizeof(addr_rep), "tcp://*:%d", PORT_BD_REPLICA_REP);
    if (zmq_bind(rep_socket, addr_rep) != 0) {
        fprintf(stderr, "[DB_REPLICA] Error bind REP en %s: %s\n",
                addr_rep, zmq_strerror(zmq_errno()));
        zmq_close(rep_socket);
        rep_socket = NULL;
    } else {
        printf("[DB_REPLICA] REP escuchando en %s (listo para failover)\n", addr_rep);
    }

    printf("[DB_REPLICA] Servicio iniciado — esperando eventos para replicar...\n\n");

    char buffer[BUFFER_SIZE];
    int eventos_guardados = 0;
    int acciones_guardadas = 0;

    /* zmq_poll sobre PULL y REP simultáneamente */
    zmq_pollitem_t items[2];
    items[0].socket  = pull_socket;
    items[0].fd      = 0;
    items[0].events  = ZMQ_POLLIN;
    items[0].revents = 0;
    items[1].socket  = rep_socket;
    items[1].fd      = 0;
    items[1].events  = ZMQ_POLLIN;
    items[1].revents = 0;

    int num_items = rep_socket ? 2 : 1;

    while (1) {
        int poll_rc = zmq_poll(items, num_items, -1);
        if (poll_rc < 0) break;

        /* Evento PULL: replicar en SQLite */
        if (items[0].revents & ZMQ_POLLIN) {
            int size = zmq_recv(pull_socket, buffer, sizeof(buffer) - 1, 0);
            if (size >= 0) {
                buffer[size] = '\0';
                if (guardar_mensaje(db, buffer) == 0) {
                    char interseccion[16] = {0};
                    json_extraer_string(buffer, "interseccion", interseccion, sizeof(interseccion));

                    if (es_mensaje_accion(buffer)) {
                        acciones_guardadas++;
                        printf("[DB_REPLICA] Accion #%d guardada (%s)\n",
                               acciones_guardadas, interseccion);
                    } else {
                        eventos_guardados++;
                        printf("[DB_REPLICA] Evento #%d guardado (%s)\n",
                               eventos_guardados, interseccion);
                    }
                }
            }
        }

        /* Consulta REP: responder como failover de BD Principal */
        if (num_items == 2 && (items[1].revents & ZMQ_POLLIN)) {
            printf("[DB_REPLICA] Peticion de failover recibida\n");
            procesar_peticion_failover(db, rep_socket);
        }
    }

    /* Cleanup */
    zmq_close(pull_socket);
    if (rep_socket) zmq_close(rep_socket);
    zmq_ctx_destroy(contexto);
    sqlite3_close(db);

    return 0;
}
