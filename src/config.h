#ifndef CONFIG_H
#define CONFIG_H

/**
 * Configuración centralizada del sistema.
 * IPs, puertos ZMQ, constantes del grid y umbrales.
 *
 * Toda configuración se define como macros para evitar
 * estado global mutable. Para cambiar valores en runtime,
 * usar argumentos de línea de comandos en cada ejecutable.
 */

/* ── IPs de las máquinas ────────────────────────── */
#define PC1_HOST "10.43.98.231"
#define PC2_HOST "10.43.100.114"
#define PC3_HOST "10.43.100.115"

/* ── Puertos ZMQ ────────────────────────────────── */
#define PORT_BROKER_XSUB        5555  /* Sensores → Broker              */
#define PORT_BROKER_XPUB        5556  /* Broker → Analítica             */
#define PORT_CTRL_PULL          5557  /* Analítica → Control            */
#define PORT_BD_PRINCIPAL_PULL  5558  /* Analítica → BD Principal       */
#define PORT_BD_REPLICA_PULL    5559  /* Analítica → BD Réplica         */
#define PORT_ANALITICA_REP      5560  /* Monitor ↔ Analítica            */
#define PORT_MONITOR_REP        5561  /* Operador → Monitor             */
#define PORT_SEMAFORO_PULL      5562  /* Control → Semáforos             */
#define PORT_BD_PRINCIPAL_REP   5563  /* Monitor ↔ BD Principal          */
#define PORT_BD_REPLICA_REP     5564  /* Monitor ↔ BD Réplica (failover) */

/* ── Grid ───────────────────────────────────────── */
#define GRID_FILAS    4
#define GRID_COLUMNAS 5

/* ── Semáforos configurables ────────────────────── */
#define MAX_SEMAFOROS          10
/* Figura 1: semáforos en C3 (cruza F3 con C3), C4 (cruza F2 con C4), C5 (fila C col 5).
   Grid de 4 filas (A=F1, B=F2, C=F3, D=F4) y 5 columnas. */
#define DEFAULT_SEMAFOROS      "INT_B3:V,INT_C4:V,INT_C2:H"
#define DEFAULT_NUM_SEMAFOROS  3

/* ── Configuración de flujo de tráfico ──────────────────────*/
#define FLUJO_FILAS {0, 0, 1, 0} // 1: flujo activo, 0: sin flujo
#define FLUJO_COLUMNAS {0, 0, 1, 1, 0} // 1: flujo activo, 0: sin flujo

/* ── Frecuencias de sensores (ms) ───────────────── */
#define SENSOR_ESPIRA_INTERVALO_MS  10000
#define SENSOR_CAMARA_INTERVALO_MS  10000
#define SENSOR_GPS_INTERVALO_MS     10000

/* ── Umbrales de congestión ─────────────────────── */
#define UMBRAL_VEHICULOS  15
#define UMBRAL_VELOCIDAD  25.0
#define UMBRAL_COLA       10

/* ── Tiempos semáforo (seg) ─────────────────────── */
#define TIEMPO_ROJO_DEFAULT   15
#define TIEMPO_VERDE_DEFAULT  15

/* ── Failover ───────────────────────────────────── */
#define HEALTH_CHECK_TIMEOUT_MS    3000
#define HEALTH_CHECK_INTERVALO_MS  5000

/* ── Utilidades portables ───────────────────────── */
/*  entornos POSIX (Ubuntu). */
#ifndef _POSIX_C_SOURCE
  #define _POSIX_C_SOURCE 199309L
#endif
#include <time.h>
#include <unistd.h>
static inline void dormir_ms(long ms) {
    struct timespec ts;
    ts.tv_sec  = ms / 1000;
    ts.tv_nsec = (ms % 1000) * 1000000L;
    (void)nanosleep(&ts, NULL);
}
#define obtener_pid()  getpid()

/* Tamaño máximo de buffers para mensajes ZMQ */
#define BUFFER_SIZE 2048


#endif /* CONFIG_H */