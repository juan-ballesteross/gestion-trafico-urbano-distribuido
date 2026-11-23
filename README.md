# Gestión Inteligente de Tráfico Urbano
**Pontificia Universidad Javeriana — Introducción a Sistemas Distribuidos 2026-10**

## Integrantes
- Juan José Ballesteros
- Juan Diego Rojas
- Danna Gabriela Rojas Bernal

---

## Arquitectura del Sistema

```
PC1 (Sensores + Broker)          PC2 (Analítica + Control)       PC3 (Monitor + BD)
─────────────────────────        ────────────────────────────     ─────────────────────
sensor_espira  ─┐                 analitica                        monitor (CLI operador)
sensor_camara  ─┼─PUB─> broker   ├─ SUB <- broker (PC1:5556)      db_principal (SQLite)
sensor_gps     ─┘       XSUB     ├─ PUSH -> control (5557)
semaforo <──PUSH─────   :5555    ├─ PUSH -> db_principal (5558)
           (PC2→PC1)    XPUB     ├─ PUSH -> db_replica (5559)
                        :5556    └─ REP :5560 <- monitor REQ

                                 control_semaforos
                                 ├─ PULL :5557 <- analitica
                                 └─ PUSH -> semaforo (PC1:5562)

                                 db_replica (SQLite — backup)
                                 ├─ PULL :5559 <- analitica
                                 └─ REP :5564  (failover si PC3 cae)
```

## Requisitos

```bash
# Ubuntu / Debian
sudo apt install libzmq3-dev libsqlite3-dev gcc make

# Fedora / RHEL
sudo dnf install zeromq-devel sqlite-devel gcc make
```

## Compilación

```bash
# Compilar todo
make all

# O por PC
make pc1   # broker, broker_multihilo, sensor_espira, sensor_camara, sensor_gps, semaforo
make pc2   # analitica, control_semaforos, db_replica
make pc3   # monitor, db_principal

make clean
```

Los binarios quedan en `bin/`.

---

## Ejecución

### Configurar IPs en `src/config.h`
```c
#define PC1_HOST "10.43.98.231"   // IP real de PC1
#define PC2_HOST "10.43.100.114"  // IP real de PC2
#define PC3_HOST "10.43.100.115"  // IP real de PC3
```
Recompilar tras cambiar las IPs.

---

### PC3 — Iniciar primero

```bash
# BD Principal (dejar corriendo en background)
./bin/db_principal

# Monitor (interfaz del operador)
./bin/monitor [ip_pc2] [ip_local_bd]
# Ejemplo usando IPs del config.h:
./bin/monitor
```

### PC2 — Iniciar segundo

```bash
# BD Réplica
./bin/db_replica

# Control de semáforos
./bin/control_semaforos [ip_pc1]
./bin/control_semaforos   # usa PC1_HOST por defecto

# Analítica (parámetros: broker_host  pc3_host  lista_semaforos_csv)
./bin/analitica [ip_pc1] [ip_pc3] [semaforos_csv]
./bin/analitica                    # usa defaults del config.h
```

### PC1 — Iniciar último

```bash
# Broker ZMQ original
./bin/broker

# Broker ZMQ multihilo para pruebas de rendimiento
# Usa los mismos puertos del broker original (5555/5556 por defecto)
./bin/broker_multihilo

# Semáforos (parámetro: lista CSV de intersecciones)
# Sintaxis extendida: ID[:H|:V] — por ejemplo INT_C4:V forza orientación vertical.
./bin/semaforo 5562 "INT_B3:V,INT_C4:V,INT_C2:H"
./bin/semaforo   # usa DEFAULT_SEMAFOROS del config.h (si DEFAULT_NUM_SEMAFOROS>0)

# Sensores (parámetro: intersección  broker_host)
./bin/sensor_espira INT_B3 [ip_pc1]
./bin/sensor_camara INT_C4 [ip_pc1]
./bin/sensor_gps    INT_C2 [ip_pc1]
```

---

## Notas sobre inicialización de semáforos y orientaciones
- La lista CSV de semáforos admite un sufijo opcional `:H` o `:V` para forzar que el semáforo controle el flujo horizontal o vertical respectivamente.
- Si no se especifica orientación, el sistema infiere una orientación por paridad de columna (columna impar → HORIZONTAL; columna par → VERTICAL) como política por defecto.
- Comportamiento de inicialización en tiempo de ejecución:
  - Si se proporciona una lista CSV al ejecutar `semaforo` (argv[2]), se usa esa lista.
  - Si no se proporciona lista y `DEFAULT_NUM_SEMAFOROS > 0` en `config.h`, se usa `DEFAULT_SEMAFOROS`.
  - Si no hay lista y `DEFAULT_NUM_SEMAFOROS == 0`, se inicializa todo el grid según `FLUJO_FILAS` y `FLUJO_COLUMNAS`.
- Estados iniciales: los semáforos con orientación HORIZONTAL arrancan en VERDE (tiempo por defecto 15s), los VERTICAL arrancan en ROJO (tiempo por defecto 15s). Los tiempos por defecto están en `config.h`.

## Formato de comandos JSON (Analítica → Control → Semáforo)
- Nuevo campo opcional: `"duracion": <segundos>` permite forzar duración de la fase en el semáforo.
- Ejemplo de comando con duración:
```
{
  "comando_id":"CMD_INT_C3_1710000000",
  "interseccion":"INT_C3",
  "nuevo_estado":"VERDE",
  "origen":"ANALITICA",
  "duracion":30,
  "timestamp":1710000000
}
```
- Analítica usa `duracion` para aplicar políticas (p. ej. congestión → dar 30s de verde al objetivo y 15s de rojo a orientación opuesta).

---

## Casos de Prueba

### Caso 1 — Tráfico Normal
Los sensores generan valores con probabilidad alta de tráfico fluido:
- Cámara: volumen 0–5 vehículos, velocidad 40–60 km/h
- Espira: 0–10 vehículos/intervalo
- GPS: 70% congestion=BAJA, 20% NORMAL, 10% ALTA

### Caso 2 — Congestión
Para forzar congestión en una intersección desde el monitor (opción 3):
```
Interseccion: INT_C3
Nuevo estado: VERDE   (simula parar tráfico de fila)
```
O editar los rangos aleatorios en el sensor para generar valores altos.

---

## Failover (Tolerancia a fallas de PC3)

El `db_replica` levanta automáticamente un socket REP en el puerto `5564`.
Si PC3 cae:
1. El **monitor** detecta timeout en health check (opción 5 del menú).
2. Automáticamente reconecta a `PC2:5564` (BD Réplica).
3. Todas las consultas posteriores van a la réplica — **transparente para el operador**.
4. La **analítica** continúa enviando eventos a la réplica vía PUSH (puerto 5559).

---

## Reglas de Tráfico

| Estado       | Condición                                              |
|--------------|--------------------------------------------------------|
| NORMAL       | cola < 10 AND velocidad > 25 km/h AND vehiculos < 15   |
| CONGESTION   | cola >= 10 OR velocidad <= 25 km/h OR vehiculos >= 15  |
| OLA_VERDE    | Comando directo del operador (menú opción 4)           |
| EMERGENCIA   | Comando directo del operador (menú opción 3 + VERDE)   |

Tiempo semáforo normal: **15 seg verde / 15 seg rojo**.

---

## Puertos ZMQ

| Puerto | Patrón   | Conexión |
|--------|----------|----------|
| 5555   | XSUB     | Sensores → Broker |
| 5556   | XPUB     | Broker → Analítica |
| 5557   | PUSH/PULL| Analítica → Control |
| 5558   | PUSH/PULL| Analítica → BD Principal |
| 5559   | PUSH/PULL| Analítica → BD Réplica |
| 5560   | REQ/REP  | Monitor ↔ Analítica |
| 5562   | PUSH/PULL| Control → Semáforos |
| 5563   | REQ/REP  | Monitor ↔ BD Principal |
| 5564   | REQ/REP  | Monitor ↔ BD Réplica (failover) |

---

## Grid de la Ciudad

```
     1(C1)   2(C2)   3(C3)   4(C4)   5(C5)
A(F1)  .       .       ↓       ↓       .
             
B(F2)  .       .       [S]     ↓       .       
                                 [S]
C(F3)  -->  .   [S]     .      .       .       
         
D(F4)   .       .       .      .       .
```
Leyenda: `-->` = flujo horizontal (fila), `↓` = flujo vertical (columna), `[S]` = semáforo

Notas:
- Valores usados (ver `src/config.h`):
  - FLUJO_FILAS = {0,0,1,0} → sólo la fila C (F3) tiene flujo horizontal activo.
  - FLUJO_COLUMNAS = {0,0,1,1,0} → las columnas C3 y C4 tienen flujo vertical activo.
  - DEFAULT_SEMAFOROS = "INT_B3:V,INT_C4:V,INT_C2:H" → semáforos por defecto en B2 (vertical), C4 (vertical) y C2 (horizontal).

Esta representación coincide con la visualización que muestra el Monitor (opción 6) y con la inicialización automática en `semaforo.c`.

