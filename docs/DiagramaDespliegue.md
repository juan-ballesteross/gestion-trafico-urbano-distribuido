## Diagrama de Despliegue

```plantuml
@startuml diagrama_despliegue
!theme plain
skinparam defaultFontSize 11
skinparam nodeBorderColor #888888
skinparam nodeFontSize 12
skinparam ArrowColor #555555
skinparam componentStyle rectangle
skinparam databaseBackgroundColor #DDEEFF

title Gestión Inteligente de Tráfico Urbano - Diagrama de Despliegue

actor "Operador" as User

' ─────────────────────────────────────────────────
node "PC1 — Sensores y Broker\n[IP: 10.43.98.231]" as PC1 #E1F5EE {

  node "Proceso: broker" as PROC_Brk {
    component "broker\n(XSUB :5555 / XPUB :5556)" as BROKER
  }

  node "Procesos: sensores" as PROC_Sen {
    component "sensor_espira\n(PUB → :5555)" as ESP
    component "sensor_camara\n(PUB → :5555)" as CAM
    component "sensor_gps\n(PUB → :5555)" as GPS
  }

  node "Proceso: semaforo" as PROC_Sem {
    component "semaforo\n(PULL :5562)" as SEM
  }

  ESP  --> BROKER : PUB → tcp://10.43.98.231:5555
  CAM  --> BROKER : PUB → tcp://10.43.98.231:5555
  GPS  --> BROKER : PUB → tcp://10.43.98.231:5555
}

' ─────────────────────────────────────────────────
node "PC2 — Analítica y Control\n[IP: 10.43.100.114]" as PC2 #FAECE7 {

  node "Proceso: analitica" as PROC_Ana {
    component "analitica\n(SUB ← :5556 | PUSH → :5557\nPUSH → :5558 | PUSH → :5559\nREP :5560)" as ANA
  }

  node "Proceso: control_semaforos" as PROC_Ctrl {
    component "control_semaforos\n(PULL ← :5557 | PUSH → :5562)" as CTRL
  }

  database "BD Réplica\n(SQLite — trafico_replica.db)" as BD_REP {
    component "db_replica\n(PULL ← :5559)" as DB_R
  }
}

' ─────────────────────────────────────────────────
node "PC3 — Monitoreo y BD Principal\n[IP: 10.43.100.115]" as PC3 #EAF3DE {

  node "Proceso: monitor" as PROC_Mon {
    component "monitor\n(REP :5561 | REQ → :5560)" as MON
  }

  database "BD Principal\n(SQLite — trafico_principal.db)" as BD_PRIN {
    component "db_principal\n(PULL ← :5558 | REQ/REP :5563)" as DB_P
  }
}

' ─────────────────────────────────────────────────
' Comunicación entre nodos
' ─────────────────────────────────────────────────

' PUB/SUB: Broker PC1 → Analítica PC2
BROKER --> ANA    : ZMQ SUB\ntcp://10.43.98.231:5556

' PUSH/PULL: Analítica → Control (intra-PC2)
ANA    --> CTRL   : ZMQ PUSH\ntcp://localhost:5557

' PUSH/PULL: Control → Semáforos PC1
CTRL   --> SEM    : ZMQ PUSH\ntcp://10.43.98.231:5562

' PUSH/PULL: Analítica → BD Principal PC3
ANA    --> DB_P   : ZMQ PUSH\ntcp://10.43.100.115:5558

' PUSH/PULL: Analítica → BD Réplica (intra-PC2)
ANA    --> DB_R   : ZMQ PUSH\ntcp://localhost:5559

' REQ/REP: Monitor ↔ Analítica
MON    <--> ANA   : ZMQ REQ/REP\ntcp://10.43.100.114:5560

' REQ/REP: BD Principal ↔ Monitor (consultas históricas)
MON    <--> DB_P  : ZMQ REQ/REP\ntcp://10.43.100.115:5563

' Usuario → Monitor
User   --> MON    : REQ/REP\ntcp://10.43.100.115:5561

' Failover condicional
ANA    ..> DB_R   : <<failover>>\nsi PC3 no responde
MON    ..> BD_REP : <<failover>>\nsi PC3 no responde

@enduml
```

### Tabla de Puertos ZMQ

| Puerto | Protocolo | Origen | Destino |
|--------|-----------|--------|---------|
| `5555` | PUB/XSUB  | Sensores (PC1) | Broker XSUB (PC1) |
| `5556` | XPUB/SUB  | Broker XPUB (PC1) | Analítica SUB (PC2) |
| `5557` | PUSH/PULL | Analítica (PC2) | Control Semáforos (PC2) |
| `5558` | PUSH/PULL | Analítica (PC2) | BD Principal (PC3) |
| `5559` | PUSH/PULL | Analítica (PC2) | BD Réplica (PC2) |
| `5560` | REQ/REP   | Monitor (PC3) ↔ Analítica (PC2) | bidireccional |
| `5561` | REQ/REP   | Operador ↔ Monitor (PC3) | bidireccional |
| `5562` | PUSH/PULL | Control Semáforos (PC2) | Semáforos (PC1) |
| `5563` | REQ/REP   | Monitor (PC3) ↔ BD Principal (PC3) | bidireccional |
| `5564` | REQ/REP   | Monitor (PC3) ↔ BD Réplica (PC2) | solo en failover |

### Artefactos por Nodo

| Nodo | Ejecutable | Compilación |
|------|-----------|-------------|
| PC1  | `broker`, `sensor_espira`, `sensor_camara`, `sensor_gps`, `semaforo` | `make pc1` |
| PC2  | `analitica`, `control_semaforos`, `db_replica` | `make pc2` |
| PC3  | `monitor`, `db_principal` | `make pc3` |