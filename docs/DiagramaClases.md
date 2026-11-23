# Diagramas de la Gestión Inteligente de Tráfico Urbano

## Diagrama de Clases (representación del código en C y mensajes JSON)

```plantuml
@startuml diagrama_clases
!theme plain
skinparam classAttributeIconSize 0
skinparam defaultFontSize 12
skinparam classBorderColor #888888
skinparam classBackgroundColor #FAFAFA
skinparam ArrowColor #555555

title Gestión Inteligente de Tráfico Urbano - Diagrama de clases

' ── Módulo de configuración ─────────────────────
class config_h <<module>> {
  MACROS: HOSTS, PUERTOS_ZMQ, GRID, TIEMPOS, INTERVALOS
  HEADER: config.h
}

' ── Mensajes JSON (no structs C en el código; se serializan/deserializan como strings JSON)
class EventoJSON <<message>> {
  tipo[64]             : char
  sensor_id[64]        : char
  interseccion[16]     : char
  vehiculos_contados   : int
  volumen              : int
  velocidad_promedio   : double
  nivel_congestion[16] : char
  timestamp            : long
  timestamp_inicio     : long
  timestamp_fin        : long
  comando_id[64]       : char
  nuevo_estado[8]      : char
  origen[32]           : char
  duracion             : int
  mensaje[128]         : char
}

' Nota: Los programas en C generan/consumen JSON usando funciones utilitarias simples (snprintf / búsquedas de substring).

' ── Sensores (programas independientes) ─────────
class sensor_espira <<proceso>> {
  sensor_id[64]   : char
  interseccion[16]: char
  intervalo_ms    : int
  pub_socket      : void*
  --
  generar_evento_json(buf, size) : void
  main(argc, argv)               : int
}

class sensor_camara <<proceso>> {
  sensor_id[64]   : char
  interseccion[16]: char
  intervalo_ms    : int
  pub_socket      : void*
  --
  generar_evento_json(buf, size) : void
  main(argc, argv)               : int
}

class sensor_gps <<proceso>> {
  sensor_id[64]   : char
  interseccion[16]: char
  intervalo_ms    : int
  pub_socket      : void*
  --
  generar_evento_json(buf, size) : void
  main(argc, argv)               : int
}

sensor_espira ..> EventoJSON : publica
sensor_camara ..> EventoJSON : publica
sensor_gps    ..> EventoJSON : publica

' ── Broker (proxy) ───────────────────────────────
class broker <<proceso>> {
  contexto    : void*
  xsub_socket : void*
  xpub_socket : void*
  puerto_sub  : int
  puerto_pub  : int
  --
  iniciar()         : void
  main(argc, argv)  : int
}

' ── Semáforos (estado en memoria) ───────────────
class SemaforoState <<struct>> {
  id[16]           : char
  estado[8]        : char
  orientacion      : int
  tiempo_verde     : int
  tiempo_rojo      : int
  tiempo_restante  : int
}

class semaforo <<proceso>> {
  grid[]       : SemaforoState
  total        : int
  pull_socket  : void*
  puerto       : int
  --
  inicializar_grid()                     : void
  inicializar_semaforos(lista_csv)      : void
  buscar_semaforo(interseccion) : SemaforoState*
  cambiar_estado(interseccion, estado)   : void
  imprimir_estados()                     : void
  json_extraer_string(json, clave, dest) : int
  escuchar_comandos()                    : void
  main(argc, argv)                       : int
}

semaforo "1" *-- "N" SemaforoState
semaforo ..> EventoJSON : recibe comandos JSON (formato de comando en el payload)

' ── Analítica y control (servicios en PC2) ─────
class analitica <<proceso>> {
  contexto     : void*
  sub_socket   : void*
  push_ctrl    : void*
  push_bd_prin : void*
  push_bd_rep  : void*
  rep_socket   : void*
  bd_activa    : int
  --
  procesar_evento(json)     : void
  evaluar_reglas(evento)    : void
  enviar_comando(cmd_json)  : void
  persistir(json)           : void
  detectar_fallo_bd()       : void
  redirigir_a_replica()     : void
  main(argc, argv)          : int
}

class control_semaforos <<proceso>> {
  contexto    : void*
  pull_socket : void*
  push_socket : void*
  --
  escuchar_comandos()        : void
  reenviar_a_semaforo(json)  : void
  main(argc, argv)           : int
}

analitica ..> EventoJSON : recibe
analitica ..> control_semaforos : genera comandos (JSON)

' ── Bases de datos y monitor (PC2/PC3) ─────────
class db_principal <<proceso>> {
  contexto    : void*
  pull_socket : void*
  rep_socket  : void*
  db          : sqlite3*
  --
  inicializar_tablas()              : void
  guardar_evento(json)              : void
  consultar_historico(t1, t2, buf)  : void
  consultar_interseccion(id, buf)   : void
  main(argc, argv)                  : int
}

class db_replica <<proceso>> {
  contexto    : void*
  pull_socket : void*
  rep_socket  : void*
  db          : sqlite3*
  es_primaria : int
  --
  inicializar_tablas()   : void
  guardar_evento(json)   : void
  activar_modo_primario(): void
  main(argc, argv)       : int
}

class monitor <<proceso>> {
  contexto       : void*
  rep_socket     : void*
  req_analitica  : void*
  req_bd         : void*
  bd_host[64]    : char
  --
  escuchar_solicitudes()         : void
  consultar_estado(id, buf)      : void
  forzar_cambio(cmd)             : void
  health_check_bd()              : int
  conmutar_a_replica()           : void
  main(argc, argv)               : int
}

' ── Relaciones entre servicios ────────────────────
analitica          --> db_principal         : PUSH\n(eventos JSON)
analitica          --> db_replica           : PUSH\n(réplica JSON)
analitica          --> control_semaforos    : PUSH\n(comandos JSON)
monitor            --> db_principal         : REQ/REP\n(consultas)
monitor            --> analitica            : REQ/REP\n(forzar cambio)
broker             --> analitica            : forward\nSUB (XPUB)
control_semaforos  --> semaforo             : PUSH\n(comando JSON)

@enduml
````
