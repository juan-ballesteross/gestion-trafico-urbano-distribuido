###################################################################################
#		Pontificia Universidad Javeriana
#	Autor: Juan José Ballesteros
#	       Juan Diego Rojas
#          Danna Gabriela Rojas Bernal
#	Materia: Sistemas Operativos
#	Descripcion: Archivo Makefile para la compilacion del sistema de
#                Gestion Inteligente de Trafico Urbano (PC1, PC2, PC3)
###################################################################################

GCC = gcc
CFLAGS = -Wall -Wextra -std=c11 -D_POSIX_C_SOURCE=200809L -I src
PTHREADFLAGS = -pthread

# Librerias
LDFLAGS = -lzmq
LDFLAGS_DB = -lzmq -lsqlite3

# Carpeta de salida
BIN_DIR = bin

# ──────────────────────────────────────────────────────────
# Regla principal
# ──────────────────────────────────────────────────────────
all: pc1 pc2 pc3

# ──────────────────────────────────────────────────────────
# Ejecutables por computadora
# ──────────────────────────────────────────────────────────

# PC1
pc1: $(BIN_DIR)/broker \
     $(BIN_DIR)/broker_multihilo \
     $(BIN_DIR)/sensor_espira \
     $(BIN_DIR)/sensor_camara \
     $(BIN_DIR)/sensor_gps \
     $(BIN_DIR)/semaforo

# PC2
pc2: $(BIN_DIR)/analitica \
     $(BIN_DIR)/control_semaforos \
     $(BIN_DIR)/db_replica

# PC3
pc3: $(BIN_DIR)/monitor \
     $(BIN_DIR)/db_principal

# ──────────────────────────────────────────────────────────
# Compilacion PC1
# ──────────────────────────────────────────────────────────

$(BIN_DIR)/broker: src/pc1/broker.c src/config.h
	mkdir -p $(BIN_DIR)
	$(GCC) $(CFLAGS) $< -o $@ $(LDFLAGS)

$(BIN_DIR)/broker_multihilo: src/pc1/broker_multihilo.c src/config.h
	mkdir -p $(BIN_DIR)
	$(GCC) $(CFLAGS) $(PTHREADFLAGS) $< -o $@ $(LDFLAGS) $(PTHREADFLAGS)

$(BIN_DIR)/sensor_espira: src/pc1/sensor_espira.c src/config.h
	mkdir -p $(BIN_DIR)
	$(GCC) $(CFLAGS) $< -o $@ $(LDFLAGS)

$(BIN_DIR)/sensor_camara: src/pc1/sensor_camara.c src/config.h
	mkdir -p $(BIN_DIR)
	$(GCC) $(CFLAGS) $< -o $@ $(LDFLAGS)

$(BIN_DIR)/sensor_gps: src/pc1/sensor_gps.c src/config.h
	mkdir -p $(BIN_DIR)
	$(GCC) $(CFLAGS) $< -o $@ $(LDFLAGS)

$(BIN_DIR)/semaforo: src/pc1/semaforo.c src/config.h
	mkdir -p $(BIN_DIR)
	$(GCC) $(CFLAGS) $< -o $@ $(LDFLAGS)

# ──────────────────────────────────────────────────────────
# Compilacion PC2
# ──────────────────────────────────────────────────────────

$(BIN_DIR)/analitica: src/pc2/analitica.c src/config.h
	mkdir -p $(BIN_DIR)
	$(GCC) $(CFLAGS) $< -o $@ $(LDFLAGS)

$(BIN_DIR)/control_semaforos: src/pc2/control_semaforos.c src/config.h
	mkdir -p $(BIN_DIR)
	$(GCC) $(CFLAGS) $< -o $@ $(LDFLAGS)

$(BIN_DIR)/db_replica: src/pc2/db_replica.c src/config.h
	mkdir -p $(BIN_DIR)
	$(GCC) $(CFLAGS) $< -o $@ $(LDFLAGS_DB)

# ──────────────────────────────────────────────────────────
# Compilacion PC3
# ──────────────────────────────────────────────────────────

$(BIN_DIR)/monitor: src/pc3/monitor.c src/config.h
	mkdir -p $(BIN_DIR)
	$(GCC) $(CFLAGS) $< -o $@ $(LDFLAGS)

$(BIN_DIR)/db_principal: src/pc3/db_principal.c src/config.h
	mkdir -p $(BIN_DIR)
	$(GCC) $(CFLAGS) $< -o $@ $(LDFLAGS_DB)

# ──────────────────────────────────────────────────────────
# Limpieza
# ──────────────────────────────────────────────────────────

clean:
	rm -rf $(BIN_DIR)