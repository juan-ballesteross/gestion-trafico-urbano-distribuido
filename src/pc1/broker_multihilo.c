#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <zmq.h>
#include "../config.h"

#define DATA_ENDPOINT "inproc://broker-data"
#define SUB_ENDPOINT "inproc://broker-subscriptions"
#define POLL_TIMEOUT_MS 250

static volatile sig_atomic_t g_running = 1;

typedef struct {
    void *contexto;
    int puerto_xsub;
    int puerto_xpub;
    int data_pipe_ready;
    int subs_pipe_ready;
    int startup_failed;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
} broker_state_t;

static void manejar_senal(int signum) {
    (void)signum;
    g_running = 0;
}

static void configurar_socket_basico(void *socket) {
    int linger = 0;
    int hwm = 10000;

    zmq_setsockopt(socket, ZMQ_LINGER, &linger, sizeof(linger));
    zmq_setsockopt(socket, ZMQ_SNDHWM, &hwm, sizeof(hwm));
    zmq_setsockopt(socket, ZMQ_RCVHWM, &hwm, sizeof(hwm));
}

static void notificar_error_inicio(broker_state_t *state, const char *etiqueta, const char *addr) {
    pthread_mutex_lock(&state->mutex);
    state->startup_failed = 1;
    pthread_cond_broadcast(&state->cond);
    pthread_mutex_unlock(&state->mutex);

    g_running = 0;
    fprintf(stderr, "[BROKER_MT] Error en %s (%s): %s\n", etiqueta, addr, zmq_strerror(zmq_errno()));
}

static int esperar_flag(broker_state_t *state, int *flag) {
    pthread_mutex_lock(&state->mutex);
    while (g_running && !state->startup_failed && !(*flag)) {
        pthread_cond_wait(&state->cond, &state->mutex);
    }
    int ok = !state->startup_failed && *flag;
    pthread_mutex_unlock(&state->mutex);
    return ok;
}

static int reenviar_multipart(void *origen, void *destino, const char *etiqueta) {
    while (g_running) {
        zmq_msg_t frame;
        int more = 0;

        if (zmq_msg_init(&frame) != 0) {
            fprintf(stderr, "[BROKER_MT] Error inicializando frame en %s\n", etiqueta);
            return -1;
        }

        if (zmq_msg_recv(&frame, origen, 0) == -1) {
            int err = zmq_errno();
            zmq_msg_close(&frame);

            if (err == EAGAIN || err == EINTR || err == ETERM) {
                return g_running ? 0 : -1;
            }

            fprintf(stderr, "[BROKER_MT] Error recibiendo en %s: %s\n", etiqueta, zmq_strerror(err));
            return -1;
        }

        more = zmq_msg_more(&frame);
        if (zmq_msg_send(&frame, destino, more ? ZMQ_SNDMORE : 0) == -1) {
            int err = zmq_errno();
            zmq_msg_close(&frame);

            if (err == EAGAIN || err == EINTR || err == ETERM) {
                return g_running ? 0 : -1;
            }

            fprintf(stderr, "[BROKER_MT] Error enviando en %s: %s\n", etiqueta, zmq_strerror(err));
            return -1;
        }

        zmq_msg_close(&frame);

        if (!more) {
            return 0;
        }
    }

    return -1;
}

static void *worker_xsub(void *arg) {
    broker_state_t *state = (broker_state_t *)arg;
    void *xsub = NULL;
    void *data_push = NULL;
    void *subs_pull = NULL;
    char addr_sub[64];

    snprintf(addr_sub, sizeof(addr_sub), "tcp://*:%d", state->puerto_xsub);

    xsub = zmq_socket(state->contexto, ZMQ_XSUB);
    subs_pull = zmq_socket(state->contexto, ZMQ_PULL);
    if (!xsub || !subs_pull) {
        notificar_error_inicio(state, "creacion de sockets XSUB", addr_sub);
        goto cleanup;
    }

    configurar_socket_basico(xsub);
    configurar_socket_basico(subs_pull);

    if (zmq_bind(xsub, addr_sub) != 0) {
        notificar_error_inicio(state, "bind XSUB", addr_sub);
        goto cleanup;
    }

    if (zmq_bind(subs_pull, SUB_ENDPOINT) != 0) {
        notificar_error_inicio(state, "bind canal de suscripciones", SUB_ENDPOINT);
        goto cleanup;
    }

    pthread_mutex_lock(&state->mutex);
    state->subs_pipe_ready = 1;
    pthread_cond_broadcast(&state->cond);
    pthread_mutex_unlock(&state->mutex);

    if (!esperar_flag(state, &state->data_pipe_ready)) {
        goto cleanup;
    }

    data_push = zmq_socket(state->contexto, ZMQ_PUSH);
    if (!data_push) {
        notificar_error_inicio(state, "creacion canal de datos", DATA_ENDPOINT);
        goto cleanup;
    }

    configurar_socket_basico(data_push);

    if (zmq_connect(data_push, DATA_ENDPOINT) != 0) {
        notificar_error_inicio(state, "connect canal de datos", DATA_ENDPOINT);
        goto cleanup;
    }

    while (g_running && !state->startup_failed) {
        zmq_pollitem_t items[] = {
            { xsub, 0, ZMQ_POLLIN, 0 },
            { subs_pull, 0, ZMQ_POLLIN, 0 }
        };

        int rc = zmq_poll(items, 2, POLL_TIMEOUT_MS);
        if (rc == -1) {
            if (zmq_errno() == ETERM || zmq_errno() == EINTR) {
                break;
            }
            fprintf(stderr, "[BROKER_MT] Error en poll XSUB: %s\n", zmq_strerror(zmq_errno()));
            break;
        }

        if (items[0].revents & ZMQ_POLLIN) {
            if (reenviar_multipart(xsub, data_push, "XSUB->COLA") != 0) {
                break;
            }
        }

        if (items[1].revents & ZMQ_POLLIN) {
            if (reenviar_multipart(subs_pull, xsub, "SUBSCRIPCION->XSUB") != 0) {
                break;
            }
        }
    }

cleanup:
    if (data_push) zmq_close(data_push);
    if (subs_pull) zmq_close(subs_pull);
    if (xsub) zmq_close(xsub);
    return NULL;
}

static void *worker_xpub(void *arg) {
    broker_state_t *state = (broker_state_t *)arg;
    void *xpub = NULL;
    void *data_pull = NULL;
    void *subs_push = NULL;
    char addr_pub[64];

    snprintf(addr_pub, sizeof(addr_pub), "tcp://*:%d", state->puerto_xpub);

    xpub = zmq_socket(state->contexto, ZMQ_XPUB);
    data_pull = zmq_socket(state->contexto, ZMQ_PULL);
    if (!xpub || !data_pull) {
        notificar_error_inicio(state, "creacion de sockets XPUB", addr_pub);
        goto cleanup;
    }

    configurar_socket_basico(xpub);
    configurar_socket_basico(data_pull);

    if (zmq_bind(xpub, addr_pub) != 0) {
        notificar_error_inicio(state, "bind XPUB", addr_pub);
        goto cleanup;
    }

    if (zmq_bind(data_pull, DATA_ENDPOINT) != 0) {
        notificar_error_inicio(state, "bind canal de datos", DATA_ENDPOINT);
        goto cleanup;
    }

    pthread_mutex_lock(&state->mutex);
    state->data_pipe_ready = 1;
    pthread_cond_broadcast(&state->cond);
    pthread_mutex_unlock(&state->mutex);

    if (!esperar_flag(state, &state->subs_pipe_ready)) {
        goto cleanup;
    }

    subs_push = zmq_socket(state->contexto, ZMQ_PUSH);
    if (!subs_push) {
        notificar_error_inicio(state, "creacion canal de suscripciones", SUB_ENDPOINT);
        goto cleanup;
    }

    configurar_socket_basico(subs_push);

    if (zmq_connect(subs_push, SUB_ENDPOINT) != 0) {
        notificar_error_inicio(state, "connect canal de suscripciones", SUB_ENDPOINT);
        goto cleanup;
    }

    while (g_running && !state->startup_failed) {
        zmq_pollitem_t items[] = {
            { xpub, 0, ZMQ_POLLIN, 0 },
            { data_pull, 0, ZMQ_POLLIN, 0 }
        };

        int rc = zmq_poll(items, 2, POLL_TIMEOUT_MS);
        if (rc == -1) {
            if (zmq_errno() == ETERM || zmq_errno() == EINTR) {
                break;
            }
            fprintf(stderr, "[BROKER_MT] Error en poll XPUB: %s\n", zmq_strerror(zmq_errno()));
            break;
        }

        if (items[0].revents & ZMQ_POLLIN) {
            if (reenviar_multipart(xpub, subs_push, "XPUB->SUBSCRIPCION") != 0) {
                break;
            }
        }

        if (items[1].revents & ZMQ_POLLIN) {
            if (reenviar_multipart(data_pull, xpub, "COLA->XPUB") != 0) {
                break;
            }
        }
    }

cleanup:
    if (subs_push) zmq_close(subs_push);
    if (data_pull) zmq_close(data_pull);
    if (xpub) zmq_close(xpub);
    return NULL;
}

int main(int argc, char *argv[]) {
    broker_state_t state;
    pthread_t hilo_xsub;
    pthread_t hilo_xpub;
    int puerto_sub = PORT_BROKER_XSUB;
    int puerto_pub = PORT_BROKER_XPUB;
    int exit_code = 0;

    if (argc >= 3) {
        puerto_sub = atoi(argv[1]);
        puerto_pub = atoi(argv[2]);
    }

    signal(SIGINT, manejar_senal);
    signal(SIGTERM, manejar_senal);

    memset(&state, 0, sizeof(state));
    state.puerto_xsub = puerto_sub;
    state.puerto_xpub = puerto_pub;

    pthread_mutex_init(&state.mutex, NULL);
    pthread_cond_init(&state.cond, NULL);

    state.contexto = zmq_ctx_new();
    if (!state.contexto) {
        fprintf(stderr, "[BROKER_MT] Error creando contexto ZMQ\n");
        pthread_cond_destroy(&state.cond);
        pthread_mutex_destroy(&state.mutex);
        return 1;
    }

    zmq_ctx_set(state.contexto, ZMQ_IO_THREADS, 2);

    if (pthread_create(&hilo_xsub, NULL, worker_xsub, &state) != 0) {
        fprintf(stderr, "[BROKER_MT] No se pudo crear el hilo XSUB\n");
        zmq_ctx_destroy(state.contexto);
        pthread_cond_destroy(&state.cond);
        pthread_mutex_destroy(&state.mutex);
        return 1;
    }

    if (pthread_create(&hilo_xpub, NULL, worker_xpub, &state) != 0) {
        fprintf(stderr, "[BROKER_MT] No se pudo crear el hilo XPUB\n");
        g_running = 0;
        zmq_ctx_shutdown(state.contexto);
        pthread_join(hilo_xsub, NULL);
        zmq_ctx_destroy(state.contexto);
        pthread_cond_destroy(&state.cond);
        pthread_mutex_destroy(&state.mutex);
        return 1;
    }

    printf("[BROKER_MT] Iniciado - XSUB:%d | XPUB:%d\n", puerto_sub, puerto_pub);
    printf("[BROKER_MT] Hilos activos: ingreso de publicaciones y despacho a suscriptores\n");

    while (g_running) {
        pthread_mutex_lock(&state.mutex);
        int fallo = state.startup_failed;
        pthread_mutex_unlock(&state.mutex);

        if (fallo) {
            exit_code = 1;
            break;
        }

        dormir_ms(500);
    }

    g_running = 0;
    zmq_ctx_shutdown(state.contexto);

    pthread_join(hilo_xsub, NULL);
    pthread_join(hilo_xpub, NULL);

    zmq_ctx_destroy(state.contexto);
    pthread_cond_destroy(&state.cond);
    pthread_mutex_destroy(&state.mutex);

    return exit_code;
}
