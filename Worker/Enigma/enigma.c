#include "../worker.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/mman.h>
#include <pthread.h>
#include <fcntl.h>
#include <ctype.h>
#include <stdio.h>

#define THREAD_POOL_SIZE 4
#define MAX_QUEUE_SIZE 10

static TaskManager task_manager;
static ConfigWorker config;
static int shared_mem_fd = -1;
static void *shared_mem = NULL;
static size_t current_mem_size = SHARED_MEM_SIZE;
pthread_mutex_t shared_mem_mutex = PTHREAD_MUTEX_INITIALIZER;
int active_threads = 0;
pthread_mutex_t threads_mutex = PTHREAD_MUTEX_INITIALIZER;

int connection_queue[MAX_QUEUE_SIZE];
int queue_front = 0, queue_rear = 0, queue_count = 0;
pthread_mutex_t queue_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t queue_not_empty = PTHREAD_COND_INITIALIZER;
pthread_cond_t queue_not_full = PTHREAD_COND_INITIALIZER;



/**
 * @brief Envia un heartbeat a Gotham per indicar que el worker segueix actiu.
 * @param signo Senyal que ha activat la funció (no utilitzat).
 */
void enviar_heartbeat(int signo) {
    (void)signo;  // Evitar warnings per paràmetre no utilitzat

    Trama heartbeat_trama;
    inicialitzar_trama(&heartbeat_trama, 0x12, "Heartbeat");

    if (write(config.base.socket_fd, &heartbeat_trama, sizeof(heartbeat_trama)) != sizeof(heartbeat_trama)) {
        write(STDERR_FILENO, "Error enviant heartbeat a Gotham. Intentant reconnectar...\n", 60);

        if (reconnectar_gotham(&config) == 0) {
            write(STDOUT_FILENO, "Reconnexió exitosa. Enviant heartbeat...\n", 43);
            write(config.base.socket_fd, &heartbeat_trama, sizeof(heartbeat_trama));
        } else {
            write(STDERR_FILENO, "Error en la reconnexió amb Gotham.\n", 36);
        }
    } else {
        write(STDOUT_FILENO, "Heartbeat enviat a Gotham\n", 27);
    }

    alarm(10);
}

/**
 * @brief Processar el fitxer de text i aplicar la distorsió, sobrescrivint el fitxer original.
 * @param file_name Nom del fitxer a processar.
 * @param threshold Límite de longitud per aplicar la distorsió.
 * @return 0 si tot va bé, -1 en cas d'error.
 */
int processar_text(const char *file_name, int threshold) {
    char tmp_name[256];
    snprintf(tmp_name, sizeof(tmp_name), "tmp_distorted_%s", file_name);

    int input_fd = open(file_name, O_RDONLY);
    int output_fd = open(tmp_name, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (input_fd < 0 || output_fd < 0) return -1;

    char ch;
    char word[256];
    int word_len = 0;

    while (read(input_fd, &ch, 1) > 0) {
        if (isspace(ch) || ispunct(ch)) {
            if (word_len >= threshold) {
                word[word_len] = '\0';
                write(output_fd, word, word_len);
                write(output_fd, " ", 1);
            }
            word_len = 0;
        } else if (word_len < (int)(sizeof(word) - 1)) {
            word[word_len++] = ch;
        }
    }

    if (word_len >= threshold) {
        write(output_fd, word, word_len);
    }

    close(input_fd);
    close(output_fd);

    if (rename(tmp_name, file_name) < 0) {
        return -1;
    }

    return 0;
}


void *handle_task(void *arg) {
    int client_socket = *(int *)arg;
    free(arg);

    pthread_mutex_lock(&threads_mutex);
    active_threads++;
    pthread_mutex_unlock(&threads_mutex);

    Trama request;
    if (safe_read(client_socket, &request, sizeof(Trama)) < 0 || request.type != 0x03) goto ko;

    // --------------------------- PARSING ROBUST COM HARLEY ---------------------------
    char file_name[256] = {0};
    char file_size_str[12] = {0};
    char received_checksum[MD5SUM_LENGTH] = {0};

    char *first_sep = strchr(request.data, '&');
    if (!first_sep) goto ko;

    size_t name_len = first_sep - request.data;
    if (name_len >= sizeof(file_name)) name_len = sizeof(file_name) - 1;
    memcpy(file_name, request.data, name_len);
    file_name[name_len] = '\0';

    char *second_sep = strchr(first_sep + 1, '&');
    if (!second_sep) goto ko;

    size_t size_len = second_sep - (first_sep + 1);
    if (size_len >= sizeof(file_size_str)) size_len = sizeof(file_size_str) - 1;
    memcpy(file_size_str, first_sep + 1, size_len);
    file_size_str[size_len] = '\0';

    uint32_t file_size = strtoul(file_size_str, NULL, 10);

    char *third_sep = strchr(second_sep + 1, '&');
    if (!third_sep) goto ko;

    size_t checksum_len = third_sep - (second_sep + 1);
    if (checksum_len >= MD5SUM_LENGTH) checksum_len = MD5SUM_LENGTH - 1;
    memcpy(received_checksum, second_sep + 1, checksum_len);
    received_checksum[checksum_len] = '\0';

    int factor = atoi(third_sep + 1);

    // -------------------------------------------------------------------------------

    Trama con_ok;
    inicialitzar_trama(&con_ok, 0x03, "");
    if (write(client_socket, &con_ok, sizeof(con_ok)) != sizeof(con_ok)) goto ko;

    TaskMetadata *metadata = buscar_tasca_per_nom(shared_mem, current_mem_size, file_name, &shared_mem_mutex);
    if (!metadata) {
        pthread_mutex_lock(&shared_mem_mutex);
        metadata = (TaskMetadata *)((char *)shared_mem + current_mem_size);
        strncpy(metadata->file_name, file_name, sizeof(metadata->file_name));
        metadata->total_size = file_size;
        metadata->processed_offset = 0;
        metadata->bytes_sent = 0;
        metadata->status = TASK_RECEIVING;
        strncpy(metadata->checksum, received_checksum, MD5SUM_LENGTH);
        pthread_mutex_unlock(&shared_mem_mutex);
        add_task_to_manager(&task_manager, client_socket, file_name, &config);
    }

    if (gestionar_recepcio_fitxer(client_socket, metadata, &shared_mem_mutex) < 0) goto ko;

    if (processar_text(file_name, factor) < 0) goto ko;

    int fd = open(file_name, O_RDONLY);
    if (fd < 0) goto ko;
    off_t size = lseek(fd, 0, SEEK_END);
    close(fd);
    if (size < 0) goto ko;

    pthread_mutex_lock(&shared_mem_mutex);
    metadata->total_size = (uint32_t)size;
    pthread_mutex_unlock(&shared_mem_mutex);

    if (enviar_fitxer_processat(client_socket, metadata, &shared_mem_mutex, config.base.socket_fd) < 0)
        marcar_tasca_com_fallida(metadata, &config);

    remove_task(&task_manager, client_socket, &task_manager.mutex);
    close(client_socket);

    pthread_mutex_lock(&threads_mutex);
    active_threads--;
    pthread_mutex_unlock(&threads_mutex);
    return NULL;

ko:
    Trama con_ko;
    inicialitzar_trama(&con_ko, 0x03, "CON_KO");
    write(client_socket, &con_ko, sizeof(con_ko));
    close(client_socket);
    return NULL;
}


void *worker_loop(void *arg) {
    (void)arg;
    while (1) {
        int client_socket = dequeue_connection(connection_queue, &queue_front, &queue_count,
            &queue_mutex, &queue_not_empty, &queue_not_full);
        int *sock_ptr = malloc(sizeof(int));
        *sock_ptr = client_socket;
        handle_task(sock_ptr);
    }
    return NULL;
}

void listener_enigma_principal(ConfigWorker *config) {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in addr = {.sin_family = AF_INET, .sin_port = htons(config->port_fleck)};
    addr.sin_addr.s_addr = inet_addr(config->ip_fleck);
    bind(server_fd, (struct sockaddr *)&addr, sizeof(addr));
    listen(server_fd, 10);
    while (1) {
        int client_socket = accept(server_fd, NULL, NULL);
        enqueue_connection(client_socket, connection_queue, &queue_rear, &queue_count,
            &queue_mutex, &queue_not_empty, &queue_not_full);
    }
}

void *gotham_listener_thread(void *arg) {
    ConfigWorker *cfg = (ConfigWorker *)arg;
    while (1) {
        Trama trama;
        if (read(cfg->base.socket_fd, &trama, sizeof(trama)) != sizeof(trama)) continue;
        if (trama.type == 0x08) {
            cfg->is_primary_worker = 1;
            listener_enigma_principal(cfg);
        }
    }
    return NULL;
}

void gestionar_signal_interrupt(int sig) {
    gestionar_signal_interrupt_worker(sig, &task_manager, &config, shared_mem, &shared_mem_fd,
                                       "/enigma_shared_mem", &task_manager.mutex, current_mem_size, &shared_mem_mutex);
}

int main(int argc, char *argv[]) {
    if (argc != 2) return 1;
    int config_fd = open(argv[1], O_RDONLY);
    if (config_fd < 0 || parse_worker_config(config_fd, &config) < 0) return 1;
    close(config_fd);

    assegurar_working_directory(config.work_dir, 0);
    char full_workdir[512];
    snprintf(full_workdir, sizeof(full_workdir), "Worker/Enigma/%s", config.work_dir);

    config.work_dir = strdup(full_workdir);

    if (chdir(config.work_dir) != 0) {
        return 1;
    }

    inicialitzar_memoria_compartida("/enigma_shared_mem", &shared_mem_fd, &shared_mem, current_mem_size, &shared_mem_mutex);
    inicialitzar_connexio_gotham(&config);

    signal(SIGINT, gestionar_signal_interrupt);
    pthread_t pool[THREAD_POOL_SIZE];
    for (int i = 0; i < THREAD_POOL_SIZE; i++) pthread_create(&pool[i], NULL, worker_loop, NULL);

    pthread_t gotham_thread;
    pthread_create(&gotham_thread, NULL, gotham_listener_thread, &config);
    configurar_heartbeats(enviar_heartbeat);

    pthread_join(gotham_thread, NULL);
    return 0;
}
