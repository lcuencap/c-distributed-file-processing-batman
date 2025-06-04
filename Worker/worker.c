#include "worker.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <errno.h>
#include <sys/wait.h>
#include <sys/mman.h>

/**
 * @brief Expande la memoria compartida para almacenar más tareas.
 * @param shared_mem Puntero a la memoria compartida.
 * @param current_mem_size Tamaño actual de la memoria compartida.
 * @param shared_mem_fd Descriptor del archivo de la memoria compartida.
 * @param shared_mem_mutex Mutex para proteger el acceso concurrente.
 * @return 0 si la expansión fue exitosa, -1 en caso de error.
 */
int expandir_memoria_compartida(void **shared_mem, size_t *current_mem_size, int shared_mem_fd, pthread_mutex_t *shared_mem_mutex) {
    pthread_mutex_lock(shared_mem_mutex);

    size_t nueva_mem_size = *current_mem_size + SHARED_MEM_SIZE;

    // Ampliar el tamaño del archivo de memoria compartida
    if (ftruncate(shared_mem_fd, nueva_mem_size) < 0) {
        write_message(STDERR_FILENO, "Error ampliant mida de la memòria compartida\n");
        pthread_mutex_unlock(shared_mem_mutex);
        return -1;
    }

    // Crear nueva región de memoria con el tamaño expandido
    void *new_mem = mmap(NULL, nueva_mem_size, PROT_READ | PROT_WRITE, MAP_SHARED, shared_mem_fd, 0);
    if (new_mem == MAP_FAILED) {
        write_message(STDERR_FILENO, "Error mapant nova memòria compartida\n");
        pthread_mutex_unlock(shared_mem_mutex);
        return -1;
    }

    // **Copiar datos de la memoria antigua a la nueva**
    memcpy(new_mem, *shared_mem, *current_mem_size);

    // **Liberar la memoria anterior**
    munmap(*shared_mem, *current_mem_size);

    // **Actualizar punteros**
    *shared_mem = new_mem;
    *current_mem_size = nueva_mem_size;

    pthread_mutex_unlock(shared_mem_mutex);
    return 0;
}


/**
 * @brief Configura un timeout per detectar desconnexions de Fleck.
 * @param socket_fd Descriptor del socket.
 * @param timeout_seconds Temps màxim d'espera en segons.
 */
void configurar_timeout(int socket_fd, int timeout_seconds) {
    struct timeval timeout = {timeout_seconds, 0};
    setsockopt(socket_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
}


/**
 * Gestiona la desconnexió del worker de Gotham.
 * @param socket_fd Descriptor del socket connectat a Gotham.
 * @param workerType Tipus de worker (e.g., "Media" o "Text").
 */
void gestionar_desconnexio_gotham(int socket_fd, const char *workerType) {
    if (socket_fd < 0 || !workerType) {
        write_message(STDOUT_FILENO, "Error: Paràmetres invàlids a gestionar_desconnexio_gotham\n");
        return;
    }

    Trama trama;
    inicialitzar_trama(&trama, 0x07, workerType);

    write(socket_fd, &trama, sizeof(trama));

    close(socket_fd);
}

/**
 * @brief Inicialitza la connexió amb Gotham.
 * @param config Configuració del Worker.
 * @return 0 si s'ha connectat correctament, -1 si hi ha error.
 */
int inicialitzar_connexio_gotham(ConfigWorker *config) {
    Trama trama;
    char data[245];
    snprintf(data, sizeof(data), "%s&%s&%d", 
         (config->base.type == HARLEY) ? "Media" : "Text",
         config->ip_fleck, config->port_fleck);
    inicialitzar_trama(&trama, 0x02, data);

    int socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_fd < 0) {
        write_message(STDERR_FILENO, "Error creant el socket\n");
        return -1;
    }

    struct sockaddr_in gotham_addr;
    gotham_addr.sin_family = AF_INET;
    gotham_addr.sin_port = htons(config->base.port);
    inet_pton(AF_INET, config->base.ip, &gotham_addr.sin_addr);

    if (connect(socket_fd, (struct sockaddr *)&gotham_addr, sizeof(gotham_addr)) < 0) {
        write_message(STDERR_FILENO, "Error connectant a Gotham\n");
        close(socket_fd);
        return -1;
    }

    if (write(socket_fd, &trama, sizeof(trama)) != sizeof(trama)) {
        write_message(STDERR_FILENO, "Error enviant la trama a Gotham\n");
        close(socket_fd);
        return -1;
    }

    Trama resposta;
    if (read(socket_fd, &resposta, sizeof(resposta)) != sizeof(resposta)) {
        write_message(STDOUT_FILENO, "Error rebent resposta de Gotham\n");
        close(socket_fd);
        return -1;
    }

    if (resposta.type == 0x02 && resposta.data_length == 0) {
        write_message(STDOUT_FILENO, "Connexió establerta amb Gotham\n");
    } else {
        write_message(STDOUT_FILENO, "Connexió rebutjada per Gotham\n");
        close(socket_fd);
        return -1;
    }

    config->base.socket_fd = socket_fd;  
    return 0;  
}

/**
 * Valida una trama per assegurar-se que el checksum és correcte.
 * @param trama Punter a la trama a validar.
 * @return 1 si és vàlida, 0 altrament.
 */
int validar_checksum(Trama *trama) {
    uint16_t checksum_calculat = generate_checksum((const uint8_t *)trama, TRAMA_SIZE - 2);
    return checksum_calculat == trama->checksum;
}

/**
 * @brief Gestiona la desconnexió de Fleck mentre el Worker processava la seva tasca.
 * @param file_name Nom del fitxer que s'estava processant.
 * @param client_socket Socket de Fleck.
 * @param gotham_socket Socket de Gotham.
 */
void gestionar_desconnexio_fleck(const char *file_name, int client_socket, int gotham_socket) {
    char log_msg[256];
    snprintf(log_msg, sizeof(log_msg), "Fleck desconnectat inesperadament mentre es processava: %s\n", file_name);
    write_message(STDERR_FILENO, log_msg);

    // Notificar a Gotham sobre la desconnexió
    Trama trama;
    inicialitzar_trama(&trama, 0x11, file_name);  // Tipus 0x11 indica tasca interrompuda
    write(gotham_socket, &trama, sizeof(trama));

    // Tancar el socket de Fleck
    close(client_socket);
}

/**
 * @brief Verifica si el Fleck segueix connectat.
 * @param client_socket Descriptor del socket de Fleck.
 * @return 1 si Fleck s'ha desconnectat, 0 si segueix connectat.
 */
int detectar_desconnexio_fleck(int client_socket) {
    char buffer[1];
    int result = recv(client_socket, buffer, sizeof(buffer), MSG_PEEK | MSG_DONTWAIT);
    if (result == 0 || (result < 0 && errno != EAGAIN && errno != EWOULDBLOCK)) {
        return 1;  // Fleck s'ha desconnectat
    }
    return 0;  // Fleck segueix connectat
}

/**
 * @brief Envia el fitxer processat al client.
 * @param client_socket Descriptor del socket del client.
 * @param metadata Punter a la metadada de la tasca.
 * @param shared_mem Punter a la memòria compartida.
 * @param mutex Mutex per a protegir l'accés a la memòria compartida.
 * @param gotham_socket_fd Descriptor del socket connectat a Gotham.
 */

 int enviar_fitxer_processat(int client_socket, TaskMetadata *metadata, pthread_mutex_t *mutex,
                             int gotham_socket_fd) {
    char file_path[512];

    if (strchr(metadata->file_name, '/')) {
        strncpy(file_path, metadata->file_name, sizeof(file_path));
    } else {
        snprintf(file_path, sizeof(file_path), "%s", metadata->file_name);
    }

    int file_fd = open(file_path, O_RDONLY);
    if (file_fd < 0) {
        pthread_mutex_lock(mutex);
        metadata->status = TASK_FAILED;
        pthread_mutex_unlock(mutex);

        Trama error_trama;
        snprintf(error_trama.data, sizeof(error_trama.data), "ERROR_OPEN_PROCESSED");
        error_trama.type = 0x06;
        write(gotham_socket_fd, &error_trama, sizeof(error_trama));
        return -1;
    }

    // enviar la trama 0x04 amb <FileSize>&<MD5SUM>
    char md5sum[MD5SUM_LENGTH];
    uint32_t distorted_file_size;
    if (calcular_md5(file_path, md5sum, &distorted_file_size) < 0) {
        close(file_fd);
        return -1;
    }


    Trama info_trama;
    char data_info[245];
    snprintf(data_info, sizeof(data_info), "%u&%s", metadata->total_size, md5sum);
    inicialitzar_trama(&info_trama, 0x04, data_info);
    ssize_t bytes_written = write(client_socket, &info_trama, sizeof(info_trama));

    if (bytes_written != sizeof(info_trama)) {
        close(file_fd);
        return -1;
    }



    // enviar les dades amb trames 0x05
    pthread_mutex_lock(mutex);
    if (metadata->bytes_sent >= metadata->total_size) {
        pthread_mutex_unlock(mutex);
        close(file_fd);
        return -1;
    }

    if (lseek(file_fd, metadata->bytes_sent, SEEK_SET) < 0) {
        metadata->status = TASK_FAILED;
        pthread_mutex_unlock(mutex);
        close(file_fd);
        return -1;
    }
    pthread_mutex_unlock(mutex);

    pthread_mutex_lock(mutex);
    metadata->status = TASK_SENDING;
    pthread_mutex_unlock(mutex);

    Trama data_trama;
    ssize_t bytes_read;

    while ((bytes_read = read(file_fd, data_trama.data, sizeof(data_trama.data))) > 0) {
    
        data_trama.type = 0x05;
        data_trama.data_length = bytes_read;
        data_trama.checksum = generate_checksum((const uint8_t *)&data_trama, sizeof(data_trama) - 2);
    
    
        ssize_t ret = write(client_socket, &data_trama, sizeof(data_trama));
        if (ret != sizeof(data_trama)) {
            close(file_fd);
            pthread_mutex_lock(mutex);
            metadata->status = TASK_FAILED;
            pthread_mutex_unlock(mutex);
    
            Trama error_trama;
            inicialitzar_trama(&error_trama, 0x06, "ERROR_SEND");
            write(gotham_socket_fd, &error_trama, sizeof(error_trama));
    
            return -1;
        } 
    
        pthread_mutex_lock(mutex);
        metadata->bytes_sent += bytes_read;
        pthread_mutex_unlock(mutex);
    }    

    close(file_fd);

    if (bytes_read < 0) {
        pthread_mutex_lock(mutex);
        metadata->status = TASK_FAILED;
        pthread_mutex_unlock(mutex);
        return -1;
    }

    unlink(file_path);

    // esperar resposta 0x06 de Fleck (CHECK_OK o CHECK_KO)
    Trama resposta_trama;
    ssize_t check_read = read(client_socket, &resposta_trama, sizeof(resposta_trama));
    if (check_read != sizeof(resposta_trama)) {
        return -1;
    }


    return 0;
}

void parse_trama_data(const char *data, uint32_t *file_size, char *checksum) {
    if (!data || !file_size || !checksum) {
        write_message(STDERR_FILENO, "Error: Paràmetres invàlids a parse_trama_data\n");
        return;
    }

    const char *separator = strchr(data, '&');
    if (!separator) {
        write_message(STDERR_FILENO, "Error: Format de dades de trama invàlid\n");
        return;
    }

    char size_buffer[16] = {0};
    strncpy(size_buffer, data, separator - data);
    *file_size = (uint32_t)strtoul(size_buffer, NULL, 10);

    strncpy(checksum, separator + 1, 32);
    checksum[32] = '\0';
    return;
}
/**
 * @brief Gestiona la recepció d'un fitxer, el desa en local i en memòria compartida, augmentant la memòria si cal.
 * @param client_socket Descriptor del socket del client.
 * @param metadata Punter a la metadada de la tasca.
 * @param shared_mem Punter a la memòria compartida.
 * @param shared_mem_fd Descriptor del fitxer de la memòria compartida.
 * @param current_mem_size Punter a la mida actual de la memòria compartida.
 * @param mutex Mutex per a protegir l'accés a la memòria compartida.
 * @param gotham_socket_fd Descriptor del socket connectat a Gotham.
 * @param config_worker Configuració del worker.
 * @return Retorna 0 si és correcte, -1 en cas d'error.
 */
int gestionar_recepcio_fitxer(int client_socket, TaskMetadata *metadata, pthread_mutex_t *mutex) {
    Trama trama;
    ssize_t bytes_read;

    pthread_mutex_lock(mutex);
    uint32_t processed_offset = metadata->processed_offset;
    metadata->status = TASK_RECEIVING;
    pthread_mutex_unlock(mutex);


    char file_path[512];
    snprintf(file_path, sizeof(file_path), "%s", metadata->file_name);

    int file_fd = open(file_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (file_fd < 0) {

        pthread_mutex_lock(mutex);
        metadata->status = TASK_FAILED;
        pthread_mutex_unlock(mutex);

        enviar_resposta_connexio_inicial(client_socket, 0);
        return -1;
    }

    if (lseek(file_fd, processed_offset, SEEK_SET) < 0) {
        close(file_fd);
        enviar_resposta_connexio_inicial(client_socket, 0);
        return -1;
    }

    while ((bytes_read = read(client_socket, &trama, sizeof(trama))) == sizeof(trama)) {
        if (!validar_trama(&trama)) {
            close(file_fd);
            enviar_resposta_md5(client_socket, 0);
            return -1;
        }
        
        if (trama.type != 0x05) {
            close(file_fd);
            enviar_resposta_md5(client_socket, 0);
            return -1;
        }

        if (trama.data_length > sizeof(trama.data)) {
            close(file_fd);
            enviar_resposta_md5(client_socket, 0);
            return -1;
        }

        if (write(file_fd, trama.data, trama.data_length) != trama.data_length) {
            close(file_fd);
            enviar_resposta_md5(client_socket, 0);
            return -1;
        }

        pthread_mutex_lock(mutex);
        metadata->processed_offset += trama.data_length;
        if (metadata->processed_offset >= metadata->total_size) {
            pthread_mutex_unlock(mutex);
            break;
        }
        pthread_mutex_unlock(mutex);
    }

    close(file_fd);

    if (bytes_read < 0) {
        pthread_mutex_lock(mutex);
        metadata->status = TASK_FAILED;
        pthread_mutex_unlock(mutex);
        enviar_resposta_md5(client_socket, 0);
        return -1;
    }

    pthread_mutex_lock(mutex);
    metadata->status = TASK_RECEIVED;
    pthread_mutex_unlock(mutex);


    char md5_str[33];
    uint32_t file_size = 0;
    if (calcular_md5(file_path, md5_str, &file_size) < 0) {
        pthread_mutex_lock(mutex);
        metadata->status = TASK_FAILED;
        pthread_mutex_unlock(mutex);
        enviar_resposta_md5(client_socket, 0);
        return -1;
    }

    if (strcmp(md5_str, metadata->checksum) != 0) {
        pthread_mutex_lock(mutex);
        metadata->status = TASK_FAILED;
        pthread_mutex_unlock(mutex);
        enviar_resposta_md5(client_socket, 0);
        return -1;
    }

    enviar_resposta_md5(client_socket, 1);
    return 0;
}

/**
 * @brief Configura els heartbeats per a Harley.
 */
void configurar_heartbeats(void (*handler)(int)) {
    signal(SIGALRM, handler);
    alarm(10);
}

void enqueue_connection(int client_socket, int* connection_queue, int* queue_rear, int* queue_count,
    pthread_mutex_t* queue_mutex, pthread_cond_t* queue_not_empty, pthread_cond_t* queue_not_full) {
    pthread_mutex_lock(queue_mutex);
    while (*queue_count == MAX_QUEUE_SIZE)
    pthread_cond_wait(queue_not_full, queue_mutex);

    connection_queue[*queue_rear] = client_socket;
    *queue_rear = (*queue_rear + 1) % MAX_QUEUE_SIZE;
    (*queue_count)++;

    pthread_cond_signal(queue_not_empty);
    pthread_mutex_unlock(queue_mutex);
}


int dequeue_connection(int* connection_queue, int* queue_front, int* queue_count,
    pthread_mutex_t* queue_mutex, pthread_cond_t* queue_not_empty, pthread_cond_t* queue_not_full) {
    pthread_mutex_lock(queue_mutex);
    while (*queue_count == 0)
    pthread_cond_wait(queue_not_empty, queue_mutex);

    int client_socket = connection_queue[*queue_front];
    *queue_front = (*queue_front + 1) % MAX_QUEUE_SIZE;
    (*queue_count)--;

    pthread_cond_signal(queue_not_full);
    pthread_mutex_unlock(queue_mutex);
    return client_socket;
}



void add_task_to_manager(TaskManager *manager, int client_socket, const char *file_name, ConfigWorker *config) {
    Task new_task = {
        .task_id = client_socket, 
        .client_socket = client_socket,
        .worker_socket = config->base.socket_fd,
        .status = TASK_PENDING,
    };
    strncpy(new_task.file_name, file_name, sizeof(new_task.file_name));
    addTask(manager, new_task);
}

void marcar_tasca_com_fallida(TaskMetadata *metadata, ConfigWorker *config) {
    metadata->status = TASK_FAILED;

    Trama trama_error;
    inicialitzar_trama(&trama_error, 0x11, metadata->file_name);
    if (write(config->base.socket_fd, &trama_error, sizeof(trama_error)) != sizeof(trama_error)){
    }
}


void inicialitzar_memoria_compartida(const char *shm_name, int *shared_mem_fd, void **shared_mem, 
                                     size_t mem_size, pthread_mutex_t *mutex) {
    *shared_mem_fd = shm_open(shm_name, O_CREAT | O_RDWR, 0644);
    if (*shared_mem_fd < 0) {
        exit(1);
    }
    
    if (ftruncate(*shared_mem_fd, mem_size) < 0) {
        shm_unlink(shm_name);
        exit(1);
    }

    *shared_mem = mmap(NULL, mem_size, PROT_READ | PROT_WRITE, MAP_SHARED, *shared_mem_fd, 0);
    if (*shared_mem == MAP_FAILED) {
        shm_unlink(shm_name);
        exit(1);
    }

    pthread_mutex_lock(mutex);
    int *worker_count = (int *)(*shared_mem);
    if (*worker_count == 0) {
        memset(*shared_mem, 0, mem_size);
    }
    (*worker_count)++;
    pthread_mutex_unlock(mutex);
}

TaskMetadata *buscar_tasca_per_nom(void *shared_mem, size_t current_mem_size, const char *file_name,
                                   pthread_mutex_t *mutex) {
    pthread_mutex_lock(mutex);

    char *shared_mem_ptr = shared_mem;
    TaskMetadata *found_metadata = NULL;

    while (shared_mem_ptr < (char *)shared_mem + current_mem_size) {
        TaskMetadata *metadata = (TaskMetadata *)shared_mem_ptr;
        if (strncmp(metadata->file_name, file_name, sizeof(metadata->file_name)) == 0) {
            found_metadata = metadata;
            break;
        }
        shared_mem_ptr += sizeof(TaskMetadata) + metadata->total_size;
    }

    pthread_mutex_unlock(mutex);
    return found_metadata;
}


/**
 * @brief Comprova si el directori existeix dins de Worker/Harley o Worker/Enigma i mostra un missatge si no existeix.
 * @param work_dir Nom del directori de treball dins de Worker/Harley o Worker/Enigma.
 * @param harley 1 si és un worker de tipus Harley, 0 si és Enigma.
 */
void assegurar_working_directory(char *work_dir, int harley) {
    struct stat st;

    // Construir el path complet 
    size_t path_len = strlen("Worker/") + strlen(harley ? "Harley" : "Enigma") + strlen(work_dir) + 1;
    char *full_path = malloc(path_len);
    if (!full_path) {
        write_message(STDERR_FILENO, "Error: No es pot reservar memòria per al path\n");
        return;
    }
    snprintf(full_path, path_len, "Worker/%s%s", harley ? "Harley" : "Enigma", work_dir);

    // Comprovar si el directori existeix
    if (stat(full_path, &st) == -1) {
        if (errno == ENOENT) { // No existeix
            size_t msg_len = strlen("[WARN] Directori inexistent: ") + strlen(full_path) + 2;
            char *msg = malloc(msg_len);
            if (!msg) {
                write_message(STDERR_FILENO, "Error: No es pot reservar memòria per al missatge\n");
            } else {
                snprintf(msg, msg_len, "[WARN] Directori inexistent: %s\n", full_path);
                write_message(STDOUT_FILENO, msg);
                free(msg);
            }
        } 
    } else {
        size_t msg_len = strlen("[INFO] Directori trobat: ") + strlen(full_path) + 2;
        char *msg = malloc(msg_len);
        if (!msg) {
            write_message(STDERR_FILENO, "Error: No es pot reservar memòria per al missatge\n");
        } else {
            snprintf(msg, msg_len, "[INFO] Directori trobat: %s\n", full_path);
            write_message(STDOUT_FILENO, msg);
            free(msg);
        }
    }

    free(full_path);
}

void mark_incomplete_tasks(TaskManager *manager, ConfigWorker *config, void *shared_mem, size_t shared_mem_size, pthread_mutex_t *shared_mem_mutex) {
    pthread_mutex_lock(&manager->mutex);  

    Task *current = manager->head;
    while (current) {
        if (current->status == TASK_IN_PROGRESS) {
            TaskMetadata *metadata = buscar_tasca_per_nom(shared_mem, shared_mem_size, current->file_name, shared_mem_mutex);
            if (metadata) {
                pthread_mutex_lock(shared_mem_mutex);
                metadata->status = TASK_INTERRUPTED;
                pthread_mutex_unlock(shared_mem_mutex);  
            }
            // Notificar a Gotham
            Trama trama_incompleta;
            inicialitzar_trama(&trama_incompleta, 0x11, current->file_name);
            write(config->base.socket_fd, &trama_incompleta, sizeof(trama_incompleta));
        }
        current = current->next;
    }

    pthread_mutex_unlock(&manager->mutex);  
}


Task *get_next_task(TaskManager *manager) {
    pthread_mutex_lock(&manager->mutex);

    Task *current = manager->head;
    while (current) {
        if (current->status == TASK_PENDING) {
            current->status = TASK_IN_PROGRESS;
            pthread_mutex_unlock(&manager->mutex);
            return current;
        }
        current = current->next;
    }

    pthread_mutex_unlock(&manager->mutex);
    return NULL;
}

void remove_task(TaskManager *manager, int client_socket, pthread_mutex_t *mutex) {
    pthread_mutex_lock(mutex);

    Task *current = manager->head, *prev = NULL;
    while (current) {
        if (current->client_socket == client_socket) {
            if (prev) {
                prev->next = current->next;
            } else {
                manager->head = current->next;
            }
            free(current);
            break;
        }
        prev = current;
        current = current->next;
    }

    pthread_mutex_unlock(mutex);
}

int reconnectar_gotham(ConfigWorker *config) {
    close(config->base.socket_fd);
    sleep(2);

    if (inicialitzar_connexio_gotham(config) == 0) {
        return 0;
    }
    return -1;
}

void gestionar_signal_interrupt_worker(int sig, TaskManager *task_manager, ConfigWorker *config, 
                                       void *shared_mem, int *shared_mem_fd, const char *shm_name, 
                                       pthread_mutex_t *task_mutex, size_t shared_mem_size, pthread_mutex_t *shared_mem_mutex) {
    (void)sig;

    pthread_mutex_lock(task_mutex);
    mark_incomplete_tasks(task_manager, config, shared_mem, shared_mem_size, shared_mem_mutex);
    pthread_mutex_unlock(task_mutex);

    gestionar_desconnexio_gotham(config->base.socket_fd, "Worker");

    pthread_mutex_lock(shared_mem_mutex);
    int *worker_count = (int *)shared_mem;
    (*worker_count)--;

    if (*worker_count == 0) {
        shm_unlink(shm_name);
        close(*shared_mem_fd);
    }
    pthread_mutex_unlock(shared_mem_mutex);

    munmap(shared_mem, SHARED_MEM_SIZE);
    close(config->base.socket_fd);
    free(config->work_dir);

    write_message(STDOUT_FILENO, "Worker finalitzat correctament\n");

    _exit(0);
}
