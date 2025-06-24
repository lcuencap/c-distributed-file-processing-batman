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
int expandir_memoria_compartida(void **shared_mem, size_t *current_mem_size, int shared_mem_fd) {
    size_t nueva_mem_size = *current_mem_size + SHARED_MEM_SIZE;

    // Ampliar el tamaño del archivo de memoria compartida
    if (ftruncate(shared_mem_fd, nueva_mem_size) < 0) {
        write_message(STDERR_FILENO, "Error ampliant mida de la memòria compartida\n");
        return -1;
    }

    // Crear nueva región de memoria con el tamaño expandido
    void *new_mem = mmap(NULL, nueva_mem_size, PROT_READ | PROT_WRITE, MAP_SHARED, shared_mem_fd, 0);
    if (new_mem == MAP_FAILED) {
        write_message(STDERR_FILENO, "Error mapant nova memòria compartida\n");
        return -1;
    }

    // **Copiar datos de la memoria antigua a la nueva**
    memcpy(new_mem, *shared_mem, *current_mem_size);

    // **Liberar la memoria anterior**
    munmap(*shared_mem, *current_mem_size);

    // **Actualizar punteros**
    *shared_mem = new_mem;
    *current_mem_size = nueva_mem_size;
    

    return 0;
}

/**
 * @brief Neteja la connexió i allibera els recursos en cas d'error.
 * @param client_socket Socket del client.
 * @param metadata Metadades de la tasca.
 * @param shared_mem_mutex Mutex per la memòria compartida.
 * @param active_threads Punter a la variable que manté el recompte de fils actius.
 * @param threads_mutex Mutex per protegir l'accés a active_threads.
 */
void netejar_connexio_per_error(int client_socket, TaskMetadata *metadata, pthread_mutex_t *shared_mem_mutex, int *active_threads, pthread_mutex_t *threads_mutex) {
    if (client_socket >= 0) {
        close(client_socket);
    }

    if (metadata != NULL) {
        free_task_metadata(metadata, shared_mem_mutex);
    }

    pthread_mutex_lock(threads_mutex);
    active_threads--;
    pthread_mutex_unlock(threads_mutex);
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
        write_message(STDERR_FILENO, "Error connecting to Gotham.\n");
        return -1;
    }

    struct sockaddr_in gotham_addr;
    gotham_addr.sin_family = AF_INET;
    gotham_addr.sin_port = htons(config->base.port);
    inet_pton(AF_INET, config->base.ip, &gotham_addr.sin_addr);

    if (connect(socket_fd, (struct sockaddr *)&gotham_addr, sizeof(gotham_addr)) < 0) {
        write_message(STDERR_FILENO, "Error connecting to Gotham\n");
        close(socket_fd);
        return -1;
    }

    if (write(socket_fd, &trama, sizeof(trama)) != sizeof(trama)) {
        write_message(STDERR_FILENO, "Error sending packet to Gotham\n");
        close(socket_fd);
        return -1;
    }

    Trama resposta;
    if (read(socket_fd, &resposta, sizeof(resposta)) != sizeof(resposta)) {
        write_message(STDOUT_FILENO, "Could not read Gotham server answer to connection petition.\n");
        close(socket_fd);
        return -1;
    }

    if (resposta.type == 0x02 && resposta.data_length == 0) {
        write_message(STDOUT_FILENO, "Connected to Gotham server.\n");
    } else {
        write_message(STDOUT_FILENO, "Gotham server refused the connection petition.\n");
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
 */
void gestionar_desconnexio_fleck(const char *file_name, int client_socket, void *shared_mem, size_t shared_mem_size, pthread_mutex_t *mutex) {
    char log_msg[256];
    snprintf(log_msg, sizeof(log_msg), "Fleck desconnectat inesperadament mentre es processava: %s\n", file_name);
    write_message(STDERR_FILENO, log_msg);
    TaskMetadata *metadata = buscar_tasca_per_nom(shared_mem, shared_mem_size, file_name, mutex);
    if (metadata) {
        pthread_mutex_lock(mutex);
        metadata->status = TASK_INTERRUPTED;
        pthread_mutex_unlock(mutex);
    }
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
 * @param output_data file path de l'output file
 */

 int enviar_fitxer_processat(int client_socket, TaskMetadata *metadata, pthread_mutex_t *mutex,
                             int gotham_socket_fd, char *output_data) {

    if (!output_data) {
        pthread_mutex_lock(mutex);
        metadata->status = TASK_FAILED;
        pthread_mutex_unlock(mutex);
        return -1;
    }

    int file_fd = open(output_data, O_RDONLY);
    if (file_fd < 0) {
        pthread_mutex_lock(mutex);
        metadata->status = TASK_FAILED;
        pthread_mutex_unlock(mutex);
        return -1;
    }

    // enviar la trama 0x04 amb <FileSize>&<MD5SUM>
    char md5sum[MD5SUM_LENGTH];
    uint32_t distorted_file_size;
    if (calcular_md5(output_data, md5sum, &distorted_file_size) < 0) {
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


    // esperar resposta 0x06 de Fleck (CHECK_OK o CHECK_KO)
    Trama resposta_trama;
    ssize_t check_read = read(client_socket, &resposta_trama, sizeof(resposta_trama));
    if (check_read != sizeof(resposta_trama)) {
        return -1;
    }

    if (strncmp(resposta_trama.data, "CHECK_OK", resposta_trama.data_length) == 0) {
        unlink(output_data); 
    }


    return 0;
}

/**
 * @brief Analitza les dades de la trama rebuda per extreure la mida del fitxer i el checksum.
 * @param data Dades de la trama rebuda.
 * @param file_size Punter a la variable on es desa la mida del fitxer.
 * @param checksum Punter al buffer on es desa el checksum.
 */
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
 * @brief Finalitza una tasca per desconnexió del Fleck.
 * @param metadata Punter a la metadada de la tasca.
 * @param file_name Nom del fitxer associat a la tasca.
 * @param socket Socket del Fleck.
 * @param shared_mem Punter a la memòria compartida.
 * @param current_mem_size Mida actual de la memòria compartida.
 * @param shared_mem_mutex Mutex per a protegir l'accés a la memòria compartida.
 */
void finalitzar_tasca_per_desconnexio(TaskMetadata *metadata, const char *file_name, int socket,
                                    void *shared_mem, size_t current_mem_size, pthread_mutex_t *shared_mem_mutex){
    gestionar_desconnexio_fleck(file_name, socket, shared_mem, current_mem_size, shared_mem_mutex);
    free_task_metadata(metadata, shared_mem_mutex);
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

    char *file_path = malloc(strlen(metadata->file_name) + 1);
    if (!file_path) {
        write_message(STDERR_FILENO, "[ERROR] Memory allocation failed for file_path\n");
        pthread_mutex_lock(mutex);
        metadata->status = TASK_FAILED;
        pthread_mutex_unlock(mutex);
        enviar_resposta_connexio_inicial(client_socket, 0);
        return -1;
    }

    strcpy(file_path, metadata->file_name);

    char *msg = malloc(strlen("[INFO] Distortion request received for: ") + strlen(file_path) + 2);
    if (msg) {
        snprintf(msg, strlen("[INFO] Distortion request received for: ") + strlen(file_path) + 2,
                 "[INFO] Distortion request received for: %s\n", file_path);
        write_message(STDOUT_FILENO, msg);
        free(msg);
    }

    if (metadata->processed_offset >= metadata->total_size) {
        return 0;  // Si ja s'ha processat completament, no cal rebre fitxer
    }

    int file_fd = open(file_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);

    if (file_fd < 0) {
        write_message(STDERR_FILENO, "[ERROR] Opening file for writing\n");
        pthread_mutex_lock(mutex);
        metadata->status = TASK_FAILED;
        pthread_mutex_unlock(mutex);

        enviar_resposta_connexio_inicial(client_socket, 0);
        return -1;
    }

    if (lseek(file_fd, processed_offset, SEEK_SET) < 0) {
        write_message(STDOUT_FILENO, "[ERROR] Could not position file pointer correctly for writing.\n");
        close(file_fd);
        enviar_resposta_connexio_inicial(client_socket, 0);
        return -1;
    }

    pthread_mutex_lock(mutex);
    metadata->status = TASK_RECEIVING;
    pthread_mutex_unlock(mutex);  

    if (metadata->processed_offset >= metadata->total_size) {
        return 0;  // Si ja s'ha processat completament, no cal rebre fitxer
    }

    while ((bytes_read = read(client_socket, &trama, sizeof(trama))) == sizeof(trama)) {
        if (!validar_trama(&trama)) {
            write_message(STDERR_FILENO, "[ERROR] Invalid frame received\n");
            close(file_fd);
            enviar_resposta_md5(client_socket, 0);
            return -1;
        }
        
        if (trama.type != 0x05) {
            write_message(STDERR_FILENO, "[ERROR] Incorrect frame type received\n");
            close(file_fd);
            enviar_resposta_md5(client_socket, 0);
            return -1;
        }

        if (trama.data_length > sizeof(trama.data)) {
            write_message(STDERR_FILENO, "[ERROR] Frame data length too large\n");
            close(file_fd);
            enviar_resposta_md5(client_socket, 0);
            return -1;
        }

        if (write(file_fd, trama.data, trama.data_length) != trama.data_length) {
            write_message(STDERR_FILENO, "[ERROR] Writing data to file\n");
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


    
    fsync(file_fd);  
    close(file_fd);  
    


    if (bytes_read < 0) {
        write_message(STDERR_FILENO, "[ERROR] Error reading data from socket\n");
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
        write_message(STDERR_FILENO, "[ERROR] Error calculating MD5 of received file\n");
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
 * @brief Afegeix una nova tasca a la memòria compartida. Amplia la memòria si cal.
 */
TaskMetadata *alloc_task_metadata(const char *file_name, uint32_t file_size, const char *checksum,
                                   void **shared_mem_ptr, size_t *mem_size,
                                   int shared_mem_fd, pthread_mutex_t *mutex) {
    pthread_mutex_lock(mutex);
    TaskMetadata *iter = (TaskMetadata *)((char *)(*shared_mem_ptr) + sizeof(int));
    size_t num_entries = (*mem_size - sizeof(int)) / sizeof(TaskMetadata);
    TaskMetadata *found = NULL;

    for (size_t i = 0; i < num_entries; i++) {
        if (iter[i].status == TASK_EMPTY || iter[i].file_name[0] == '\0') {
            found = &iter[i];
            break;
        }
    }

    if (!found) {
        write_message(STDOUT_FILENO, "[ERROR] No empty slot found, expanding shared memory.\n");
        if (expandir_memoria_compartida(shared_mem_ptr, mem_size, shared_mem_fd) < 0) {
            pthread_mutex_unlock(mutex);
            return NULL;
        }
        iter = (TaskMetadata *)(*shared_mem_ptr);
        found = &iter[*mem_size / sizeof(TaskMetadata) - 1];  // Última posició
    }
    



    memset(found, 0, sizeof(TaskMetadata));
    snprintf(found->file_name, sizeof(found->file_name), "%s", file_name);
    found->total_size = file_size;
    found->processed_offset = 0;
    found->bytes_sent = 0;
    found->status = TASK_RECEIVING;
    strncpy(found->checksum, checksum, MD5SUM_LENGTH);

    pthread_mutex_unlock(mutex);
    return found;
}

/**
 * @brief Allibera l'espai ocupat per una tasca (la marca com a buida).
 */
void free_task_metadata(TaskMetadata *metadata, pthread_mutex_t *mutex) {
    pthread_mutex_lock(mutex);
    memset(metadata, 0, sizeof(TaskMetadata));
    metadata->status = TASK_EMPTY;
    pthread_mutex_unlock(mutex);
}


void marcar_tasca_com_fallida(TaskMetadata *metadata) {
    metadata->status = TASK_FAILED;
}

/**
 * @brief Inicialitza la memòria compartida per a les tasques.
 * @param shm_name Nom del segment de memòria compartida.
 * @param shared_mem_fd Descriptor del fitxer de la memòria compartida.
 * @param shared_mem Punter a la memòria compartida.
 * @param mem_size Mida total de la memòria compartida.
 * @param mutex Mutex per a protegir l'accés a la memòria compartida.
 */
int inicialitzar_memoria_compartida(const char *shm_name, int *shared_mem_fd, void **shared_mem, 
                                     size_t mem_size, pthread_mutex_t *mutex) {
    int primer_worker = 0;
    *shared_mem_fd = shm_open(shm_name, O_CREAT | O_RDWR, 0644);
    if (*shared_mem_fd < 0) {
        perror("shm_open");
        exit(1);
    }

    struct stat shm_stat;
    if (fstat(*shared_mem_fd, &shm_stat) == 0 && shm_stat.st_size == 0) {
        primer_worker = 1;
        if (ftruncate(*shared_mem_fd, mem_size) < 0) {
            perror("ftruncate");
            shm_unlink(shm_name);
            exit(1);
        }
    }

    *shared_mem = mmap(NULL, mem_size, PROT_READ | PROT_WRITE, MAP_SHARED, *shared_mem_fd, 0);
    if (*shared_mem == MAP_FAILED) {
        perror("mmap");
        shm_unlink(shm_name);
        exit(1);
    }

    if (primer_worker) {
        memset(*shared_mem, 0, mem_size);
    }

    pthread_mutex_lock(mutex);
    int *worker_count = (int *)(*shared_mem);
    if (primer_worker) *worker_count = 0;
    (*worker_count)++;  
    pthread_mutex_unlock(mutex);


    return primer_worker;  

}


/*
 * @brief Volca el contingut de la memòria compartida a la sortida estàndard.
 * @param shared_mem Punter a la memòria compartida.
 * @param mem_size Mida total de la memòria compartida.
void volcar_memoria_tasques(void *shared_mem, size_t mem_size) {
    write_message(STDOUT_FILENO, "\n================== DUMP DE MEMÒRIA COMPARTIDA ==================\n");
    
    int *worker_count = (int *)shared_mem;
    char count_msg[128];
    snprintf(count_msg, sizeof(count_msg), "Workers actius: %d\n", *worker_count);
    write_message(STDOUT_FILENO, count_msg);
    char *ptr = (char *)shared_mem + sizeof(int);

    size_t offset = 0;
    int index = 0;

    while (offset + sizeof(TaskMetadata) <= mem_size) {
        TaskMetadata *task = (TaskMetadata *)(ptr + offset);

        // Considerem que una tasca vàlida té un nom de fitxer no buit
        if (strlen(task->file_name) > 0) {
            char msg[512];
            snprintf(msg, sizeof(msg),
                "[%d] Nom: %s | Estat: %d | Total: %u | Offset: %u | Enviats: %u | Checksum: %s\n",
                index, task->file_name, task->status, task->total_size,
                task->processed_offset, task->bytes_sent, task->checksum);
            write_message(STDOUT_FILENO, msg);
            index++;
        }

        offset += sizeof(TaskMetadata);
    }

    if (index == 0) {
        write_message(STDOUT_FILENO, "Cap tasca trobada a la memòria compartida.\n");
    }

    write_message(STDOUT_FILENO, "==============================================================\n\n");
}
*/


/**
 * Elimina totes les tasques de la memòria compartida (reseteig complet).
 * @param shared_mem Punter a la memòria compartida.
 * @param mem_size Mida total de la memòria compartida.
 */
void netejar_memoria_tasques(void *shared_mem, size_t mem_size, pthread_mutex_t *shared_mem_mutex) {
    pthread_mutex_lock(shared_mem_mutex);
    *((int *)shared_mem) = 0;  
    char *ptr = (char *)shared_mem + sizeof(int);

    size_t offset = 0;
    int comptador = 0;

    while (offset + sizeof(TaskMetadata) <= mem_size) {
        TaskMetadata *task = (TaskMetadata *)(ptr + offset);

        if (strlen(task->file_name) > 0) {
            memset(task, 0, sizeof(TaskMetadata));
            comptador++;
        }

        offset += sizeof(TaskMetadata);
    }

    pthread_mutex_unlock(shared_mem_mutex);
}

/**
 * @brief Busca una tasca per nom a la memòria compartida.
 * @param shared_mem Punter a la memòria compartida.
 * @param current_mem_size Mida actual de la memòria compartida.
 * @param file_name Nom del fitxer a buscar.
 * @param mutex Mutex per a protegir l'accés a la memòria compartida.
 * @return Punter a la metadada de la tasca trobada, o NULL si no s'ha trobat.
 */
TaskMetadata *buscar_tasca_per_nom(void *shared_mem, size_t current_mem_size, const char *file_name,
                                   pthread_mutex_t *mutex) {
    pthread_mutex_lock(mutex);

    char *shared_mem_ptr = (char *)shared_mem + sizeof(int);

    TaskMetadata *found_metadata = NULL;

    while (shared_mem_ptr < ((char *)shared_mem + current_mem_size)){
        TaskMetadata *metadata = (TaskMetadata *)shared_mem_ptr;
        if (strncmp(metadata->file_name, file_name, sizeof(metadata->file_name)) == 0) {
            found_metadata = metadata;
            break;
        }
        shared_mem_ptr += sizeof(TaskMetadata);
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
        write_message(STDERR_FILENO, "[ERROR] Could not validate worker directory.\n");
        return;
    }
    snprintf(full_path, path_len, "Worker/%s%s", harley ? "Harley" : "Enigma", work_dir);

    // Comprovar si el directori existeix
    if (stat(full_path, &st) == -1) {
        if (errno == ENOENT) { // No existeix
            size_t msg_len = strlen("[WARN] Non-existing directory: ") + strlen(full_path) + 2;
            char *msg = malloc(msg_len);
            if (msg) {
                snprintf(msg, msg_len, "[WARN] Worker directory not found: %s\n", full_path);
                write_message(STDOUT_FILENO, msg);
                free(msg);            } 
        } 
    } 

    free(full_path);
}

int reconnectar_gotham(ConfigWorker *config, int keep_running) {
    if (!keep_running) return -1;
    close(config->base.socket_fd);
    sleep(2);

    if (inicialitzar_connexio_gotham(config) == 0) {
        return 0;
    }
    return -1;
}