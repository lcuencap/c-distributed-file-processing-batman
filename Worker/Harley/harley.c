#include "../worker.h"
#include "../../Utils/so_compression.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/mman.h>
#include <pthread.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <libgen.h>
#include <pthread.h>


// Variables globals
static ConfigWorker config;
static int shared_mem_fd = -1;
static void *shared_mem = NULL;
static size_t current_mem_size = SHARED_MEM_SIZE;
pthread_mutex_t shared_mem_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_rwlock_t shared_mem_rwlock = PTHREAD_RWLOCK_INITIALIZER;  
int active_threads = 0;
pthread_mutex_t threads_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_t gotham_thread = 0;
pthread_t harley_thread = 0;

static int server_fd = -1; 

volatile sig_atomic_t keep_running = 1;



/**
 * Copia un fitxer origen a destí
 */
int copiar_fitxer(const char *origen, const char *desti) {
    int in_fd = open(origen, O_RDONLY);
    if (in_fd < 0) return -1;

    int out_fd = open(desti, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (out_fd < 0) {
        close(in_fd);
        return -1;
    }

    char buffer[4096];
    ssize_t bytes;
    while ((bytes = read(in_fd, buffer, sizeof(buffer))) > 0) {
        if (write(out_fd, buffer, bytes) != bytes) {
            close(in_fd);
            close(out_fd);
            return -1;
        }
    }

    close(in_fd);
    close(out_fd);
    return (bytes < 0) ? -1 : 0;
}


/**
 * @brief Processar la compressió de media.
 */
int processar_media(const char *file_name, int factor) {
    char tmp_file[300];
    snprintf(tmp_file, sizeof(tmp_file), "tmp_%s", basename((char *)file_name));

    if (copiar_fitxer(file_name, tmp_file) < 0) {
        write_message(STDERR_FILENO, "[ERROR] Could not duplicate file for safe distorsion.\n");
        return -1;
    }

  
    int result;

    if (strstr(file_name, ".wav")) {
        result = SO_compressAudio(tmp_file, factor);
    } else if (strstr(file_name, ".jpg") || strstr(file_name, ".png")) {
        result = SO_compressImage(tmp_file, factor);
    } else if (strstr(file_name, ".mp3")) {
        write_message(STDERR_FILENO, "[ERROR] .mp3 not supported. Only .wav.\n");
        result = ERROR_NOT_WAV;
    } else {
        write_message(STDERR_FILENO, "[ERROR] Invalid format.\n");
        result =  ERROR_UNSUPPORTED_FORMAT;
    }


    if (result != NO_ERROR) {
        write_message(STDERR_FILENO, "[ERROR] Could not distort media.\n");
        unlink(tmp_file);
        return result;
    }

    // Substituir l’original pel fitxer comprimit
    if (rename(tmp_file, file_name) < 0) {
        write_message(STDERR_FILENO, "[ERROR] Something happened trying to save the distorted file.\n");
        unlink(tmp_file);  // Evitar que quedi penjat
        return -1;
    }


    return 0;  // Retorna 0 si tot ha anat bé
}


/**
 * @brief Envia un heartbeat a Gotham per indicar que el worker segueix actiu.
 */
void enviar_heartbeat() {
    if (!keep_running) return;

    Trama heartbeat_trama;
    inicialitzar_trama(&heartbeat_trama, 0x12, "");

    if (write(config.base.socket_fd, &heartbeat_trama, sizeof(heartbeat_trama)) != sizeof(heartbeat_trama)) {
        keep_running = 0; 
        return;            
    }
}

/**
 * Aquest thread s'encarrega de gestionar les tasques rebudes des de Fleck.
 * Cada tasca es processa de manera independent i es gestiona la recepció del fitxer,   
 * processament i enviament de resposta.
 * @param arg Punter a l'argument que conté el socket del client.
 * @return Retorna NULL quan la tasca ha finalitzat.
 */
void *handle_task(void *arg) {
    int client_socket = *(int *)arg;
    free(arg);

    pthread_mutex_lock(&threads_mutex);
    active_threads++;
    pthread_mutex_unlock(&threads_mutex);


    Trama request_trama;
    if (safe_read(client_socket, &request_trama, sizeof(Trama)) < 0) {
        write_message(STDERR_FILENO, "[ERROR] Could not read complete packet.\n");
        enviar_resposta_connexio_inicial(client_socket, 0);
        netejar_connexio_per_error(client_socket, NULL, &shared_mem_mutex, &active_threads, &threads_mutex);
        return NULL;
    }

    if (request_trama.data_length > sizeof(request_trama.data)) {
        write_message(STDERR_FILENO, "[ERROR] Data length too great. Discarding packet. \n");
        enviar_resposta_connexio_inicial(client_socket, 0);
        netejar_connexio_per_error(client_socket, NULL, &shared_mem_mutex, &active_threads, &threads_mutex);
        return NULL;
    }

    if (request_trama.type != 0x03) {
        write_message(STDERR_FILENO, "[ERROR] Recieved an incorrect packet type petition. \n Disconnecting from client.");
        enviar_resposta_connexio_inicial(client_socket, 0);
        netejar_connexio_per_error(client_socket, NULL, &shared_mem_mutex, &active_threads, &threads_mutex);
        return NULL;
    }

    // --------------------------- PROCESSAMENT NORMAL ---------------------------
    char file_name[256] = {0};
    char file_size_str[12] = {0};
    char received_checksum[MD5SUM_LENGTH] = {0};

    char *first_sep = strchr(request_trama.data, '&');
    if (!first_sep){
        enviar_resposta_connexio_inicial(client_socket, 0);
        netejar_connexio_per_error(client_socket, NULL, &shared_mem_mutex, &active_threads, &threads_mutex);
        return NULL;
    }

    size_t name_len = first_sep - request_trama.data;
    if (name_len >= sizeof(file_name)) name_len = sizeof(file_name) - 1;
    memcpy(file_name, request_trama.data, name_len);
    file_name[name_len] = '\0';

    char *second_sep = strchr(first_sep + 1, '&');
    if (!second_sep) {
        enviar_resposta_connexio_inicial(client_socket, 0);
        netejar_connexio_per_error(client_socket, NULL, &shared_mem_mutex, &active_threads, &threads_mutex);
        return NULL;
    }

    size_t size_len = second_sep - (first_sep + 1);
    if (size_len >= sizeof(file_size_str)) size_len = sizeof(file_size_str) - 1;
    memcpy(file_size_str, first_sep + 1, size_len);
    file_size_str[size_len] = '\0';

    uint32_t file_size = strtoul(file_size_str, NULL, 10);

    char *third_sep = strchr(second_sep + 1, '&');
    if (!third_sep){
        enviar_resposta_connexio_inicial(client_socket, 0);
        netejar_connexio_per_error(client_socket, NULL, &shared_mem_mutex, &active_threads, &threads_mutex);
        return NULL;
    }

    size_t checksum_len = third_sep - (second_sep + 1);
    if (checksum_len >= MD5SUM_LENGTH) checksum_len = MD5SUM_LENGTH - 1;
    memcpy(received_checksum, second_sep + 1, checksum_len);
    received_checksum[checksum_len] = '\0';

    int factor = atoi(third_sep + 1);

    enviar_resposta_connexio_inicial(client_socket, 1);

    // Buscar/crear metadata
    TaskMetadata *metadata = buscar_tasca_per_nom(shared_mem, current_mem_size, file_name, &shared_mem_mutex);

    if (!metadata) {
        metadata = alloc_task_metadata(file_name, file_size, received_checksum,
                                        &shared_mem, &current_mem_size, shared_mem_fd, &shared_mem_mutex);
        if (!metadata) {
            write_message(STDERR_FILENO, "[ERROR] Could not alloc memory for task.\n");
            netejar_connexio_per_error(client_socket, metadata, &shared_mem_mutex, &active_threads, &threads_mutex);
            return NULL;
        }
        strncpy(metadata->file_name, file_name, sizeof(metadata->file_name));
        metadata->total_size = file_size;
        metadata->processed_offset = 0;
        metadata->bytes_sent = 0;
        metadata->status = TASK_RECEIVING;
        strncpy(metadata->checksum, received_checksum, MD5SUM_LENGTH);
    } 

    if (gestionar_recepcio_fitxer(client_socket, metadata, &shared_mem_mutex) < 0) {
        write_message(STDERR_FILENO, "[ERROR] Could not recieve the file to distort.\n");
        netejar_connexio_per_error(client_socket, metadata, &shared_mem_mutex, &active_threads, &threads_mutex);
        return NULL;
    }


    metadata->status = TASK_PROCESSING;

    
    if (processar_media(file_name, factor) != NO_ERROR) {
        write_message(STDERR_FILENO, "[ERROR] Error processing the file.\n");

        netejar_connexio_per_error(client_socket, metadata, &shared_mem_mutex, &active_threads, &threads_mutex);
        return NULL;
    }

    if (!keep_running) {
        write_message(STDERR_FILENO, "[HARLEY] Gotham is down.\n");
        free_task_metadata(metadata, &shared_mem_mutex);  
        close(client_socket);
        pthread_mutex_lock(&threads_mutex);
        active_threads--;
        pthread_mutex_unlock(&threads_mutex);
        return NULL;
    }


    if (detectar_desconnexio_fleck(client_socket)) {
        write_message(STDERR_FILENO, "Client Fleck disconnection detected.\n");
        finalitzar_tasca_per_desconnexio(metadata, file_name, client_socket,
                                  shared_mem, current_mem_size, &shared_mem_mutex);
        return NULL;
    }

    write_message(STDOUT_FILENO, "[HARLEY] Distorsion completed.\n");


    // Recalcular la mida real del fitxer processat
    int fd = open(file_name, O_RDONLY);
    if (fd < 0) {
        write_message(STDERR_FILENO, "[HARLEY] Error opening processed file.\n");
        netejar_connexio_per_error(client_socket, metadata, &shared_mem_mutex, &active_threads, &threads_mutex);
        return NULL;
    }
    uint32_t distorted_file_size;
    off_t size = lseek(fd, 0, SEEK_END);
    if (size == (off_t)-1) {
        close(fd);
        write_message(STDERR_FILENO, "[ERROR] Could not determine distorted files' size.\n");
        netejar_connexio_per_error(client_socket, metadata, &shared_mem_mutex, &active_threads, &threads_mutex);
        return NULL;  // Error al determinar la mida del fitxer
    }
    distorted_file_size = (uint32_t)size;

    pthread_mutex_lock(&shared_mem_mutex);
    metadata->total_size = distorted_file_size;
    pthread_mutex_unlock(&shared_mem_mutex);
    write_message(STDOUT_FILENO, "[HARLEY] Sending distorted file...\n");


    if (enviar_fitxer_processat(client_socket, metadata, &shared_mem_mutex, config.base.socket_fd) < 0){
            write_message(STDOUT_FILENO, "[HARLEY] Could not send distorted file :(\n");
        netejar_connexio_per_error(client_socket, metadata, &shared_mem_mutex, &active_threads, &threads_mutex);
        return NULL;  // Error enviant el fitxer processat
    }


    free_task_metadata(metadata, &shared_mem_mutex);  

    close(client_socket);

    pthread_mutex_lock(&threads_mutex);
    active_threads--;
    pthread_mutex_unlock(&threads_mutex);

    
    return NULL;
}


/**
 * @brief Escoltar connexions entrants i gestionar la possible desconnexió de Fleck.
 */
void listener_harley_principal(ConfigWorker *config) {
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, NULL);


    server_fd = socket(AF_INET, SOCK_STREAM, 0);

    if (server_fd < 0) {
        write_message(STDERR_FILENO, "[HARLEY] Can't listen for client connections. Aborting...\n");
        exit(EXIT_FAILURE);
    }

    // Configurar el socket per reutilitzar l'adreça
    int opt = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        write_message(STDERR_FILENO, "[HARLEY] Can't listen for client connections. Aborting...\n");
        close(server_fd);
        exit(EXIT_FAILURE);
    }
    fcntl(server_fd, F_SETFL, O_NONBLOCK);  

    struct sockaddr_in address = {
        .sin_family = AF_INET,
        .sin_addr.s_addr = inet_addr(config->ip_fleck),
        .sin_port = htons(config->port_fleck),
    };

    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        write_message(STDERR_FILENO, "[HARLEY] Can't listen for client connections. Aborting...\n");
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    if (listen(server_fd, 10) < 0) {
        write_message(STDERR_FILENO, "[HARLEY] Can't listen for client connections. Aborting...\n");
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    while (keep_running) {
        if (!keep_running) break;
        
        fd_set readfds;
        struct timeval timeout;

        FD_ZERO(&readfds);
        FD_SET(server_fd, &readfds);
        timeout.tv_sec = 1;
        timeout.tv_usec = 0;

        int activity = select(server_fd + 1, &readfds, NULL, NULL, &timeout);

        if (activity < 0 && errno != EINTR) {
            write_message(STDERR_FILENO, "[ERROR] select() \n");
            break;
        }

        if (activity == 0) {
            // Timeout: no connexions
            continue;
        }

        if (FD_ISSET(server_fd, &readfds)) {
            int client_socket = accept(server_fd, NULL, NULL);
            
            if (client_socket < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    if (!keep_running) break;  
                    usleep(100000); // Evitar bucle de CPU 100%
                    continue;  
                } else {
                    char dbg[128];
                    snprintf(dbg, sizeof(dbg), "[ERROR] accept() errno=%d (%s)\n", errno, strerror(errno));
                    write_message(STDERR_FILENO, dbg);
                    break;
                }
            }

            if (client_socket >= 0) {

                int *socket_ptr = malloc(sizeof(int));
                if (!socket_ptr) {
                    write_message(STDERR_FILENO, "[ERROR] Memory allocation error handling the task.\n");
                    close(client_socket);
                    continue;
                }

                *socket_ptr = client_socket;

                pthread_t task_thread;
                if (pthread_create(&task_thread, NULL, handle_task, socket_ptr) != 0) {
                    write_message(STDERR_FILENO, "[ERROR] Could not create thread for handling the task.\n");
                    close(client_socket);
                    free(socket_ptr);
                    continue;
                }

                pthread_detach(task_thread);  
            }
        }

    }

    close(server_fd);

}

void *listener_harley_wrapper(void *arg) {
    listener_harley_principal((ConfigWorker *)arg);
    return NULL;
}



void *gotham_listener_thread(void *arg) {
    ConfigWorker *config = (ConfigWorker *)arg;
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, NULL);


    while (keep_running) {
        if (!keep_running) break;
        Trama trama;
        if (config->base.socket_fd <= 0) {
            write_message(STDERR_FILENO, "[ERROR] Could not validate Gothams' connection. Reconnecting...\n");
            reconnectar_gotham(config, keep_running);
            sleep(1);
            continue;
        }
        fd_set readfds;
        struct timeval timeout;
        FD_ZERO(&readfds);
        FD_SET(config->base.socket_fd, &readfds);
        timeout.tv_sec = 1;
        timeout.tv_usec = 0;

        int result = select(config->base.socket_fd + 1, &readfds, NULL, NULL, &timeout);
        if (result < 0) {
            if (errno == EINTR) break;  // senyal rebuda
            continue;
        } else if (result == 0) {
            if (!keep_running) break;  // timeout
            continue;
        }

        ssize_t bytes_read = read(config->base.socket_fd, &trama, sizeof(trama));
        if (bytes_read < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                sleep(1);  // Evitar CPU 100%
                continue;
            }
        }
        if (bytes_read != sizeof(trama)) {
            write_message(STDERR_FILENO, "[ERROR] Could not read Gotham's packet.\n");
            if (!keep_running) break;

            if (reconnectar_gotham(config, keep_running) < 0) {
                write_message(STDERR_FILENO, "[ERROR] Could not connect to Gotham. Worker is stopping.\n");
                keep_running = 0;
                break;  
            }
            continue;
        }

        switch (trama.type) {
            case 0x02:
                // Trama de connexió de Gotham
                write_message(STDOUT_FILENO, "Server Gotham has accepted connection.\n");
                break;
            case 0x08:
                config->is_primary_worker = 1;
                write_message(STDOUT_FILENO, "Now I am the main worker.\n");
                write_message(STDOUT_FILENO, "[HARLEY] Listening for Fleck client petitions...\n");
                pthread_create(&harley_thread, NULL, listener_harley_wrapper, config);
                break;
            case 0x09:
                // Trama d'error de Gotham
                write_message(STDERR_FILENO, "Gotham has sent an error packet.\n");
                break;
            case 0x12:
                // Heartbeat de Gotham
                write_message(STDOUT_FILENO, "Gotham has asked for a heartbeat. Answering...\n");
                enviar_heartbeat();
                break;
            default:
                write_message(STDERR_FILENO, "Trama desconeguda de Gotham.\n");
                // Enviar trama de error (0x09) de respuesta
                Trama error_trama;
                error_trama.type = 0x09;
                error_trama.data_length = 0; // Sin datos
                memset(error_trama.data, 0, sizeof(error_trama.data)); // Vaciar data

                if (write(config->base.socket_fd, &error_trama, sizeof(error_trama)) != sizeof(error_trama)) {
                    write_message(STDERR_FILENO, "[ERROR] Could not send error packet to Gotham server.\n");
                }

                break;
        }
    }
    write_message(STDOUT_FILENO, "[HARLEY] Gotham loop finalitzat.\n");
    return NULL;
}

/**
 * @brief Gestiona la recepció de la senyal SIGINT per netejar els recursos del worker.
 * @param sig Senyal rebut (no s'utilitza, però és necessari per la funció de gestió de senyals).
 */
void gestionar_signal_interrupt(int sig) {
    (void)sig;
    keep_running = 0;
   
    // Enviar desconnexió a Gotham
    gestionar_desconnexio_gotham(config.base.socket_fd, "media");

    if (config.base.socket_fd > 0) {
        close(config.base.socket_fd);
        config.base.socket_fd = -1;
    }
    if (server_fd > 0) {
        close(server_fd);
        server_fd = -1;
    }

    // Decrementar comptador de workers
    write_message(STDOUT_FILENO, "[DEBUG] Decrementant comptador de workers a la memòria compartida...\n");
    if (pthread_mutex_lock(&shared_mem_mutex) != 0) {
        write_message(STDERR_FILENO, "[ERROR] No s'ha pogut bloquejar el mutex de memòria compartida.\n");
    } else {
        int *worker_count = (int *)shared_mem;
        
        if (*worker_count > 0) (*worker_count)--; 


        char count_msg[128];
        snprintf(count_msg, sizeof(count_msg), "[DEBUG] Nombre actual de workers: %d\n", *worker_count);
        write_message(STDOUT_FILENO, count_msg);

        if (*worker_count == 0) {
            write_message(STDOUT_FILENO, "[DEBUG] Últim worker en peu. Alliberant recursos compartits...\n");
            if (shm_unlink("/harley_shared_mem") < 0) {
                write_message(STDERR_FILENO, "[ERROR] No s'ha pogut fer unlink de la memòria compartida.\n");
            }
        }
        pthread_mutex_unlock(&shared_mem_mutex);
    }

    msync(shared_mem, current_mem_size, MS_SYNC);


    // Alliberar memòria compartida
    if (munmap(shared_mem, current_mem_size) < 0) {
        write_message(STDERR_FILENO, "[ERROR] No s'ha pogut fer munmap.\n");
    }

    if (!pthread_equal(gotham_thread, (pthread_t)0)) {
        pthread_cancel(gotham_thread);
        pthread_join(gotham_thread, NULL);
    }

    if (config.is_primary_worker && !pthread_equal(harley_thread, (pthread_t)0)) {
        pthread_cancel(harley_thread);
        pthread_join(harley_thread, NULL);
    }


    // Alliberar memòria de configuració
    free(config.work_dir);

    exit(0);
}

/**
 * @brief Thread que envia un heartbeat cada 5 segons per indicar que Harley està actiu.
 * @param arg Punter a l'argument (no s'utilitza).
 * @return Retorna NULL quan el thread finalitza.
 */
void *heartbeat_thread(void *arg) {
    (void)arg;  
    while (keep_running) {
        sleep(5);  // o cada 10 segons
        enviar_heartbeat();
    }
    return NULL;
}


/**
 * @brief Punt d'entrada principal.
 */
int main(int argc, char *argv[]) {
    if (argc != 2) {
        write_message(STDERR_FILENO, "Expected: ./harley <config file>\n");
        return 1;
    }

    struct sigaction sa;
    sa.sa_handler = gestionar_signal_interrupt;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0; 
    sigaction(SIGINT, &sa, NULL);

    int config_fd = open(argv[1], O_RDONLY);
    if (config_fd < 0 || parse_worker_config(config_fd, &config) < 0) return 1;
    close(config_fd);

    assegurar_working_directory(config.work_dir, 1);
    

    char full_workdir[512];
    snprintf(full_workdir, sizeof(full_workdir), "Worker/Harley/%s", config.work_dir);

    if (config.work_dir) free(config.work_dir);  


    config.work_dir = strdup(full_workdir);

    // Canviar el directori actual del procés a aquest path
    if (chdir(config.work_dir) != 0) {
        return 1;
    }

    int primer_worker = inicialitzar_memoria_compartida("/harley_shared_mem", &shared_mem_fd, &shared_mem, 
                                    current_mem_size, &shared_mem_mutex); 
    
    //netejar_memoria_tasques(shared_mem, current_mem_size, &shared_mem_mutex);


    pthread_mutex_lock(&shared_mem_mutex);
    volcar_memoria_tasques(shared_mem, current_mem_size);
    pthread_mutex_unlock(&shared_mem_mutex);                                

    int *worker_count = (int *)shared_mem;

    if (primer_worker) {
        write_message(STDOUT_FILENO, "[DEBUG] Aquest és el primer worker. Inicialitzant estat de memòria...\n");
        netejar_memoria_tasques(shared_mem, current_mem_size, &shared_mem_mutex);
    }

    pthread_mutex_lock(&shared_mem_mutex);
    volcar_memoria_tasques(shared_mem, current_mem_size);
    pthread_mutex_unlock(&shared_mem_mutex);  

    char dbg[128];
    snprintf(dbg, sizeof(dbg), "[DEBUG] Nombre actual de workers després d’iniciar: %d\n", *worker_count);
    write_message(STDOUT_FILENO, dbg);


    inicialitzar_connexio_gotham(&config);
    int flags = fcntl(config.base.socket_fd, F_GETFL, 0);
    fcntl(config.base.socket_fd, F_SETFL, flags | O_NONBLOCK);


    pthread_create(&gotham_thread, NULL, gotham_listener_thread, &config);

    pthread_t heartbeat_tid;
    pthread_create(&heartbeat_tid, NULL, heartbeat_thread, NULL);
    pthread_detach(heartbeat_tid);  


    if (config.is_primary_worker) {
        pthread_join(harley_thread, NULL);
    }
    pthread_join(gotham_thread, NULL);

    // Esperar que s'acabin totes les tasques abans de netejar
    while (1) {
        pthread_mutex_lock(&threads_mutex);
        int threads = active_threads;
        pthread_mutex_unlock(&threads_mutex);

        if (threads == 0) break;

        sleep(10);
    }

    gestionar_signal_interrupt(SIGINT);

    write_message(STDOUT_FILENO, "Worker Harley finalitzat correctament.\n");
        
    return 0;
}
