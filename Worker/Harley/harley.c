#include "../worker.h"
#include "../../Utils/task_manager.h"
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
static TaskManager task_manager;
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

    write_message(STDOUT_FILENO, "[DEBUG] Estat del fitxer ABANS de copiar a temporal:\n");
    mostrar_estat_fitxer(file_name);


    // DEBUG
    char dbg[512];
    snprintf(dbg, sizeof(dbg), "[DEBUG] Copiant fitxer %s a temporal %s\n", file_name, tmp_file);
    write_message(STDOUT_FILENO, dbg);

    if (copiar_fitxer(file_name, tmp_file) < 0) {
        write_message(STDERR_FILENO, "Error copiant el fitxer per compressió segura\n");
        return -1;
    }

    write_message(STDOUT_FILENO, "[DEBUG] Estat del fitxer DESPRÉS de copiar a temporal:\n");
    mostrar_estat_fitxer(file_name);

  
    int result;

    if (strstr(file_name, ".wav")) {
        write_message(STDOUT_FILENO, "[HARLEY] Processant fitxer d'àudio .wav...\n");
        result = SO_compressAudio(tmp_file, factor);
        char msg[128];
        snprintf(msg, sizeof(msg), "[HARLEY] Compressió d'àudio finalitzada, resultat: %d\n", result);
        write_message(STDOUT_FILENO, msg);
    } else if (strstr(file_name, ".jpg") || strstr(file_name, ".png")) {
        write_message(STDOUT_FILENO, "[HARLEY] Processant imatge...\n");
        result = SO_compressImage(tmp_file, factor);
        char msg[128];
        snprintf(msg, sizeof(msg), "[HARLEY] Compressió d'imatge finalitzada, resultat: %d\n", result);
        write_message(STDOUT_FILENO, msg);
    } else if (strstr(file_name, ".mp3")) {
        write_message(STDERR_FILENO, "[ERROR] .mp3 no suportat. Només .wav.\n");
        result = ERROR_NOT_WAV;
    } else {
        write_message(STDERR_FILENO, "[ERROR] Tipus de fitxer no suportat per a compressió.\n");
        result =  ERROR_UNSUPPORTED_FORMAT;
    }

    write_message(STDOUT_FILENO, "[DEBUG] Estat del fitxer DESPRÉS de la compressió:\n");
    mostrar_estat_fitxer(tmp_file);

    if (result != NO_ERROR) {
        write_message(STDERR_FILENO, "Error en compressió. No es substituirà l’original.\n");
        unlink(tmp_file);
        return result;
    }

    // Substituir l’original pel fitxer comprimit
    if (rename(tmp_file, file_name) < 0) {
        write_message(STDERR_FILENO, "Error substituint l’original pel fitxer comprimit.\n");
        unlink(tmp_file);  // Evitar que quedi penjat
        return -1;
    }

    write_message(STDOUT_FILENO, "[DEBUG] Estat del fitxer DESPRÉS del rename:\n");
    mostrar_estat_fitxer(tmp_file);

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
    write_message(STDOUT_FILENO, "[HARLEY] Iniciant gestió de tasca...\n");


    Trama request_trama;
    if (safe_read(client_socket, &request_trama, sizeof(Trama)) < 0) {
        write_message(STDERR_FILENO, "[ERROR] No es pot llegir la trama completa de 256 bytes\n");
        enviar_resposta_connexio_inicial(client_socket, 0);
        netejar_connexio_per_error(client_socket, NULL, &shared_mem_mutex, &active_threads, &threads_mutex);
        return NULL;
    }

    if (request_trama.data_length > sizeof(request_trama.data)) {
        write_message(STDERR_FILENO, "[ERROR] Data_length massa gran, descartant trama\n");
        enviar_resposta_connexio_inicial(client_socket, 0);
        netejar_connexio_per_error(client_socket, NULL, &shared_mem_mutex, &active_threads, &threads_mutex);
        return NULL;
    }

    if (request_trama.type != 0x03) {
        write_message(STDERR_FILENO, "[ERROR] Trama rebutjada per tipus incorrecte\n");
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
    char debug[64];
    snprintf(debug, sizeof(debug), "[DEBUG] Nom rebut per guardar: %s\n", file_name);
    write_message(STDOUT_FILENO, debug);

    if (!metadata) {
        write_message(STDOUT_FILENO, "[ERROR] Tasca no existent, creant nova tasca.\n");
        metadata = alloc_task_metadata(file_name, file_size, received_checksum,
                                        &shared_mem, &current_mem_size, shared_mem_fd, &shared_mem_mutex);
        if (!metadata) {
            write_message(STDERR_FILENO, "[ERROR] No s'ha pogut reservar espai per la nova tasca.\n");
            //GESTIONAR ERROR (NETEJAR MEMORIA, actualitzar estat de la tasca, etc.)
            netejar_connexio_per_error(client_socket, metadata, &shared_mem_mutex, &active_threads, &threads_mutex);
            return NULL;
        }
        strncpy(metadata->file_name, file_name, sizeof(metadata->file_name));
        metadata->total_size = file_size;
        metadata->processed_offset = 0;
        metadata->bytes_sent = 0;
        metadata->status = TASK_RECEIVING;
        strncpy(metadata->checksum, received_checksum, MD5SUM_LENGTH);
        add_task_to_manager(&task_manager, client_socket, file_name, &config);
    } else {
        write_message(STDOUT_FILENO, "Reprenent tasca existent.\n");
    }

    write_message(STDOUT_FILENO, "[HARLEY] Iniciant recepció de fitxer...\n");
    if (gestionar_recepcio_fitxer(client_socket, metadata, &shared_mem_mutex) < 0) {
        write_message(STDERR_FILENO, "[ERROR] Error rebent el fitxer.\n");
        //GESTIONAR ERROR (NETEJAR MEMORIA, actualitzar estat de la tasca, etc.)
        netejar_connexio_per_error(client_socket, metadata, &shared_mem_mutex, &active_threads, &threads_mutex);
        return NULL;
    }
    write_message(STDOUT_FILENO, "[DEBUG] Estat del fitxer després de recepció:\n");
    mostrar_estat_fitxer(file_name);


    metadata->status = TASK_PROCESSING;
    
    write_message(STDOUT_FILENO, "[HARLEY] Executant processar_media per fitxer: ");
    write_message(STDOUT_FILENO, file_name);
    write_message(STDOUT_FILENO, "\n");
    
    if (processar_media(file_name, factor) != NO_ERROR) {
        write_message(STDERR_FILENO, "[ERROR] Error processant el fitxer.\n");

        //GESTIONAR ERROR (NETEJAR MEMORIA, actualitzar estat de la tasca, etc.)
        netejar_connexio_per_error(client_socket, metadata, &shared_mem_mutex, &active_threads, &threads_mutex);
        return NULL;
    }

    if (detectar_desconnexio_fleck(client_socket)) {
        write_message(STDERR_FILENO, "Detectada desconnexió Fleck.\n");
        //GESTIONAR ERROR (NETEJAR MEMORIA, actualitzar estat de la tasca, etc.)
        finalitzar_tasca_per_desconnexio(metadata, file_name, client_socket, &task_manager,
                                  shared_mem, current_mem_size, &shared_mem_mutex);
        return NULL;
    }

    write_message(STDOUT_FILENO, "[HARLEY] Compressió finalitzada. Obrint fitxer per determinar mida...\n");


    // Recalcular la mida real del fitxer processat
    int fd = open(file_name, O_RDONLY);
    if (fd < 0) {
        write_message(STDERR_FILENO, "[HARLEY] Error obrint fitxer processat. Potser no s'ha creat?\n");
        //GESTIONAR ERROR (NETEJAR MEMORIA, actualitzar estat de la tasca, etc.)
        return NULL;
    }
    uint32_t distorted_file_size;
    off_t size = lseek(fd, 0, SEEK_END);
    if (size == (off_t)-1) {
        close(fd);
        write_message(STDERR_FILENO, "[HARLEY] Error al determinar la mida del fitxer processat.\n");
        //GESTIONAR ERROR (NETEJAR MEMORIA, actualitzar estat de la tasca, etc.)
        return NULL;  // Error al determinar la mida del fitxer
    }
    distorted_file_size = (uint32_t)size;

    pthread_mutex_lock(&shared_mem_mutex);
    metadata->total_size = distorted_file_size;
    pthread_mutex_unlock(&shared_mem_mutex);
    write_message(STDOUT_FILENO, "[HARLEY] Enviant fitxer processat a Fleck...\n");


    if (enviar_fitxer_processat(client_socket, metadata, &shared_mem_mutex, config.base.socket_fd) < 0){
        write_message(STDERR_FILENO, "Error enviant el fitxer processat al client.\n");
        //GESTIONAR ERROR (NETEJAR MEMORIA, actualitzar estat de la tasca, etc.)
        return NULL;  // Error enviant el fitxer processat
    }


    remove_task(&task_manager, client_socket, &task_manager.mutex);
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

    write_message(STDOUT_FILENO, "[HARLEY] Entrem al thread de listener de flecks.\n");

    server_fd = socket(AF_INET, SOCK_STREAM, 0);

    if (server_fd < 0) {
        write_message(STDERR_FILENO, "[HARLEY] Error creant el socket del servidor.\n");
        exit(EXIT_FAILURE);
    }

    // Configurar el socket per reutilitzar l'adreça
    int opt = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        write_message(STDERR_FILENO, "[HARLEY] Error en setsockopt(SO_REUSEADDR).\n");
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
        write_message(STDERR_FILENO, "[HARLEY] Error en el bind del socket del servidor.\n");
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    if (listen(server_fd, 10) < 0) {
        write_message(STDERR_FILENO, "[HARLEY] Error en el listen del socket del servidor.\n");
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    char info[256];
    time_t now = time(NULL);
    strftime(info, sizeof(info), "[%Y-%m-%d %H:%M:%S]", localtime(&now));

    char msg[512];
    snprintf(msg, sizeof(msg), "%s Worker Harley principal escoltant connexions a %s:%d\n", 
            info, config->ip_fleck, config->port_fleck);
    write_message(STDOUT_FILENO, msg);


    write_message(STDOUT_FILENO, "Worker Harley principal iniciat i escoltant connexions...\n");

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
            write_message(STDERR_FILENO, "[ERROR] select() ha fallat.\n");
            break;
        }

        if (activity == 0) {
            // Timeout: no connexions
            continue;
        }

        if (FD_ISSET(server_fd, &readfds)) {
            int client_socket = accept(server_fd, NULL, NULL);
            
            if (client_socket < 0) {
                write_message(STDOUT_FILENO, "[DEBUG] accept() retornó -1, comprovant errno...\n");
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    write_message(STDOUT_FILENO, "[DEBUG] Motiu de -1 era EAGAIN o EWOULDBLOCK\n");
                    if (!keep_running) break;  
                    usleep(100000); // Evitar bucle de CPU 100%
                    continue;  
                } else {
                    char dbg[128];
                    snprintf(dbg, sizeof(dbg), "[ERROR] accept() ha fallat amb errno=%d (%s)\n", errno, strerror(errno));
                    write_message(STDERR_FILENO, dbg);
                    break;
                }
            }

            if (client_socket >= 0) {
                char dbg_msg[128];
                snprintf(dbg_msg, sizeof(dbg_msg), "[DEBUG] accept() retornó socket %d\n", client_socket);
                write_message(STDOUT_FILENO, dbg_msg);

                int *socket_ptr = malloc(sizeof(int));
                if (!socket_ptr) {
                    write_message(STDERR_FILENO, "[ERROR] No s'ha pogut reservar memòria per al socket_ptr\n");
                    close(client_socket);
                    continue;
                }

                *socket_ptr = client_socket;

                pthread_t task_thread;
                if (pthread_create(&task_thread, NULL, handle_task, socket_ptr) != 0) {
                    write_message(STDERR_FILENO, "[ERROR] No s'ha pogut crear el thread per a la tasca\n");
                    close(client_socket);
                    free(socket_ptr);
                    continue;
                }

                pthread_detach(task_thread);  
            }
        }

    }

    close(server_fd);
    write_message(STDOUT_FILENO, "[HARLEY] Fleck thread finalitzat.\n");

}

void *listener_harley_wrapper(void *arg) {
    write_message(STDOUT_FILENO, "[HARLEY] Entrem al wrapper del thread.\n");
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
            write_message(STDERR_FILENO, "Socket de Gotham no vàlid, intentant reconectar...\n");
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
            write_message(STDERR_FILENO, "Error llegint trama de Gotham.\n");
            if (!keep_running) break;

            if (reconnectar_gotham(config, keep_running) < 0) {
                write_message(STDERR_FILENO, "No s'ha pogut reconnectar amb Gotham. Aturant el worker.\n");
                keep_running = 0;
                break;  
            }
            continue;
        }

        switch (trama.type) {
            case 0x02:
                // Trama de connexió de Gotham
                write_message(STDOUT_FILENO, "Gotham ha confirmat la connexió.\n");
                break;
            case 0x08:
                char info[256];
                time_t now = time(NULL);
                strftime(info, sizeof(info), "[%Y-%m-%d %H:%M:%S]", localtime(&now));

                char msg[512];
                snprintf(msg, sizeof(msg), "%s Gotham ha enviat una trama de promoció.\n", 
                        info);
                write_message(STDOUT_FILENO, msg);

                config->is_primary_worker = 1;
                write_message(STDOUT_FILENO, "Harley ara és el worker principal.\n");
                write_message(STDOUT_FILENO, "[Harley] Iniciant thread per a Flecks...\n");
                pthread_create(&harley_thread, NULL, listener_harley_wrapper, config);
                break;
            case 0x09:
                // Trama d'error de Gotham
                write_message(STDERR_FILENO, "Gotham ha enviat una trama d'error.\n");
                break;
            case 0x12:
                // Heartbeat de Gotham
                write_message(STDOUT_FILENO, "Gotham ha demanat un heartbeat. Responent...\n");
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
                    write_message(STDERR_FILENO, "Error enviant trama de resposta d'error a Gotham.\n");
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
   
    write_message(STDOUT_FILENO, "[DEBUG] Signal SIGINT rebut. Iniciant neteja del worker...\n");

    // Enviar desconnexió a Gotham
    write_message(STDOUT_FILENO, "[DEBUG] Notificant desconnexió a Gotham...\n");
    gestionar_desconnexio_gotham(config.base.socket_fd, "media");

    if (config.base.socket_fd > 0) {
        write_message(STDOUT_FILENO, "[DEBUG] Tancant el socket d’escolta de Harley...\n");
        close(config.base.socket_fd);
        config.base.socket_fd = -1;
    }
    if (server_fd > 0) {
        write_message(STDOUT_FILENO, "[DEBUG] Tancant socket d’escolta del listener de Harley...\n");
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
    write_message(STDOUT_FILENO, "[DEBUG] Alliberant memòria compartida...\n");
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
    write_message(STDOUT_FILENO, "[DEBUG] Alliberant memòria de config...\n");
    free(config.work_dir);

    write_message(STDOUT_FILENO, "[DEBUG] Finalització de Harley completa.\n");
    exit(0);
}

/**
 * @brief Thread que envia un heartbeat cada 5 segons per indicar que Harley està actiu.
 * @param arg Punter a l'argument (no s'utilitza).
 * @return Retorna NULL quan el thread finalitza.
 */
void *heartbeat_thread(void *arg) {
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

        write_message(STDOUT_FILENO, "[DEBUG] Esperant que finalitzin %d distorsions...\n", threads);
        sleep(10);
    }

    gestionar_signal_interrupt(SIGINT);

    write_message(STDOUT_FILENO, "Worker Harley finalitzat correctament.\n");
        
    return 0;
}
