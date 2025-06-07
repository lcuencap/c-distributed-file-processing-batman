#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <ctype.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <time.h>
#include <pthread.h>

#include <stdio.h>
#include "../Utils/file_utils.h"
#include "../Utils/sys_params.h"
#include "../Utils/trames.h"
#include "../Utils/so_compression.h"

#define BUF_INICIAL 256
#define TRAMA_HEADER_SIZE (sizeof(((Trama *)0)->type) + sizeof(((Trama *)0)->data_length) + sizeof(((Trama *)0)->timestamp) + sizeof(((Trama *)0)->checksum))
#define TIMEOUT_SECONDS 5 // Temps límit per rebre respostes
#define MAX_RETRIES 3     // Nombre màxim de retries per connectar a Gotham o un worker
#define ERROR_OUTPUT_FILE  -1
#define ERROR_READ         -2
#define ERROR_WORKER_DOWN  -3
#define ERROR_NEW_WORKER   -4
#define ERROR_CONNECT_NEW  -5
#define ERROR_VALIDATION   -6
#define ERROR_MD5_MISMATCH -7

ConfigFleck config;
int connected_to_gotham = 0;  // Indica si Fleck està connectat a Gotham
int sock_fd = -1;             // Descriptor del socket connectat a Gotham
int worker_fd = -1;          // Descriptor del socket connectat al worker

static DistorsioStatus *head = NULL;
static pthread_mutex_t status_mutex = PTHREAD_MUTEX_INITIALIZER;

void register_new_file(const char *file_name) {
    pthread_mutex_lock(&status_mutex);
    DistorsioStatus *new_status = malloc(sizeof(DistorsioStatus));
    if (new_status) {
        strncpy(new_status->file_name, file_name, sizeof(new_status->file_name));
        new_status->file_name[sizeof(new_status->file_name) - 1] = '\0';
        new_status->progress = 0;
        new_status->next = head;
        head = new_status;
    }
    pthread_mutex_unlock(&status_mutex);
}

void update_progress(const char *file_name, int percent) {
    pthread_mutex_lock(&status_mutex);
    DistorsioStatus *curr = head;
    while (curr) {
        if (strcmp(curr->file_name, file_name) == 0) {
            curr->progress = percent;
            break;
        }
        curr = curr->next;
    }
    pthread_mutex_unlock(&status_mutex);
}

void unregister_file(const char *file_name) {
    pthread_mutex_lock(&status_mutex);
    DistorsioStatus **curr = &head;
    while (*curr) {
        if (strcmp((*curr)->file_name, file_name) == 0) {
            DistorsioStatus *to_delete = *curr;
            *curr = (*curr)->next;
            free(to_delete);
            break;
        }
        curr = &((*curr)->next);
    }
    pthread_mutex_unlock(&status_mutex);
}

void print_all_progress() {
    pthread_mutex_lock(&status_mutex);
    DistorsioStatus *curr = head;
    char buffer[512];

    if (!head) {
        write_message(STDOUT_FILENO, "You have no ongoing or finished distorsions.\n");
        pthread_mutex_unlock(&status_mutex);
        return;
    }

    while (curr) {
        int offset = snprintf(buffer, sizeof(buffer), "%s %d%% |", curr->file_name, curr->progress);
        int bars = curr->progress / 5;
        for (int i = 0; i < bars && offset < (int)(sizeof(buffer) - 1); ++i) {
            buffer[offset++] = '=';
        }
        if (offset < (int)(sizeof(buffer) - 1)) {
            buffer[offset++] = '\n';
        }
        write_message(STDOUT_FILENO, buffer);
        curr = curr->next;
    }

    pthread_mutex_unlock(&status_mutex);
}


void enviar_trama_desconnexio_worker() {
    if (worker_fd >= 0) {
        Trama trama;
        inicialitzar_trama(&trama, 0x07, config.username);
        write(worker_fd, &trama, sizeof(trama));
        close(worker_fd);
        worker_fd = -1;
        write_message(STDOUT_FILENO, "Desconnectat correctament del worker.\n");
    }
}

void enviar_trama_desconnexio_gotham() {
    if (sock_fd >= 0) {
        Trama trama;
        inicialitzar_trama(&trama, 0x07, config.username);
        write(sock_fd, &trama, sizeof(trama));
        close(sock_fd);
        sock_fd = -1;
        connected_to_gotham = 0;
        write_message(STDOUT_FILENO, "Desconnectat correctament de Gotham.\n");
    }
}



/**
 * Connecta amb un servidor (ja sigui Gotham o un worker) usant sockets TCP.
 * @param ip L'adreça IP del servidor.
 * @param port El port del servidor.
 * @return Descriptor del socket connectat, o -1 si hi ha un error.
 */
int connectar_a_server(const char *ip, int port) {
    int retries = 0;
    char buffer[256];
    int sock_fd = -1;

    while (retries < MAX_RETRIES) {
        snprintf(buffer, sizeof(buffer), "Intentant connectar a %s:%d (Intent %d/%d)\n", ip, port, retries + 1, MAX_RETRIES);
        write_message(STDERR_FILENO, buffer);

        sock_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (sock_fd < 0) {
            write_message(STDERR_FILENO, "Error creant el socket\n");
            return -1;
        }

        struct sockaddr_in server_addr;
        memset(&server_addr, 0, sizeof(server_addr));
        server_addr.sin_family = AF_INET;
        server_addr.sin_port = htons(port);

        if (inet_pton(AF_INET, ip, &server_addr.sin_addr) <= 0) {
            write_message(STDERR_FILENO, "Error convertint l'adreça IP\n");
            close(sock_fd);
            return -1;
        }

        if (connect(sock_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
            close(sock_fd);
            retries++;
            write_message(STDERR_FILENO, "Error connectant al servidor, reintentant...\n");
            sleep(1);
            continue;
        }

        

        struct timeval timeout;
        timeout.tv_sec = TIMEOUT_SECONDS;
        timeout.tv_usec = 0;
        setsockopt(sock_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

        snprintf(buffer, sizeof(buffer), "Connexió establerta amb %s:%d\n", ip, port);
        write_message(STDERR_FILENO, buffer);
        return sock_fd;
    }
    write_message(STDERR_FILENO, "Error: No s'ha pogut connectar després de múltiples intents.\n");    
    if (sock_fd >= 0) {
        Trama trama_desconnexio;
        inicialitzar_trama(&trama_desconnexio, 0x07, config.username);
        write(sock_fd, &trama_desconnexio, sizeof(trama_desconnexio));
        close(sock_fd);
    }


    return -1;
}



/**
 * Gestió de la senyal Ctrl+C per tancar correctament Fleck.
 * Envia una trama de desconnexió a Gotham i al worker (si escau) i allibera recursos.
 * @param sig Senyal rebuda (no utilitzada).
 */
void ctrlc_handler(int sig) {
    (void)sig;  
    write_message(STDOUT_FILENO, "S'ha rebut Ctrl+C. Tancant Fleck...\n");

     if (connected_to_gotham) {
        enviar_trama_desconnexio_gotham();  
    }

    if (worker_fd >= 0) {
        enviar_trama_desconnexio_worker();
    }
    write_message(STDOUT_FILENO, "Fleck finalitzat correctament.\n");
    exit(0);
}



/**
 * Estableix la connexió inicial amb Gotham enviant una trama de tipus 0x01.
 */
void establir_connexio_gotham() {
    Trama trama;
    char data[365];
    snprintf(data, sizeof(data), "%s&%s&%d", config.username, config.base.ip, config.base.port);
    inicialitzar_trama(&trama, 0x01, data);

    write(sock_fd, &trama, sizeof(trama));

    Trama resposta;
    if (safe_read(sock_fd, &resposta, sizeof(Trama)) < 0) {
        write_message(STDOUT_FILENO, "Error rebent resposta de Gotham.\n");
        return;
    }

    if (!validar_trama(&resposta)) {
        write_message(STDOUT_FILENO, "Resposta de Gotham invàlida.\n");
        return;
    }

    if (resposta.type == 0x01 && resposta.data_length == 0) {
        write_message(STDOUT_FILENO, "Connexió establerta amb Gotham.\n");
    } else if (strcmp(resposta.data, "CON_KO") == 0) {
        write_message(STDOUT_FILENO, "Connexió rebutjada per Gotham.\n");
    }
}


void gestionar_comanda_list(const char **media_types, size_t count) {
    DIR *dir = opendir(".");
    if (!dir) {
        return;
    }

    struct dirent *entry;
    int index = 1;
    char buffer[512];

    while ((entry = readdir(dir)) != NULL) {
        if (entry->d_type == DT_REG) {
            const char *file_name = entry->d_name;
            for (size_t i = 0; i < count; i++) {
                const char *ext = media_types[i];
                size_t len_file = strlen(file_name);
                size_t len_ext = strlen(ext);
                if (len_file > len_ext && strcmp(file_name + len_file - len_ext, ext) == 0) {
                    int len = snprintf(buffer, sizeof(buffer), "%d. %s\n", index++, file_name);
                    if (len > 0) {
                        write_message(STDOUT_FILENO, buffer);
                    }
                    break;
                }
            }
        }
    }

    if (index == 1) {
        write_message(STDOUT_FILENO, "No s'han trobat fitxers amb aquestes extensions.\n");
    }

    closedir(dir);
}



int solicitar_nou_worker(const char *media_type, const char *file_name, char *worker_ip, int *worker_port) {
    Trama peticio;
    char data[245];
    snprintf(data, sizeof(data), "%s&%s", media_type, file_name);
    inicialitzar_trama(&peticio, 0x11, data);

    // Enviar la petició a Gotham
    if (write(sock_fd, &peticio, sizeof(peticio)) != sizeof(peticio)) {
        write_message(STDOUT_FILENO, "Error enviant petició a Gotham per assignar nou worker.\n");
        return -1;
    }

    // Llegir la resposta de Gotham
    Trama resposta;
    if (safe_read(sock_fd, &resposta, sizeof(Trama)) < 0) {
        write_message(STDOUT_FILENO, "Error rebent resposta de Gotham.\n");
        return -1;
    }

    if (!validar_trama(&resposta)) {
        write_message(STDOUT_FILENO, "Resposta de Gotham invàlida.\n");
        return -1;
    }

    if (strcmp(resposta.data, "DISTORT_KO") == 0) {
        write_message(STDOUT_FILENO, "Error: No hi ha workers disponibles.\n");
        return -1;
    } else if (strcmp(resposta.data, "MEDIA_KO") == 0) {
        write_message(STDOUT_FILENO, "Error: Tipus de media incorrecte.\n");
        return -1;
    } else {
        // Extraer IP y puerto del nuevo Worker manualmente
        char *delimiter = strchr(resposta.data, '&');
        if (!delimiter) {
            write_message(STDOUT_FILENO, "Error: Resposta de Gotham invàlida.\n");
            return -1;
        }

        size_t ip_length = delimiter - resposta.data;
        strncpy(worker_ip, resposta.data, ip_length);
        worker_ip[ip_length] = '\0'; // Añadir terminador nulo

        *worker_port = atoi(delimiter + 1); // Convertir el puerto a entero

        return 0;
    }
}

int validar_connexio_worker() {
    Trama resposta;

    if (safe_read(worker_fd, &resposta, sizeof(Trama)) < 0) {
        write_message(STDOUT_FILENO, "[ERROR] No s'ha pogut llegir la resposta del Worker\n");
        close(worker_fd);
        return -1;
    }

    if (resposta.type == 0x03 && resposta.data_length == 0) {
        write_message(STDOUT_FILENO, "[OK] Connexió amb el worker validada.\n");
        return 0;
    }

    if (resposta.type == 0x03 && strcmp(resposta.data, "CON_KO") == 0) {
        write_message(STDOUT_FILENO, "El worker ha rebutjat la connexió.\n");
        return -1;
    }

    write_message(STDOUT_FILENO, "[ERROR] Resposta inesperada del worker.\n");
    return -1;
}


/**
 * Envia el fitxer al worker.
 * @param file_name Nom del fitxer a enviar.
 * @param file_size Mida del fitxer en bytes.
 * @param md5sum MD5SUM del fitxer per verificar la integritat.
 * @param factor Factor de distorsió.
 * @return 0 si l'enviament és correcte, -1 si hi ha un error.
 */
int enviar_fitxer_worker(const char *file_name, uint32_t file_size, const char *md5sum, int factor) {
    Trama trama;
    char data[245];
    struct timeval timeout;

    off_t total_enviat = 0;
    snprintf(data, sizeof(data), "%s&%u&%s&%d", file_name, file_size, md5sum, factor);
    inicialitzar_trama(&trama, 0x03, data);

    // Enviar metadades del fitxer al Worker
    if (write(worker_fd, &trama, sizeof(Trama)) < 0) {
        write_message(STDOUT_FILENO, "Error enviant la trama de informació del  fitxer al worker.\n");
        enviar_trama_desconnexio_worker();
        return -1;
    }

    // Esperar CON_OK o CON_KO del Worker
    Trama resposta_worker;

    // Llegir la capçalera primer
    if (safe_read(worker_fd, &resposta_worker, sizeof(Trama)) < 0) {
        write_message(STDOUT_FILENO, "[ERROR] No s'ha pogut llegir la resposta del Worker\n");
        close(worker_fd);
        return -1;
    }


    if (resposta_worker.type != 0x03) {
        write_message(STDOUT_FILENO, "[ERROR] Trama inesperada del Worker\n");
        close(worker_fd);
        return -1;
    }

    if (resposta_worker.data_length == 0) {
        write_message(STDOUT_FILENO, "[OK] Connexió amb el Worker validada.\n");
    } else if (strncmp(resposta_worker.data, "CON_KO", resposta_worker.data_length) == 0) {
        write_message(STDOUT_FILENO, "[ERROR] El Worker ha rebutjat la connexió.\n");
        close(worker_fd);
        return -1;
    } else {
        write_message(STDOUT_FILENO, "[ERROR] Resposta inesperada del Worker.\n");
        close(worker_fd);
        return -1;
    }

    int file_fd = open(file_name, O_RDONLY);
    if (file_fd < 0) {
        enviar_trama_desconnexio_worker();
        return -1;
    }

    char buffer[245];
    ssize_t bytes_read;
    off_t position = 0; // Posició actual del fitxer

    while (total_enviat < file_size) {
        size_t chunk_size = (size_t)(file_size - total_enviat) < sizeof(buffer) ? (size_t)(file_size - total_enviat) : sizeof(buffer);
        bytes_read = read(file_fd, buffer, chunk_size);
        if (bytes_read <= 0) {
            break;
        }
    
        memset(&trama, 0, sizeof(trama));
        trama.type = 0x05;
        trama.data_length = bytes_read;
        memcpy(trama.data, buffer, bytes_read);
        trama.timestamp = (uint32_t)time(NULL);
        trama.checksum = 0;
        trama.checksum = generate_checksum((uint8_t *)&trama, TRAMA_SIZE - 2);
    
        if (write(worker_fd, &trama, sizeof(Trama)) < 0) {
            if (errno == ECONNRESET || errno == EPIPE) {
                write_message(STDOUT_FILENO, "[FLECK] Worker desconnectat durant l'enviament.\n");
                enviar_trama_desconnexio_worker();
    
                // Reconnectar a un nou Worker
                char new_worker_ip[16];
                int new_worker_port;
                if (solicitar_nou_worker("media", file_name, new_worker_ip, &new_worker_port) < 0) {
                    close(file_fd);
                    return -1;
                }
    
                worker_fd = connectar_a_server(new_worker_ip, new_worker_port);
                if (worker_fd < 0) {
                    close(file_fd);
                    return -1;
                }
    
                if (validar_connexio_worker(worker_fd) < 0) {
                    close(file_fd);
                    return -1;
                }
    
                lseek(file_fd, position, SEEK_SET); // Continuar des d'on ens havíem quedat
                continue;
            } else {
                close(file_fd);
                enviar_trama_desconnexio_worker();
                return -1;
            }
        }
    
        total_enviat += bytes_read;
        position += bytes_read;
        int percent = (int)((total_enviat * 100) / file_size);
        update_progress(file_name, percent);

    }

    // Eliminar timeout per esperar indefinidament CHECK_OK
    timeout.tv_sec = 0;
    timeout.tv_usec = 0;
    setsockopt(worker_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

    Trama resposta_md5;
    if (safe_read(worker_fd, &resposta_md5, sizeof(Trama)) < 0) {
        write_message(STDOUT_FILENO, "[FLECK] Error llegint confirmació del Worker.\n");
        enviar_trama_desconnexio_worker();
        return -1;
    }

    if (!validar_trama(&resposta_md5)) {
        write_message(STDOUT_FILENO, "[FLECK] Trama de confirmació no vàlida.\n");
        enviar_trama_desconnexio_worker();
        return -1;
    }

    if (resposta_md5.type != 0x06) {
        enviar_trama_desconnexio_worker();
        return -1;
    } 

    if (strncmp(resposta_md5.data, "CHECK_OK", resposta_md5.data_length) != 0) {
        write_message(STDOUT_FILENO, "[FLECK] El Worker ha indicat un error de recepció del fitxer.\n");
        enviar_trama_desconnexio_worker();
        return -1;
    }

    write_message(STDOUT_FILENO, "[FLECK] Worker ha rebut correctament el fitxer. (CHECK_OK)\n");

    unregister_file(file_name);

    close(file_fd);
    return 0;
}

int rebre_fitxer_worker(const char *output_file, const char *media_type, const char *file_name) {
    Trama trama;
    time_t last_print = time(NULL);
    // Desactivar timeout per esperar indefinidament la trama 0x04
    struct timeval timeout;
    timeout.tv_sec = 0;
    timeout.tv_usec = 0;
    setsockopt(worker_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));



    // 1. Rebre trama 0x04 amb FileSize & MD5SUM
    while (1) {
        ssize_t bytes_read = safe_read(worker_fd, &trama, sizeof(trama));
        write_message(STDOUT_FILENO, "[DEBUG] Esperant trama 0x04 del Worker...\n");
        if (bytes_read < 0) {
            if (errno == EINTR) {
                continue;  // Senyal, tornar a intentar
            } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
                time_t now = time(NULL);
                if (now - last_print >= 10) {
                    last_print = now;
                    write_message(STDOUT_FILENO, "[DEBUG] Encara esperant trama 0x04 del Worker...\n");
                }
                continue;  // Tornar a esperar.
            } else {
                write_message(STDERR_FILENO, "[ERROR] Error llegint del socket del Worker\n");
                enviar_trama_desconnexio_worker();
                return ERROR_READ;
            }
        }

        if (bytes_read == sizeof(trama)){ 
            write_message(STDOUT_FILENO, "[DEBUG] Trama 0x04 rebuda del Worker.\n");
            break;
        }  // Correcte, ja tenim la trama 0x04.
        

        if (bytes_read == 0 || errno == ECONNRESET || errno == EPIPE) {
            write_message(STDOUT_FILENO, "[FLECK] Worker desconnectat inesperadament.\n");
            enviar_trama_desconnexio_worker();

            char new_worker_ip[16];
            int new_worker_port;
            write_message(STDOUT_FILENO, "[FLECK] Sol·licitant un nou Worker...\n");
            if (solicitar_nou_worker(media_type, file_name, new_worker_ip, &new_worker_port) < 0) {
                return ERROR_NEW_WORKER;
            }

            worker_fd = connectar_a_server(new_worker_ip, new_worker_port);
            if (worker_fd < 0 || validar_connexio_worker() < 0) {
                return ERROR_CONNECT_NEW;
            }
            write_message(STDOUT_FILENO, "[FLECK] Nou Worker connectat. Continuant espera de la trama 0x04...\n");
            continue;  // Tornar a esperar indefinidament.
        }

        return ERROR_READ;
    }



    if (trama.type != 0x04) {
        return ERROR_READ;
    }

    char *sep = strchr(trama.data, '&');
    if (!sep) {
        write(STDERR_FILENO, "[ERROR] Format incorrecte a la trama 0x04\n", 42);
        return ERROR_READ;
    }

    char file_size_str[32];
    char md5sum_expected[MD5SUM_LENGTH];
    size_t len_file_size = sep - trama.data;
    strncpy(file_size_str, trama.data, len_file_size);
    file_size_str[len_file_size] = '\0';

    strncpy(md5sum_expected, sep + 1, MD5SUM_LENGTH - 1);
    md5sum_expected[MD5SUM_LENGTH - 1] = '\0';

    uint32_t file_size = strtoul(file_size_str, NULL, 10);

    // 2. Obrir fitxer de sortida
    int output_fd = open(output_file, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (output_fd < 0) {
        enviar_trama_desconnexio_worker();
        return ERROR_OUTPUT_FILE;
    }

    uint32_t total_rebut = 0;

    // 3. Rebre trames 0x05 fins completar file_size
    while (total_rebut < file_size) {
        ssize_t bytes_read = safe_read(worker_fd, &trama, sizeof(trama));
        write_message(STDOUT_FILENO, "[DEBUG] Esperant trama 0x05 del Worker...\n");
        if (bytes_read != sizeof(trama)) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) continue;

            if (errno == ECONNRESET || errno == EPIPE) {
                write_message(STDERR_FILENO, "[FLECK] Worker desconnectat inesperadament durant la recepció del fitxer.\n");
                enviar_trama_desconnexio_worker();
                write_message(STDOUT_FILENO, "[FLECK] Sol·licitant un nou Worker...\n");
                char new_worker_ip[16];
                int new_worker_port;
                if (solicitar_nou_worker(media_type, file_name, new_worker_ip, &new_worker_port) < 0) {
                    close(output_fd);
                    return ERROR_NEW_WORKER;
                }

                worker_fd = connectar_a_server(new_worker_ip, new_worker_port);
                if (worker_fd < 0 || validar_connexio_worker() < 0) {
                    close(output_fd);
                    return ERROR_CONNECT_NEW;
                }
                write_message(STDOUT_FILENO, "[FLECK] Nou Worker connectat. Continuant recepció...\n");
                continue;
            }

            enviar_trama_desconnexio_worker();
            close(output_fd);
            return ERROR_READ;
        }

        

        if (write(output_fd, trama.data, trama.data_length) != trama.data_length) {
            enviar_trama_desconnexio_worker();
            close(output_fd);
            return ERROR_READ;
        }

        total_rebut += trama.data_length;
        int percent = (int)((total_rebut * 100) / file_size);
        update_progress(file_name, percent);


    }
    write_message(STDOUT_FILENO, "[DEBUG] Fitxer rebut completament del Worker.\n");

    close(output_fd);

    // 4. Comprovar MD5SUM del fitxer rebut
    char md5sum_calculat[MD5SUM_LENGTH];
    uint32_t dummy_size;
    if (calcular_md5(output_file, md5sum_calculat, &dummy_size) < 0) {
        return ERROR_READ;
    }


    // 5. Enviar CHECK_OK o CHECK_KO segons validació
    Trama resposta_trama;
    if (strncmp(md5sum_calculat, md5sum_expected, MD5SUM_LENGTH) == 0) {
        inicialitzar_trama(&resposta_trama, 0x06, "CHECK_OK");
        write_message(STDOUT_FILENO, "[FLECK] MD5SUM correcte, enviant CHECK_OK al Worker.\n");
    } else {
        inicialitzar_trama(&resposta_trama, 0x06, "CHECK_KO");
        write_message(STDOUT_FILENO, "[FLECK] MD5SUM incorrecte, enviant CHECK_KO al Worker.\n");
    }
    unregister_file(file_name);


    if (write(worker_fd, &resposta_trama, sizeof(resposta_trama)) != sizeof(resposta_trama)) {
        return ERROR_READ;
    }

    return 0;
}


/**
 * Gestiona la distorsió d'un fitxer amb el worker.
 * @param worker_ip IP del worker assignat.
 * @param worker_port Port del worker assignat.
 * @param file_name Nom del fitxer a distorsionar.
 * @param factor Factor de distorsió.
 * @return 0 si la distorsió és correcta, -1 si hi ha un error.
 */
int gestionar_distorsio_worker(const char *worker_ip, int worker_port, const char *file_name, int factor) {
    struct timeval timeout;

    if (!worker_ip || worker_port <= 0 || !file_name || factor <= 0) {
        write_message(STDOUT_FILENO, "Error: Paràmetres de distorsió no vàlids\n");
        return -1;
    }
    
    write_message(STDOUT_FILENO, "Connectant amb el worker: ");
    write_message(STDOUT_FILENO, worker_ip);
    write_message(STDOUT_FILENO, "\n");
    

    worker_fd = connectar_a_server(worker_ip, worker_port);

    
    if (worker_fd < 0) {
        write_message(STDOUT_FILENO, "Error connectant amb el worker.\n");
        enviar_trama_desconnexio_worker();
        return -1;
    }
    
    uint32_t file_size;
    char md5sum[33]; 
    if (calcular_md5(file_name, md5sum, &file_size) < 0) {
        write_message(STDOUT_FILENO, "Error calculant MD5SUM.\n");
        enviar_trama_desconnexio_worker();
        return -1;
    }
    write_message(STDOUT_FILENO, "Distorsion started!\n");


    if (enviar_fitxer_worker(file_name, file_size, md5sum, factor) < 0) {
        enviar_trama_desconnexio_worker();
        return -1;
    }

    //Reactivar timeout per lectures futures (ex. 5 segons)
    timeout.tv_sec = TIMEOUT_SECONDS;
    timeout.tv_usec = 0;
    setsockopt(worker_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));


    char output_file[256];
    snprintf(output_file, sizeof(output_file), "distorted_%s", file_name);
    if (rebre_fitxer_worker(output_file, "media", file_name) < 0) {
        enviar_trama_desconnexio_worker();
        return -1;
    }

    enviar_trama_desconnexio_worker();  
    return 0;
}


void *thread_distorsio_worker(void *arg) {
    DistorsioArgs *args = (DistorsioArgs *)arg;

    if (gestionar_distorsio_worker(args->worker_ip, args->worker_port, args->file_name, args->factor) == 0) {
        write_message(STDOUT_FILENO, "Distorsió completed successfuly\n");
    } else {
        write_message(STDOUT_FILENO, "Error during distortion :(\n");
    }
    write_message(STDOUT_FILENO, "\n$ ");

    free(arg);
    return NULL;
}



/**
 * Envia una petició de distorsió a Gotham.
 * @param media_type Tipus de media (per exemple: "media", "text").
 * @param file Nom del fitxer a distorsionar.
 * @param factor Factor de distorsió.
 * @return 0 si la petició s'envia correctament, -1 si hi ha un error.
 */
int enviar_peticio_distorsio(const char *media_type, const char *file, int factor) {
    if (!media_type || !file) {
        write_message(STDOUT_FILENO, "Error: Tipus de media o nom de fitxer no vàlid.\n");
        return -1;
    }

    Trama trama;
    char data[245];
    snprintf(data, sizeof(data), "%s&%s", media_type, file);
    inicialitzar_trama(&trama, 0x10, data);


    if (write(sock_fd, &trama, sizeof(trama)) != sizeof(trama)) {
        write_message(STDOUT_FILENO, "Error enviant la petició de distorsió.\n");
        return -1;
    }

    Trama resposta;
    if (safe_read(sock_fd, &resposta, sizeof(Trama)) < 0) {
        write_message(STDOUT_FILENO, "Error rebent la resposta de Gotham.\n");
        return -1;
    }

    if (!validar_trama(&resposta)) {
        write_message(STDOUT_FILENO, "Resposta de Gotham invàlida.\n");
        return -1;
    }

    if (resposta.type == 0x10 && resposta.data_length > 0) {
        if (strcmp(resposta.data, "DISTORT_KO") == 0) {
            write_message(STDOUT_FILENO, "Error: No hi ha workers disponibles\n");
            return -1;
        } else if (strcmp(resposta.data, "MEDIA_KO") == 0) {
            write_message(STDOUT_FILENO, "Error: Tipus de media incorrecte\n");
            return -1;
        } else {
            char worker_ip[16];
            int worker_port;
            sscanf(resposta.data, "%15[^&]&%d", worker_ip, &worker_port);

            DistorsioArgs *args = malloc(sizeof(DistorsioArgs));
            if (!args) {
                write_message(STDOUT_FILENO, "Error assignant memòria pel thread\n");
                return -1;
            }
            strncpy(args->worker_ip, worker_ip, sizeof(args->worker_ip));
            args->worker_port = worker_port;
            strncpy(args->file_name, file, sizeof(args->file_name));
            args->factor = factor;

            pthread_t tid;
            if (pthread_create(&tid, NULL, thread_distorsio_worker, args) != 0) {
                write_message(STDOUT_FILENO, "Error creant el thread per la distorsió\n");
                free(args);
                return -1;
            }
            pthread_detach(tid);
            return 0;
        }
    }

    write_message(STDOUT_FILENO, "Resposta inesperada de Gotham\n");
    return -1;
}

void netejar_distorsions_acabades() {
    pthread_mutex_lock(&status_mutex);
    DistorsioStatus **curr = &head;
    while (*curr) {
        if ((*curr)->progress == 100) {
            DistorsioStatus *a_eliminar = *curr;
            *curr = (*curr)->next;
            free(a_eliminar);
        } else {
            curr = &((*curr)->next);
        }
    }
    pthread_mutex_unlock(&status_mutex);
    write_message(STDOUT_FILENO, "Distorsions acabades eliminades\n");
}



/**
 * Divideix i delega gestions de comandes en funcions més petites.
 * Gestió de la comanda DISTORT.
 * @param command Comanda completa introduïda per l'usuari.
 */
void gestionar_comanda_distort(char *command) {
    if (!connected_to_gotham) {
        write_message(STDOUT_FILENO, "No estas connectat a Gotham\n");
        return;
    }

    char *file = strtok(command + 8, " ");
    char *factor_str = strtok(NULL, " ");

    if (!file || !factor_str || !isdigit(factor_str[0])) {
        write_message(STDOUT_FILENO, "ERROR: DISTORT <file.xxx> <factor>\n");
        return;
    }

    int factor = atoi(factor_str);
    if (factor <= 0) {
        write_message(STDOUT_FILENO, "ERROR: Factor invalid\n");
        return;
    }

    char *ext = strrchr(file, '.');
    if (!ext) {
        write_message(STDOUT_FILENO, "ERROR: El fitxer no té extensió\n");
        return;
    }

    char *media_extensions[] = {".wav", ".mp3", ".jpg", ".png"};
    char *text_extensions[] = {".txt"};

    char *file_type = NULL;
    
    for (size_t i = 0; i < sizeof(media_extensions) / sizeof(media_extensions[0]); i++) {
        if (strcasecmp(ext, media_extensions[i]) == 0) {
            file_type = "media";
            break;
        }
    }

    if (!file_type) {
        for (size_t i = 0; i < sizeof(text_extensions) / sizeof(text_extensions[0]); i++) {
            if (strcasecmp(ext, text_extensions[i]) == 0) {
                file_type = "text";
                break;
            }
        }
    }

    if (!file_type) {
        write_message(STDOUT_FILENO, "ERROR: Tipus de fitxer no suportat\n");
        return;
    }

    if (access(file, R_OK) != 0) {
        return;
    } 
    enviar_peticio_distorsio(file_type, file, factor);
}



/**
 * Gestió principal de comandes introduïdes per l'usuari.
 */
void gestionar_comandes_fleck() {
    char *command = NULL;
    size_t len = BUF_INICIAL;

    command = malloc(len);
    if (!command) {
        return;
    }

    while (1) {
        write_message(STDOUT_FILENO, "$ ");
        ssize_t leido = read(STDIN_FILENO, command, len - 1);
        if (leido <= 0) break;

        command[leido] = '\0';
        trim_whitespace(command);

        char command_upper[BUF_INICIAL];
        strncpy(command_upper, command, sizeof(command_upper));
        command_upper[sizeof(command_upper) - 1] = '\0';

        for (size_t i = 0; command_upper[i]; i++) {
            command_upper[i] = toupper((unsigned char)command_upper[i]);
        }

        if (strcmp(command_upper, "CONNECT") == 0) {
            sock_fd = connectar_a_server(config.base.ip, config.base.port);
            if (sock_fd >= 0) {
                connected_to_gotham = 1;
                establir_connexio_gotham();
            }        
        } else if (strcmp(command_upper, "LIST MEDIA") == 0) {
            const char *extensions[] = {".wav", ".mp3", ".jpg", ".png"};
            gestionar_comanda_list(extensions, 4);
        } else if (strcmp(command_upper, "LIST TEXT") == 0) {
            const char *extensions[] = {".txt"};
            gestionar_comanda_list(extensions, 1);
        } else if (strncasecmp(command, "DISTORT", 7) == 0) {
            char *cmd_copy = strdup(command);
            if (!cmd_copy) {
                write_message(STDOUT_FILENO, "Error assignant memòria per la comanda\n");
            } else {
                pthread_t tid;
                if (pthread_create(&tid, NULL, gestionar_comanda_distort_thread, cmd_copy) != 0) {
                    write_message(STDOUT_FILENO, "Error creant el thread per la comanda DISTORT\n");
                    free(cmd_copy);
                } else {
                    pthread_detach(tid);
                }
            }
        } else if (strcmp(command_upper, "LOGOUT") == 0) {
            if (connected_to_gotham) {
                Trama trama;
                inicialitzar_trama(&trama, 0x07, config.username);
                write(sock_fd, &trama, sizeof(trama));
                close(sock_fd);
                sock_fd = -1;
                connected_to_gotham = 0;
                write_message(STDOUT_FILENO, "Thanks for using Mr. J System, see you soon, chaos lover :)\n");
            } else {
                write_message(STDOUT_FILENO, "Not connected to Mr. J System. Exiting...\n");
            }
        } else if (strcmp(command_upper, "CHECK STATUS") == 0) {
            print_all_progress();
        } else if (strcmp(command_upper, "CLEAR ALL") == 0) {
            netejar_distorsions_acabades();
        } else {
            write_message(STDOUT_FILENO, "ERROR: Please input a valid command.\n");
        }
    }

    free(command);
    if (sock_fd >= 0) {
        close(sock_fd);
    }
}



/**
 * Punt d'entrada principal del programa Fleck.
 * @param argc Nombre d'arguments.
 * @param argv Array d'arguments.
 * @return Retorna 0 si l'execució és correcta, 1 en cas d'error.
 */
int main(int argc, char *argv[]) {
    if (argc != 2) {
        write_message(STDOUT_FILENO, "Expected: ./fleck <config file>\n");
        return 1;
    }

    int config_fd = open(argv[1], O_RDONLY);
    if (config_fd < 0) {
        return 1;
    }

    // Carregar configuració a `config`
    if (parse_fleck_config(config_fd, &config) < 0) {
        write_message(STDOUT_FILENO, "ERROR: Couldn't parse the config. file\n");
        close(config_fd);
        return 1;
    }

    if (chdir(config.work_dir) != 0) {
        return 1;
    }
    

    
    signal(SIGINT, ctrlc_handler);

    connected_to_gotham = 0;
    sock_fd = -1;  

    // Iniciar gestió de comandes
    gestionar_comandes_fleck();

    return 0;
}