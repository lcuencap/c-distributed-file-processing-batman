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
#include <libgen.h>
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

pthread_mutex_t fd_mutex = PTHREAD_MUTEX_INITIALIZER;
int harley_fd = -1;
int enigma_fd = -1;

static DistorsioStatus *head = NULL;
static pthread_mutex_t status_mutex = PTHREAD_MUTEX_INITIALIZER;




/**
 * Extreu la IP i el port d'una cadena amb format "ip&port".
 * @param input Cadena d'entrada amb el format "ip&port".
 * @param ip_buffer Buffer de sortida on s'escriurà la IP (mínim 16 bytes).
 * @param port Punter on s'emmagatzemarà el port.
 * @return 0 si tot ha anat bé, -1 si hi ha un error de format.
 */
int parse_ip_port(const char *input, char *ip_buffer, int *port) {
    if (!input || !ip_buffer || !port) return -1;

    const char *sep = strchr(input, '&');
    if (!sep) return -1;

    size_t ip_len = sep - input;
    if (ip_len >= 16) ip_len = 15;  // Evitar desbordament

    strncpy(ip_buffer, input, ip_len);
    ip_buffer[ip_len] = '\0';

    *port = atoi(sep + 1);
    if (*port <= 0) return -1;

    return 0;
}



/**
 * Indica si hi ha una distorsió activa per al tipus de media especificat.
 * @param media_type Tipus de media ("media", "text").
 * @return 1 si hi ha una distorsió activa, 0 si no.
 */
int existeix_distorsio_activa(const char *media_type) {
    pthread_mutex_lock(&fd_mutex);
    int activa = 0;
    if (strcmp(media_type, "media") == 0)
        activa = (harley_fd != -1);
    else if (strcmp(media_type, "text") == 0)
        activa = (enigma_fd != -1);
    pthread_mutex_unlock(&fd_mutex);
    return activa;
}


/**
 * Marca un fitxer com a error en el processat.
 * @param file_name Nom del fitxer processat.
 */
void mark_error_status(const char *file_name) {
    pthread_mutex_lock(&status_mutex);
    DistorsioStatus *curr = head;
    while (curr) {
        if (strcmp(curr->file_name, file_name) == 0) {
            curr->error = 1;
            break;
        }
        curr = curr->next;
    }
    pthread_mutex_unlock(&status_mutex);
}

/**
 * Registra un nou fitxer per processar.
 * @param file_name Nom del fitxer a processar.
 */
void register_new_file(const char *file_name) {
    pthread_mutex_lock(&status_mutex);
    DistorsioStatus *new_status = malloc(sizeof(DistorsioStatus));
    if (new_status) {
        strncpy(new_status->file_name, file_name, sizeof(new_status->file_name));
        new_status->file_name[sizeof(new_status->file_name) - 1] = '\0';
        new_status->progress = 0;
        new_status->error = 0; 
        new_status->next = head;
        head = new_status;
    }
    pthread_mutex_unlock(&status_mutex);
}

/**
 * Sol·licita a Gotham un nou worker per processar un fitxer.
 * @param media_type Tipus de media (media, text).
 * @param file_name Nom del fitxer a processar. 
 * @param worker_ip Punter a una cadena on es guardarà la IP del nou worker.
 * @param worker_port Punter a un enter on es guardarà el port del nou worker.
 * @return Retorna 0 si tot va bé, -1 en cas d'error.
 */
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
    write_message(STDOUT_FILENO, "Trama 0x11 enviada\n");
    write_message(STDOUT_FILENO, "Esperant resposta de Gotham...\n");

    // Eliminar timeout temporalment per esperar la resposta
    struct timeval timeout;
    timeout.tv_sec = 0;
    timeout.tv_usec = 0;
    setsockopt(sock_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    
    // Llegir la resposta de Gotham
    Trama resposta;
    if (safe_read(sock_fd, &resposta, sizeof(Trama)) < 0) {
        write_message(STDOUT_FILENO, "Error rebent resposta de Gotham.\n");
        return -1;
    }
    write_message(STDOUT_FILENO, "Resposta rebuda de Gotham.\n");

    if (!validar_trama(&resposta)) {
        write_message(STDOUT_FILENO, "Resposta de Gotham invàlida.\n");
        return -1;
    }
    write_message(STDOUT_FILENO, "Resposta rebuda vàlida.\n");

    if (resposta.type != 0x11) {
        write_message(STDOUT_FILENO, "Resposta de Gotham no és del tipus esperat.\n");
        return -1;
    }
    write_message(STDOUT_FILENO, "Resposta rebuda de tipus 0x11.\n");


    if (strcmp(resposta.data, "DISTORT_KO") == 0) {
        write_message(STDOUT_FILENO, "Error: No hi ha workers disponibles.\n");
        return -1;
    } else if (strcmp(resposta.data, "MEDIA_KO") == 0) {
        write_message(STDOUT_FILENO, "Error: Tipus de media incorrecte.\n");
        return -1;
    } else {
        if (parse_ip_port(resposta.data, worker_ip, worker_port) < 0) {
            write_message(STDOUT_FILENO, "Error: Format de resposta de Gotham incorrecte.\n");
            return -1;
        }
        return 0;
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
    int sock_fd = -1;

    while (retries < MAX_RETRIES) {
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
 * Valida la connexió amb el worker esperant la trama de validació 0x03.
 * @return 0 si la connexió és vàlida, -1 si hi ha un error.
 */
int esperar_resposta_connexio(int worker_fd) {
    Trama resposta;

    if (safe_read(worker_fd, &resposta, sizeof(Trama)) < 0) {
        write_message(STDOUT_FILENO, "[ERROR] Could not read answer from worker.\n");
        close(worker_fd);
        return -1;
    }

    if (resposta.type == 0x03 && resposta.data_length == 0) {
        write_message(STDOUT_FILENO, "[OK] Connected to worker.\n");
        return 0;
    }

    if (resposta.type == 0x03 && strcmp(resposta.data, "CON_KO") == 0) {
        write_message(STDOUT_FILENO, "[KO] Connection was refused by worker.\n");
        return -1;
    }

    write_message(STDOUT_FILENO, "[ERROR] Could not validate connection to worker.\n");
    return -1;
}

/**
 * Actualitza l'estat del progrés d'una distorsió.
 * Si el fitxer no està registrat, no fa res.
 * @param file_name Nom del fitxer a actualitzar.
 * @param estat Estat de la distorsió (ENVIANT, ESPERANT, REBENT, FINALITZAT).
 * @param mida_original Mida original del fitxer.
 * @param mida_distorsionat Mida del fitxer distorsionat.
 * @param bytes_enviats Bytes enviats fins ara.
 * @param bytes_rebuts Bytes rebuts fins ara.
 */
void update_progress_status(const char *file_name, EstatFase estat, uint32_t mida_original, uint32_t mida_distorsionat, uint32_t bytes_enviats, uint32_t bytes_rebuts) {
    pthread_mutex_lock(&status_mutex);
    DistorsioStatus *curr = head;
    while (curr) {
        if (strcmp(curr->file_name, file_name) == 0) {
            curr->estat = estat;
            curr->mida_original = mida_original;
            curr->mida_distorsionat = mida_distorsionat;
            curr->bytes_enviats = bytes_enviats;
            curr->bytes_rebuts = bytes_rebuts;

            int percent = 0;
            if (estat == ENVIANT && mida_original > 0) {
                percent = (bytes_enviats * 40) / mida_original;
            } else if (estat == ESPERANT) {
                percent = 40;
            } else if (estat == REBENT && mida_distorsionat > 0) {
                percent = 60 + (bytes_rebuts * 40) / mida_distorsionat;
            } else if (estat == FINALITZAT) {
                percent = 100;
            }
            curr->progress = percent;
            break;
        }
        curr = curr->next;
    }
    pthread_mutex_unlock(&status_mutex);
}

/**
 * Desregistra un fitxer de la llista d'estats de distorsió.
 * Si el fitxer no existeix, no fa res.
 * @param file_name Nom del fitxer a desregistrar.
 */
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

/**
 * Imprimeix l'estat de totes les distorsions en curs o finalitzades.
 * Mostra el nom del fitxer, el progrés i l'estat de la distorsió.
 */
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
        if (curr->error) {
            snprintf(buffer, sizeof(buffer), "\033[0;31m%s %d%% |ABORTED|\033[0m\n", curr->file_name, curr->progress);
            write_message(STDOUT_FILENO, buffer);
            curr = curr->next;
        } else {
            int offset = snprintf(buffer, sizeof(buffer), "%s %d%% |", curr->file_name, curr->progress);
            int bars = curr->progress / 5;
            for (int i = 0; i < bars && offset < (int)(sizeof(buffer) - 2); ++i) {
                buffer[offset++] = '=';
            }
            if (offset < (int)(sizeof(buffer) - 2)) {
                buffer[offset++] = '|';
            }
            buffer[offset++] = '\n';
            buffer[offset] = '\0';
            write_message(STDOUT_FILENO, buffer);
            curr = curr->next;
        }
    }

    pthread_mutex_unlock(&status_mutex);
}

/**
 * Envia una trama de desconnexió al worker de tipus 0x07 i tanca el socket.
 * Aquesta funció s'ha d'invocar quan Fleck es vol desconnectar del worker.
 */
void enviar_trama_desconnexio_worker(int worker_fd) {
    if (worker_fd >= 0) {
        Trama trama;
        inicialitzar_trama(&trama, 0x07, config.username);
        write(worker_fd, &trama, sizeof(trama));
        close(worker_fd);

        if (worker_fd == harley_fd) {
            harley_fd = -1;
        } else if (worker_fd == enigma_fd) {
            enigma_fd = -1;
        }
    }
}

/**
 * Envia una trama de desconnexió a Gotham de tipus 0x07 i tanca el socket.
 * Aquesta funció s'ha d'invocar quan Fleck es vol desconnectar de Gotham.
 */
void enviar_trama_desconnexio_gotham() {
    if (sock_fd >= 0) {
        Trama trama;
        inicialitzar_trama(&trama, 0x07, config.username);
        write(sock_fd, &trama, sizeof(trama));
        close(sock_fd);
        sock_fd = -1;
        connected_to_gotham = 0;
        write_message(STDOUT_FILENO, "Disconnected from Gotham successfully.\n");
    }
}


/**
 * Gestió de la senyal Ctrl+C per tancar correctament Fleck.
 * Envia una trama de desconnexió a Gotham i al worker (si escau) i allibera recursos.
 * @param sig Senyal rebuda (no utilitzada).
 */
void parar_fleck(int sig) {
    (void)sig;  

    if (connected_to_gotham) {
        enviar_trama_desconnexio_gotham();  
    } 

    if (harley_fd >= 0) {
        enviar_trama_desconnexio_worker(harley_fd);
    }
    if (enigma_fd >= 0) {
        enviar_trama_desconnexio_worker(enigma_fd);
    }

    pthread_mutex_lock(&status_mutex);
    DistorsioStatus *curr = head;
    while (curr) {
        DistorsioStatus *to_free = curr;
        curr = curr->next;
        free(to_free);
    }
    head = NULL;
    pthread_mutex_unlock(&status_mutex);
    
    write_message(STDOUT_FILENO, "Thanks for using Mr. J System, see you soon, chaos lover :)\n");

    exit(0);
}


/**
 * Envia un heartbeat a Gotham per mantenir la connexió viva.
 * @param fd Descriptor del socket de Gotham.
 */
void *heartbeat_gotham(void *arg) {
    (void)arg;
    while (1) {
        sleep(5);
        
        Trama hb;
        inicialitzar_trama(&hb, 0x12, "");

        if (write(sock_fd, &hb, sizeof(hb)) != sizeof(hb)) {
            write_message(STDOUT_FILENO, "X Gotham is down X\n");
            parar_fleck(0);
        }
    }
    return NULL;
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
        write_message(STDOUT_FILENO, "Error recieving answer from server.\n");
        return;
    }

    if (!validar_trama(&resposta)) {
        write_message(STDOUT_FILENO, "Couldn not validate server's answer.\n");
        return;
    }

    if (resposta.type == 0x01 && resposta.data_length == 0) {
        write_message(STDOUT_FILENO, "Connected to Gotham server.\n");
        pthread_t heartbeat_thread;
        pthread_create(&heartbeat_thread, NULL, heartbeat_gotham, NULL);
        pthread_detach(heartbeat_thread);
    } else if (strcmp(resposta.data, "CON_KO") == 0) {
        write_message(STDOUT_FILENO, "Connection refused by Gotham server.\n");
    }
}

/**
 * Llista els fitxers disponibles a la carpeta actual que coincideixin amb les extensions de media especificades.
 * @param media_types Array de punters a cadenes de caràcters amb les extensions de fitxer a buscar.
 * @param count Nombre d'extensions a buscar.
 */
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
                    int len = snprintf(buffer, sizeof(buffer), "\t %d. %s\n", index++, file_name);
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

/**
 * Valida la connexió amb el Worker enviant trama 0x03 i esperant la resposta del worker (0x03).
 * @param worker_fd Descriptor del socket del worker.
 * @param file_name Nom del fitxer a enviar.
 * @param file_size Mida del fitxer en bytes.
 * @param md5sum MD5SUM del fitxer per verificar la integritat.
 * @param factor Factor de distorsió.
 * @return 0 si la connexió és vàlida, -1 si hi ha un error.
 */
int validar_connexio_distort_worker(int worker_fd, const char *file_name, uint32_t file_size, const char *md5sum, int factor) {
    Trama trama;
    char data[245];
    snprintf(data, sizeof(data), "%s&%u&%s&%d", file_name, file_size, md5sum, factor);
    inicialitzar_trama(&trama, 0x03, data);

    // Enviar metadades del fitxer al Worker
    if (write(worker_fd, &trama, sizeof(Trama)) < 0) {
        write_message(STDOUT_FILENO, "Error enviant la trama de informació del  fitxer al worker.\n");
        enviar_trama_desconnexio_worker(worker_fd);
        return -1;
    }


    if (esperar_resposta_connexio(worker_fd) != 0) {
        write_message(STDOUT_FILENO, "Error validant la connexió amb el Worker.\n");
        enviar_trama_desconnexio_worker(worker_fd);
        return -1;
    }

    return 0;
}


/**
 * Gestiona la caiguda del Worker a qualsevol fase de la distorsió.
 * @param fase La fase actual ("ENVIANT", "ESPERANT_04", "REBENT").
 * @param media_type Tipus de media ("media", "text").
 * @param file_name Nom del fitxer.
 * @param fd_a_tancar Descriptor opcional a tancar (ex: file_fd o output_fd). Passa -1 si no n’hi ha.
 * @param file_size
 * @param md5sum
 * @param factor
 * @return 0 si la recuperació ha tingut èxit, <0 si ha fallat.
 */
int gestionar_caiguda_worker(int worker_fd, const char *fase, const char *media_type, const char *file_name, int fd_a_tancar, 
                             uint32_t file_size, const char *md5sum, int factor) {
    char buffer[128];
    snprintf(buffer, sizeof(buffer), "[FLECK] Worker desconnectat inesperadament durant la fase: %s.\n", fase);
    write_message(STDERR_FILENO, buffer);

    if (fd_a_tancar >= 0) {
        close(fd_a_tancar);
        write_message(STDOUT_FILENO, "[FLECK] S'ha tancat el fitxer o descriptor actiu.\n");
    }

    enviar_trama_desconnexio_worker(worker_fd);
    write_message(STDOUT_FILENO, "[FLECK] Sol·licitant un nou Worker...\n");

    char new_worker_ip[16];
    int new_worker_port;
    if (solicitar_nou_worker(media_type, file_name, new_worker_ip, &new_worker_port) < 0) {
        return ERROR_NEW_WORKER;
    }

    int new_worker_fd = connectar_a_server(new_worker_ip, new_worker_port);

    if (new_worker_fd < 0) {
        write_message(STDOUT_FILENO, "[FLECK] Error connectant al nou Worker.\n");
        return ERROR_CONNECT_NEW;
    }


    if (validar_connexio_distort_worker(new_worker_fd, file_name, file_size, md5sum, factor) < 0) {
        write_message(STDOUT_FILENO, "[FLECK] Error validant connexió amb el nou Worker.\n");
        enviar_trama_desconnexio_worker(new_worker_fd);
        return ERROR_VALIDATION;
    }

    write_message(STDOUT_FILENO, "[FLECK] Nou Worker connectat.\n");

    return 0;
}


/**
 * Envia el fitxer al worker.
 * @param media_type Tipus de media ("media", "text").
 * @param file_name Nom del fitxer a enviar.
 * @param file_size Mida del fitxer en bytes.
 * @param md5sum MD5SUM del fitxer per verificar la integritat.
 * @param factor Factor de distorsió.
 * @return 0 si l'enviament és correcte, -1 si hi ha un error.
 */
int enviar_fitxer_worker(int worker_fd, const char *media_type, const char *file_name, uint32_t file_size, const char *md5sum, int factor) {
    Trama trama;
    struct timeval timeout;
    off_t total_enviat = 0;
    
    
    if (validar_connexio_distort_worker(worker_fd, file_name, file_size, md5sum, factor) != 0) {
        write_message(STDOUT_FILENO, "[ERROR] Could not establish connection with worker.\n");
        return -1;
    }


    int file_fd = open(file_name, O_RDONLY);
    if (file_fd < 0) {
        enviar_trama_desconnexio_worker(worker_fd);
        return -1;
    }
    char buffer[245];
    ssize_t bytes_read;
    off_t position = 0; // Posició actual del fitxer
    
    update_progress_status(file_name, ENVIANT, file_size, 0, total_enviat, 0);



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
            write_message(STDOUT_FILENO, "[ERROR] Error sending file information packet to gotham.\n");
            if (errno == ECONNRESET || errno == EPIPE) {
                write_message(STDOUT_FILENO, "[ERROR] Worker disconnected unexpectedly.\n");
                if (gestionar_caiguda_worker(worker_fd, "ENVIANT", media_type, file_name, file_fd, file_size, md5sum, factor) < 0) {
                    write_message(STDOUT_FILENO, "[ERROR] Could not reconnect to new worker. Aborting.\n");
                    return ERROR_WORKER_DOWN;
                } else {
                    write_message(STDOUT_FILENO, "[ERROR] Reconnected to new Worker. Continuing process...\n");
                }
                lseek(file_fd, position, SEEK_SET); // Continuar des d'on ens havíem quedat
                continue;
            } else {
                write_message(STDOUT_FILENO, "[ERROR] Unknown. Disconnecting from Worker.\n");
                close(file_fd);
                enviar_trama_desconnexio_worker(worker_fd);
                return -1;
            }
        }
    
        total_enviat += bytes_read;
        position += bytes_read;
        update_progress_status(file_name, ENVIANT, file_size, 0, total_enviat, 0);
    }

    // Eliminar timeout per esperar indefinidament CHECK_OK
    timeout.tv_sec = 0;
    timeout.tv_usec = 0;
    setsockopt(worker_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

    Trama resposta_md5;
    if (safe_read(worker_fd, &resposta_md5, sizeof(Trama)) < 0) {
        write_message(STDOUT_FILENO, "[ERROR] Could not read Worker reception confirmation. Disconnecting.\n");
        enviar_trama_desconnexio_worker(worker_fd);
        return -1;
    }

    if (!validar_trama(&resposta_md5)) {
        write_message(STDOUT_FILENO, "[ERROR] Invalid answer from Worker. Disconnecting.\n");
        enviar_trama_desconnexio_worker(worker_fd);
        return -1;
    }

    if (resposta_md5.type != 0x06) {
        enviar_trama_desconnexio_worker(worker_fd);
        return -1;
    } 

    if (resposta_md5.data_length != 8 || strncmp(resposta_md5.data, "CHECK_OK", 8) != 0) {
        write_message(STDOUT_FILENO, "[ERROR] Worker has indicated a problem recieving.\n");
        enviar_trama_desconnexio_worker(worker_fd);
        return -1;
    }
    
    update_progress_status(file_name, ESPERANT, file_size, 0, total_enviat, 0);

    close(file_fd);
    return 0;
}

/**
 * Rebrà el fitxer del Worker i el desarà al fitxer de sortida especificat.
 * @param worker_fd Descriptor del socket del worker.
 * @param output_file Fitxer de sortida on es desarà el contingut rebut.
 * @param media_type Tipus de media ("media", "text").
 * @param file_name Nom del fitxer que s'està rebent.
 * @param file_size Mida del fitxer a rebre.
 * @param MD5SUM esperat del fitxer per verificar la integritat.
 * @param factor Factor de distorsió.
 * @return 0 si la recepció és correcta, <0 si hi ha un error.
 */
int rebre_fitxer_worker(int worker_fd, const char *output_file, const char *media_type, const char *file_name, uint32_t file_size, const char *md5sum, int factor) {
    Trama trama;
    time_t last_print = time(NULL);
    ssize_t bytes_read;
    // Desactivar timeout per esperar indefinidament la trama 0x04
    struct timeval timeout;
    timeout.tv_sec = 0;
    timeout.tv_usec = 0;
    setsockopt(worker_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));



    // 1. Rebre trama 0x04 amb FileSize & MD5SUM
    while (1) {
        bytes_read = safe_read(worker_fd, &trama, sizeof(trama));
        if (bytes_read < 0) {
            if (errno == EINTR) {
                continue;  // Senyal, tornar a intentar
            } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
                time_t now = time(NULL);
                if (now - last_print >= 10) {
                    last_print = now;
                }
                continue;  // Tornar a esperar.
            } else {
                if (gestionar_caiguda_worker(worker_fd, "ESPERANT", media_type, file_name, worker_fd, file_size, md5sum, factor) < 0) {
                    write_message(STDOUT_FILENO, "[FLECK] Error gestionant la caiguda del Worker.\n");
                    return ERROR_WORKER_DOWN;
                } else {
                    write_message(STDOUT_FILENO, "[FLECK] Reconnectat al nou Worker. Continuant recepció...\n");
                    continue;  
                }
            }

                return ERROR_READ;
        }

        if (bytes_read == sizeof(trama)){ 
            break;
        }  // Correcte, ja tenim la trama 0x04.
        

        if (bytes_read == 0 || errno == ECONNRESET || errno == EPIPE) {
            if (gestionar_caiguda_worker(worker_fd, "REBENT", media_type, file_name, worker_fd, file_size, md5sum, factor) < 0) {
                write_message(STDOUT_FILENO, "[FLECK] Error gestionant la caiguda del Worker.\n");
                return ERROR_WORKER_DOWN;
            } else {
                write_message(STDOUT_FILENO, "[FLECK] Reconnectat al nou Worker. Continuant recepció...\n");                    continue;  
            }
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

    uint32_t file_size_rebuda = strtoul(file_size_str, NULL, 10);

    // 2. Obrir fitxer de sortida
    int output_fd = open(output_file, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (output_fd < 0) {
        enviar_trama_desconnexio_worker(worker_fd);
        return ERROR_OUTPUT_FILE;
    }
    update_progress_status(file_name, REBENT, file_size, file_size_rebuda, 0, 0);


    uint32_t total_rebut = 0;

    // 3. Rebre trames 0x05 fins completar file_size
    while (total_rebut < file_size_rebuda) {
        ssize_t bytes_read = safe_read(worker_fd, &trama, sizeof(trama));
        if (bytes_read != sizeof(trama)) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) continue;

            if (errno == ECONNRESET || errno == EPIPE) {
                if (gestionar_caiguda_worker(worker_fd, "REBENT", media_type, file_name, worker_fd, file_size, md5sum, factor) < 0) {
                    write_message(STDOUT_FILENO, "[FLECK] Error gestionant la caiguda del Worker.\n");
                    close(output_fd);
                    return ERROR_WORKER_DOWN;
                } else {
                    write_message(STDOUT_FILENO, "[FLECK] Reconnectat al nou Worker. Continuant recepció...\n");
                    lseek(output_fd, total_rebut, SEEK_SET);
                    continue;  
                }
            }

            enviar_trama_desconnexio_worker(worker_fd);
            close(output_fd);
            return ERROR_READ;
        }

        

        if (write(output_fd, trama.data, trama.data_length) != trama.data_length) {
            enviar_trama_desconnexio_worker(worker_fd);
            close(output_fd);
            return ERROR_READ;
        }

        total_rebut += trama.data_length;
        update_progress_status(file_name, REBENT, 0, file_size_rebuda, 0, total_rebut);
    }

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
        update_progress_status(file_name, FINALITZAT, 0, 0, 0, 0);
    } else {
        inicialitzar_trama(&resposta_trama, 0x06, "CHECK_KO");
        unregister_file(file_name);
        write_message(STDOUT_FILENO, "[FLECK] MD5SUM incorrecte, enviant CHECK_KO al Worker.\n");
    }


    if (write(worker_fd, &resposta_trama, sizeof(resposta_trama)) != sizeof(resposta_trama)) {
        return ERROR_READ;
    }

    return 0;
}


/**
 * Gestiona la distorsió d'un fitxer amb el worker.
 * @param media_type Tipus de media ("media", "text").
 * @param worker_ip IP del worker assignat.
 * @param worker_port Port del worker assignat.
 * @param file_name Nom del fitxer a distorsionar.
 * @param factor Factor de distorsió.
 * @return 0 si la distorsió és correcta, -1 si hi ha un error.
 */
int gestionar_distorsio_worker(const char *media_type, const char *worker_ip, int worker_port, const char *file_name, int factor) {
    struct timeval timeout;

    if (!worker_ip || worker_port <= 0 || !file_name || factor <= 0) {
        write_message(STDOUT_FILENO, "Error: Invalid distortion parameters\n");
        return -1;
    }
    

    int worker_fd = connectar_a_server(worker_ip, worker_port);

    if (worker_fd < 0) {
        write_message(STDOUT_FILENO, "Error connecting to the worker.\n");
        enviar_trama_desconnexio_worker(worker_fd);
        return -1;
    }



    if (strcmp(media_type, "media") == 0) {
        harley_fd = worker_fd;  // Assignar el descriptor de socket al worker de media
    } else if (strcmp(media_type, "text") == 0) {
        enigma_fd = worker_fd;  // Assignar el descriptor de socket al worker de text
    } else {
        write_message(STDOUT_FILENO, "Error: Invalid media type. Must be 'media' or 'text'.\n");
        enviar_trama_desconnexio_worker(worker_fd);
        return -1;
    }


    
    uint32_t file_size;
    char md5sum[33]; 
    if (calcular_md5(file_name, md5sum, &file_size) < 0) {
        write_message(STDOUT_FILENO, "Error calculating MD5SUM.\n");
        enviar_trama_desconnexio_worker(worker_fd);
        return -1;
    }
    write_message(STDOUT_FILENO, "Distorsion started!\n");


    if (enviar_fitxer_worker(worker_fd, media_type, file_name, file_size, md5sum, factor) < 0) {
        enviar_trama_desconnexio_worker(worker_fd);
        return -1;
    }

    //Reactivar timeout per lectures futures (ex. 5 segons)
    timeout.tv_sec = TIMEOUT_SECONDS;
    timeout.tv_usec = 0;
    setsockopt(worker_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));


    // Generate output_file dynamically
    size_t output_len = strlen("distorted_") + 20 + strlen(file_name) + 1;
    char *output_file = malloc(output_len);
    if (!output_file) {
        enviar_trama_desconnexio_worker(worker_fd);
        return -1;
    }
    snprintf(output_file, output_len, "distorted_%d_%s", getpid(), file_name);

    if (rebre_fitxer_worker(worker_fd, output_file, media_type, file_name, file_size, md5sum, factor) < 0) {
        enviar_trama_desconnexio_worker(worker_fd);
        return -1;
    }

    enviar_trama_desconnexio_worker(worker_fd);  
    return 0;
}



/**
 * Envia una petició de distorsió a Gotham.
 * @param media_type Tipus de media ("media", "text").
 * @param file Nom del fitxer a distorsionar.
 * @param factor Factor de distorsió.
 * @return 0 si la petició s'envia correctament, -1 si hi ha un error.
 */
int enviar_peticio_distorsio(const char *media_type, const char *file, int factor) { 
    if (!media_type || !file) {
        write_message(STDOUT_FILENO, "Error: Invalid media type or file name.\n");
        return -1;
    }

    Trama trama;
    char data[245];
    snprintf(data, sizeof(data), "%s&%s", media_type, file);
    inicialitzar_trama(&trama, 0x10, data);


    if (write(sock_fd, &trama, sizeof(trama)) != sizeof(trama)) {
        write_message(STDOUT_FILENO, "Error sending the distortion request.\n");
        return -1;
    }

    Trama resposta;
    if (safe_read(sock_fd, &resposta, sizeof(Trama)) < 0) {
        write_message(STDOUT_FILENO, "Error receiving response from Gotham.\n");
        return -1;
    }

    if (!validar_trama(&resposta)) {
        write_message(STDOUT_FILENO, "Invalid response from Gotham.\n");
        return -1;
    }

    if (resposta.type == 0x10 && resposta.data_length > 0) {
        if (strcmp(resposta.data, "DISTORT_KO") == 0) {
            write_message(STDOUT_FILENO, "[ERROR] No workers available\n");
            return -1;
        } else if (strcmp(resposta.data, "MEDIA_KO") == 0) {
            write_message(STDOUT_FILENO, "[ERROR] Invalid media type\n");
            return -1;
        } else {
            char worker_ip[16];
            int worker_port;

            parse_ip_port(resposta.data, worker_ip, &worker_port);

            DistorsioArgs *args = malloc(sizeof(DistorsioArgs));
            if (!args) {
                return -1;
            }

            strncpy(args->worker_ip, worker_ip, sizeof(args->worker_ip));
            args->worker_port = worker_port;
            strncpy(args->file_name, file, sizeof(args->file_name));
            args->factor = factor;

            if (existeix_distorsio_activa(media_type)) {
                write_message(STDOUT_FILENO, "[ERROR] There is already an active distortion of this type.\n");
                return -1;
            }

            if (*file) {
                register_new_file(file); 
            }

            if (gestionar_distorsio_worker(media_type, args->worker_ip, args->worker_port, args->file_name, args->factor) == 0) {
                write_message(STDOUT_FILENO, "Distorsion completed successfuly\n");
            } else {
                mark_error_status(file);
                write_message(STDOUT_FILENO, "Error during distortion :(\n");
            }
           
            return 0;
        }
    }

    write_message(STDOUT_FILENO, "Unexpected response from Gotham\n");
    return -1;
}

void netejar_distorsions_acabades() {
    pthread_mutex_lock(&status_mutex);
    
    DistorsioStatus **curr = &head;
    if (!head) {
        write_message(STDOUT_FILENO, "You have no ongoing or finished distorsions.\n");
        pthread_mutex_unlock(&status_mutex);
        return;
    }
    while (*curr) {
        if ((*curr)->progress == 100 || (*curr)->error == 1) {
            DistorsioStatus *a_eliminar = *curr;
            *curr = (*curr)->next;
            free(a_eliminar);
        } else {
            curr = &((*curr)->next);
        }
    }
    pthread_mutex_unlock(&status_mutex);
    write_message(STDOUT_FILENO, "Finished distortions removed\n");
}



/**
 * Divideix i delega gestions de comandes en funcions més petites.
 * Gestió de la comanda DISTORT.
 * @param command Comanda completa introduïda per l'usuari.
 */
void gestionar_comanda_distort(char *command) {
    char *file = strtok(command + 8, " ");
    char *factor_str = strtok(NULL, " ");
    
    if (!file || !factor_str || !isdigit(factor_str[0])) {
        write_message(STDOUT_FILENO, "[ERROR] DISTORT <file.xxx> <factor>\n");
        return;
    }

    int factor = atoi(factor_str);
    if (factor <= 0) {
        write_message(STDOUT_FILENO, "[ERROR] Invalid factor\n");
        return;
    }

    char *ext = strrchr(file, '.');
    if (!ext) {
        write_message(STDOUT_FILENO, "[ERROR] The file does not have an extension\n");
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
        write_message(STDOUT_FILENO, "[ERROR] Unsupported file type\n");
        return;
    }

    if (access(file, R_OK) != 0) {
        return;
    } 
    

    enviar_peticio_distorsio(file_type, file, factor);
}


void *gestionar_comanda_distort_thread(void *arg) {
    char *command = (char *)arg;
    gestionar_comanda_distort(command);  
    free(command);
    return NULL;
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
        fflush(stdout);
        ssize_t leido = read(STDIN_FILENO, command, len - 1);
        if (leido <= 0) break;

        command[leido] = '\0';
        trim_whitespace(command);
        if (strlen(command) == 0) {
            continue;  
        }


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
            if (!connected_to_gotham) {
                write_message(STDOUT_FILENO, "[ERROR] You must connect to the Gotham server first.\n");
                write_message(STDOUT_FILENO, "Use the CONNECT command to establish a connection.\n");
                continue;
            }
            char *cmd_copy = strdup(command);
            if (!cmd_copy) {
                write_message(STDOUT_FILENO, "Error allocating memory for the command\n");
            } else {
                pthread_t tid;
                if (pthread_create(&tid, NULL, gestionar_comanda_distort_thread, cmd_copy) != 0) {
                    write_message(STDOUT_FILENO, "Error creating thread for the DISTORT command\n");
                    free(cmd_copy);
                } else {
                    pthread_detach(tid);
                }
            }
        } else if (strcmp(command_upper, "LOGOUT") == 0) {
            parar_fleck(0);
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
    
    signal(SIGINT, parar_fleck);

    connected_to_gotham = 0;
    sock_fd = -1;  

    // Iniciar gestió de comandes
    gestionar_comandes_fleck();

    return 0;
}