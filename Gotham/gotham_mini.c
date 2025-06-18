#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <signal.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include "../Utils/sys_params.h"
#include "../Utils/file_utils.h"
#include "../Utils/trames.h"

ConfigGotham config;
pthread_t fleck_thread, worker_thread;
pthread_t task_thread;
pthread_t monitor_thread;
ConfigWorker *workers = NULL;
int num_workers = 0;
pthread_mutex_t worker_mutex = PTHREAD_MUTEX_INITIALIZER;


TaskManager task_manager;     // Gestor de tasques centralitzat
int arkham_pid;
int log_pipe_fd[2];           // Pipe per al logger Arkham
pthread_mutex_t arkham_mutex = PTHREAD_MUTEX_INITIALIZER;


/**
 * Logger: registra un esdeveniment al sistema.
 */
void log_event(const char *event) {
    pthread_mutex_lock(&arkham_mutex);

    time_t now = time(NULL);
    struct tm *tm_info = localtime(&now);

    char timestamp[64];
    strftime(timestamp, sizeof(timestamp), "[%Y-%m-%d %H:%M:%S]", tm_info);

    size_t total_len = strlen(timestamp) + strlen(event) + 3; // +3 per espai, \n i \0
    char *log_msg = malloc(total_len);
    if (!log_msg) {
        pthread_mutex_unlock(&arkham_mutex);
        write_message(STDERR_FILENO, "[ARKHAM] Error reservant memòria per al log.\n");
        return; 
    }

    snprintf(log_msg, total_len, "%s %s\n", timestamp, event);

    if (write(log_pipe_fd[1], log_msg, strlen(log_msg)) < 0) {
        write_message(STDOUT_FILENO, "[ARKHAM] Error escrivint al log.\n");
    }

    free(log_msg);
    pthread_mutex_unlock(&arkham_mutex);
}


/*
* Envia una trama d'error quan es rebutja una trama per format o checksum incorrecte.
* Aquesta funció registra l'esdeveniment i envia una resposta al socket especificat.    
* @param socket_fd El descriptor de fitxer del socket on enviar la resposta.
* @param origen Opcional: cadena que indica l'origen de la trama rebutjada.
*/
void respondre_trama_erronia(int socket_fd, const char *origen) {
    if (origen) {
        char log_msg[128];
        snprintf(log_msg, sizeof(log_msg), "Trama rebutjada de %s per format o checksum incorrecte.", origen);
        log_event(log_msg);
    }

    Trama resposta;
    inicialitzar_trama(&resposta, 0x09, "");
    write(socket_fd, &resposta, sizeof(resposta));
}

/**
 * Consulta qui és el principal (si n’hi ha). No el canvia.
 * @param tipus_worker Tipus de worker a buscar (ENIGMA o HARLEY).
 * @return Retorna un punter al worker principal, o NULL si no n'hi ha cap.
 */
ConfigWorker* obtenir_principal(InstanceType tipus_worker) {
    for (int i = 0; i < num_workers; i++) {
        if (workers[i].base.type == tipus_worker && workers[i].is_primary_worker) {
            return &workers[i];
        }
    }
    return NULL;
}

/**
 * Selecciona un nou worker principal del tipus especificat.
 * Si no hi ha candidats, retorna NULL.
 * @param tipus_worker Tipus de worker a seleccionar (ENIGMA o HARLEY).
 * @return Retorna un punter al nou worker principal, o NULL si no n'hi ha cap candidat.
 */
ConfigWorker* seleccionar_nou_principal(InstanceType tipus_worker) {
    write_message(STDOUT_FILENO, "🔍 seleccionant nou principal...\n");

    int *indexos = malloc(num_workers * sizeof(int));  // en el pitjor cas, tots podrien ser candidats
    if (!indexos) {
        write_message(STDERR_FILENO, "Error: no s'ha pogut reservar memòria per seleccionar nou principal.\n");
        return NULL;
    }

    int pos = 0;
    for (int i = 0; i < num_workers; i++) {
        char debug[128];
        snprintf(debug, sizeof(debug), "Worker ID %d, tipus %d, principal: %d\n",
                workers[i].worker_id, workers[i].base.type, workers[i].is_primary_worker);
        write_message(STDOUT_FILENO, debug);

        if (workers[i].base.type == tipus_worker && !workers[i].is_primary_worker) {
            indexos[pos++] = i;
        }
    }

    if (pos == 0) {
        write_message(STDOUT_FILENO, "[DEBUG] No hi ha candidats per promoure com a principal!\n");
        free(indexos);
        return NULL;
    }

    int sel = indexos[rand() % pos];
    free(indexos);

    workers[sel].is_primary_worker = 1;
    workers[sel].last_heartbeat = time(NULL);

    Trama promotora;
    inicialitzar_trama(&promotora, 0x08, "");
    write(workers[sel].base.socket_fd, &promotora, sizeof(promotora));

    char log_msg[128];
    snprintf(log_msg, sizeof(log_msg),
             "Worker ID %d seleccionat com a nou principal del tipus %d.",
             workers[sel].worker_id, tipus_worker);
    log_event(log_msg);

    return &workers[sel];
}

/**
 * elimina un worker de la llista de workers.
 * @param socket_fd Socket del worker a eliminar.   
 * @return Retorna 1 si s'ha eliminat correctament, 0 si no s'ha trobat el worker.
 */

void eliminar_worker_per_socket(int socket_fd) {
    pthread_mutex_lock(&worker_mutex);

    for (int i = 0; i < num_workers; i++) {
        if (workers[i].base.socket_fd == socket_fd) {
            int tipus_worker = workers[i].base.type;
            int era_principal = workers[i].is_primary_worker;
            free(workers[i].base.ip);
            free(workers[i].ip_fleck);

            for (int j = i; j < num_workers - 1; j++) {
                workers[j] = workers[j + 1];
            }

            num_workers--;
            // Redimensionar l'array de workers 

            workers = realloc(workers, num_workers * sizeof(ConfigWorker));
            if (workers == NULL && num_workers > 0) {
                log_event("Error real·locant memòria en eliminar worker.");
            }

            // Reassignar si cal
            if (era_principal) {
                ConfigWorker *nou = seleccionar_nou_principal(tipus_worker);
                if (!nou) {
                    log_event("No hi ha cap altre worker per reassignar com a principal.");
                }
            }

            break;
        }
    }

    pthread_mutex_unlock(&worker_mutex);
}

/**
 * Monitoritza els heartbeats dels workers i elimina els que superin el timeout.
 * Aquesta funció s'executa en un thread separat.
 */
void *monitoritzar_heartbeats(void *arg) {
    (void)arg;

    const int TIMEOUT = 15;  // segons

    while (1) {
        time_t ara = time(NULL);

        for (int i = 0; i < num_workers; i++) {
            int worker_id;
            int socket_fd;
            time_t last_heartbeat;

            pthread_mutex_lock(&worker_mutex);
            // Copiem les dades crítiques que necessitem abans de sortir del lock
            if (i < num_workers) {  // Comprovació per si num_workers ha canviat
                worker_id = workers[i].worker_id;
                socket_fd = workers[i].base.socket_fd;
                last_heartbeat = workers[i].last_heartbeat;
            }
            pthread_mutex_unlock(&worker_mutex);

            double diff = difftime(ara, last_heartbeat);
            if (diff > TIMEOUT) {
                char log_msg[128];
                snprintf(log_msg, sizeof(log_msg),
                         "Worker ID %d ha superat el timeout de %ds. Eliminant-lo.",
                         worker_id, TIMEOUT);
                log_event(log_msg);

                eliminar_worker_per_socket(socket_fd);

                i = -1;  // Reiniciem l’iteració perquè la llista ha canviat
                break;
            }
        }
        sleep(5);  // Interval entre comprovacions
    }

    return NULL;
}



/**
 * Afegeix un nou worker a la llista i estableix un worker principal si és el primer del seu tipus.
 * @param new_worker Configuració del nou worker.
 */
void afegir_worker(ConfigWorker new_worker) {
    pthread_mutex_lock(&worker_mutex);

    // Validar IP Fleck
    if (!new_worker.ip_fleck) {
        log_event("Error: Worker intentant registrar-se amb IP Fleck NULL.");
        pthread_mutex_unlock(&worker_mutex);
        return;
    }

    // Prevenir duplicats (mateix socket_fd)
    for (int i = 0; i < num_workers; i++) {
        if (workers[i].base.socket_fd == new_worker.base.socket_fd) {
            log_event("Intent d'afegir un worker duplicat. No es permet.");
            pthread_mutex_unlock(&worker_mutex);
            return;
        }
    }

    // 1. Redimensionar l’array abans de res
    ConfigWorker *temp = realloc(workers, (num_workers + 1) * sizeof(ConfigWorker));
    if (!temp) {
        log_event("Error d'allocació de memòria per a un nou worker.");
        pthread_mutex_unlock(&worker_mutex);
        return;
    }
    workers = temp;
    workers[num_workers] = new_worker;
    workers[num_workers].base.ip = strdup(new_worker.base.ip);
    workers[num_workers].ip_fleck = strdup(new_worker.ip_fleck);
    workers[num_workers].base.port = new_worker.base.port;
    workers[num_workers].port_fleck = new_worker.port_fleck;
    workers[num_workers].worker_id = num_workers + 1;
    workers[num_workers].last_heartbeat = time(NULL);

    // 2. Enviar CON_OK
    Trama resposta;
    inicialitzar_trama(&resposta, 0x02, "");
    write(new_worker.base.socket_fd, &resposta, sizeof(resposta));

    // 3. Comprovar si ha de ser principal
    int ja_te_principal = 0;
    for (int i = 0; i < num_workers; i++) {
        if (workers[i].base.type == new_worker.base.type && workers[i].is_primary_worker) {
            ja_te_principal = 1;
            break;
        }
    }

    if (ja_te_principal == 0) {
        workers[num_workers].is_primary_worker = 1;
        Trama promotora;
        inicialitzar_trama(&promotora, 0x08, "");
        write(new_worker.base.socket_fd, &promotora, sizeof(promotora));
        log_event("Trama 0x08 enviada al primer worker com a principal.");
    } else {
        workers[num_workers].is_primary_worker = 0;
    }

    // 4. Fer el log
    char log_msg[256];
    snprintf(log_msg, sizeof(log_msg), 
            "Worker ID: %d, Tipus: %d, IP Gotham: %s, IP Fleck: %s, Port Fleck: %d, Principal: %d",
            workers[num_workers].worker_id, new_worker.base.type, 
            new_worker.base.ip, new_worker.ip_fleck, new_worker.port_fleck,
            workers[num_workers].is_primary_worker);
    log_event(log_msg);

    num_workers++;

    pthread_mutex_unlock(&worker_mutex);
}

/**
 * Thread que gestiona les connexions dels workers.
 * Accepta connexions entrants i crea un nou thread per cada worker.
 */
void *handle_worker(void *arg) {
    int worker_socket = *(int *)arg;
    free(arg);

    Trama trama;

    while (read(worker_socket, &trama, sizeof(trama)) == sizeof(trama)) {
        if (!validar_trama(&trama)) {
            respondre_trama_erronia(worker_socket, "Worker");
            continue;
        }

        if (trama.type == 0x02) {
            char *workerType = strtok(trama.data, "&");
            char *workerIP = strtok(NULL, "&");
            char *workerPortStr = strtok(NULL, "&");

            if (!workerType || strlen(workerType) == 0 ||
                !workerIP || strlen(workerIP) == 0 ||
                !workerPortStr || strlen(workerPortStr) == 0 ||
                atoi(workerPortStr) <= 0) {
                
                write_message(STDOUT_FILENO, "Trama de worker mal formada o amb camps buits.\n");

                Trama resposta;
                inicialitzar_trama(&resposta, 0x02, "CON_KO");
                write(worker_socket, &resposta, sizeof(resposta));

                close(worker_socket);
                continue;
            }

            ConfigWorker new_worker;
            new_worker.base.socket_fd = worker_socket;
            new_worker.base.ip = strdup(config.base.ip);  // IP de Gotham
            new_worker.base.port = config.base.port;
            new_worker.ip_fleck = strdup(workerIP);
            new_worker.port_fleck = atoi(workerPortStr);
            new_worker.base.type = (strcasecmp(workerType, "TEXT") == 0) ? ENIGMA : HARLEY;

            afegir_worker(new_worker);


            // Mensaje tipo
            const char *tipus = (new_worker.base.type == ENIGMA) ? "ENIGMA" : "HARLEY";

            // Mensaje registro
            int len2 = snprintf(NULL, 0, "Worker registrat: %s:%d (type: %s)\n", workerIP, new_worker.port_fleck, tipus) + 1;
            char *msg2 = malloc(len2);
            if (msg2) {
                snprintf(msg2, len2, "Worker registrat: %s:%d (%s)\n", workerIP, new_worker.port_fleck, workerType);
                log_event(msg2);
                free(msg2);
            }
        } else if (trama.type == 0x07) {
            int len3 = snprintf(NULL, 0, "Worker %s desconnectat\n", trama.data) + 1;
            char *msg3 = malloc(len3);
            if (msg3) {
                snprintf(msg3, len3, "Worker %s desconnectat\n", trama.data);
                write_message(STDOUT_FILENO, msg3);
                free(msg3);
            }

            printf("Worker %s desconnectat\n", trama.data);

            int len4 = snprintf(NULL, 0, "Desconnectant worker %d\n", trama.type) + 1;
            char *msg4 = malloc(len4);
            if (msg4) {
                snprintf(msg4, len4, "Desconnectant worker %d\n", trama.type);
                write_message(STDOUT_FILENO, msg4);
                free(msg4);
            }
            log_event("Trama 0x07 rebuda: worker sol·licita desconnexió.");
            eliminar_worker_per_socket(worker_socket);
        } else if (trama.type == 0x12) {
            pthread_mutex_lock(&worker_mutex);
            for (int i = 0; i < num_workers; i++) {
                if (workers[i].base.socket_fd == worker_socket) {
                    workers[i].last_heartbeat = time(NULL);
                    break;
                }
            }
            pthread_mutex_unlock(&worker_mutex);
        } else {
            int len6 = snprintf(NULL, 0, "Trama desconeguda rebuda del worker: 0x%02X\n", trama.type) + 1;
            char *msg6 = malloc(len6);
            if (msg6) {
                snprintf(msg6, len6, "Trama desconeguda rebuda del worker: 0x%02X\n", trama.type);
                write_message(STDOUT_FILENO, msg6);
                free(msg6);
            }
            // Enviem trama 0x09 per notificar error desconegut
            Trama error_trama;
            memset(&error_trama, 0, sizeof(Trama));
            error_trama.type = 0x09;
            error_trama.data_length = 0;
            write(worker_socket, &error_trama, sizeof(error_trama));
        }
    }

    close(worker_socket);
    return NULL;
}

/**
 * Thread que escolta les connexions dels workers.
 * Accepta connexions entrants i crea un nou thread per gestionar cada worker.
 */
void *listener_worker(void *arg) {
    (void)arg;
    struct sockaddr_in address;
    socklen_t addrlen = sizeof(address);

    int server_fd_worker = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd_worker < 0) {
        exit(EXIT_FAILURE);
    }
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = inet_addr(config.worker_ip);
    address.sin_port = htons(config.worker_port);

    if (bind(server_fd_worker, (struct sockaddr *)&address, sizeof(address)) < 0) {
        exit(EXIT_FAILURE);
    }

    if (listen(server_fd_worker, 10) < 0) {
        exit(EXIT_FAILURE);
    }

    while (1) {
        int worker_socket = accept(server_fd_worker, (struct sockaddr *)&address, &addrlen);
        if (worker_socket >= 0) {
            int *socket_ptr = malloc(sizeof(int));
            *socket_ptr = worker_socket;
            pthread_t thread;
            pthread_create(&thread, NULL, handle_worker, socket_ptr);                    
            pthread_detach(thread);

        }
    }
    return NULL;
}

/**
 * Valida si el fitxer té una extensió vàlida per al tipus indicat.
 * Retorna 1 si és vàlid, 0 si no. Escriu el tipus corresponent a tipus_worker.
 * @param mediaType Tipus de media ("media" o "text").
 * @param fileName Nom del fitxer a validar.
 * @param tipus_worker Punter a la variable on es guardarà el tipus de worker (HARLEY o ENIGMA).
 * @return Retorna 1 si el fitxer és vàlid, 0 si no.
 */
int validar_tipus_fitxer(const char *mediaType, const char *fileName, int *tipus_worker) {
    if (!mediaType || !fileName || !tipus_worker) return 0;

    char *ext = strrchr(fileName, '.');
    if (!ext) return 0;

    if (strcmp(mediaType, "media") == 0) {
        const char *media_exts[] = {".wav", ".mp3", ".jpg", ".png"};
        for (size_t i = 0; i < sizeof(media_exts)/sizeof(media_exts[0]); i++) {
            if (strcasecmp(ext, media_exts[i]) == 0) {
                *tipus_worker = HARLEY;
                return 1;
            }
        }
    } else if (strcmp(mediaType, "text") == 0) {
        if (strcasecmp(ext, ".txt") == 0) {
            *tipus_worker = ENIGMA;
            return 1;
        }
    }

    return 0;  // No és vàlid
}

/**
 * Retorna un worker del tipus donat que no sigui el principal (alternatiu).
 * @param tipus_worker Tipus de worker (ENIGMA o HARLEY).
 * @return Punter al worker alternatiu, o NULL si no n’hi ha cap.
 */
ConfigWorker* buscar_alternatiu_no_principal(InstanceType tipus_worker) {
    for (int i = 0; i < num_workers; i++) {
        if (workers[i].base.type == tipus_worker && !workers[i].is_primary_worker) {
            return &workers[i];
        }
    }
    return NULL;
}


/**
 * Genera una cadena amb format "<ip>&<port>" a partir d’un worker.
 * @param worker Punter al ConfigWorker.
 * @param buffer Destí on s’escriurà la cadena (ha de tenir almenys 64 bytes).
 */
void construir_data_worker(const ConfigWorker *worker, char *buffer) {
    if (worker && buffer) {
        snprintf(buffer, 64, "%s&%d", worker->ip_fleck, worker->port_fleck);
    }
}

/**
 * Thread que gestiona les connexions dels clients Fleck.
 * Accepta connexions entrants i crea un nou thread per cada client.
 */
void *gestionar_client_fleck(void *arg) {
    int client_socket = *(int *)arg;
    free(arg);  // Liberar la memoria del socket pasado por puntero

    Trama trama;
    while (read(client_socket, &trama, sizeof(trama)) == sizeof(trama)) {
        if (!validar_trama(&trama)) {
            respondre_trama_erronia(client_socket, "Fleck");
            continue;
        }
        if (trama.type == 0x01) {
            Trama resposta;
            inicialitzar_trama(&resposta, 0x01, "");
            write(client_socket, &resposta, sizeof(resposta));

            // Extreure el nom del Fleck
            char *username = strtok(trama.data, "&");

            char msg[128];
            if (username) {
                snprintf(msg, sizeof(msg), "%s connected to Mr. J System. Let the chaos begin!:)\n", username);
            } else {
                snprintf(msg, sizeof(msg), "Unknown user connected to Mr. J System. Let the chaos begin!:)\n");
            }
            write(STDOUT_FILENO, msg, strlen(msg));
        } else if (trama.type == 0x07) {

            log_event("Fleck ha sol·licitat la desconnexió.");
            close(client_socket);
            break;

        } else if (trama.type == 0x10) {

            char *mediaType = strtok(trama.data, "&");
            char *fileName = strtok(NULL, "&");

            Trama resposta;

            if (!mediaType || !fileName) {
                inicialitzar_trama(&resposta, 0x10, "MEDIA_KO");
                write(client_socket, &resposta, sizeof(resposta));
                continue;
            }

            // Verifica que l'extensió del fitxer coincideixi amb el tipus de media
            int tipus_worker;
            if (!validar_tipus_fitxer(mediaType, fileName, &tipus_worker)) {
                inicialitzar_trama(&resposta, 0x10, "MEDIA_KO");
                write(client_socket, &resposta, sizeof(resposta));
                continue;
            }

           // Buscar el worker principal del tipus sol·licitat
            pthread_mutex_lock(&worker_mutex);
            ConfigWorker *principal = NULL;

            if (strcmp(mediaType, "media") == 0) {
                principal = obtenir_principal(HARLEY);
            } else if (strcmp(mediaType, "text") == 0) {
                principal = obtenir_principal(ENIGMA);
            }
            pthread_mutex_unlock(&worker_mutex);

            if (principal) {
                char resposta_data[64];
                construir_data_worker(principal, resposta_data);
                inicialitzar_trama(&resposta, 0x10, resposta_data);
            } else {
                inicialitzar_trama(&resposta, 0x10, "DISTORT_KO");
            }
            write(client_socket, &resposta, sizeof(resposta));

        } else if (trama.type == 0x11) {
            char *mediaType = strtok(trama.data, "&");
            char *fileName = strtok(NULL, "&");

            Trama resposta;

            if (!mediaType || !fileName) {
                inicialitzar_trama(&resposta, 0x11, "MEDIA_KO");
                write(client_socket, &resposta, sizeof(resposta));
                return NULL;
            }

            int tipus_worker;
            if (!validar_tipus_fitxer(mediaType, fileName, &tipus_worker)) {
                inicialitzar_trama(&resposta, 0x11, "MEDIA_KO");
                write(client_socket, &resposta, sizeof(resposta));
                return NULL;
            }
            
            int principal_socket_fd = -1;
            pthread_mutex_lock(&worker_mutex);
            ConfigWorker *principal = obtenir_principal(tipus_worker);
            if (principal) {
                principal_socket_fd = principal->base.socket_fd;
            }
            pthread_mutex_unlock(&worker_mutex);

            if (principal_socket_fd != -1) {
                char debug_msg[128];
                snprintf(debug_msg, sizeof(debug_msg),
                        "[REASSIGN] Eliminant principal (socket_fd %d) del tipus %d abans de reassignar.",
                        principal_socket_fd, tipus_worker);
                log_event(debug_msg);
                eliminar_worker_per_socket(principal_socket_fd);
            }
            
            char ip_tmp[64];
            int port_tmp = -1;

            pthread_mutex_lock(&worker_mutex);
            ConfigWorker *nou_principal = obtenir_principal(tipus_worker);
            if (!nou_principal) {
                write_message(STDOUT_FILENO, "[REASSIGN] Cap principal trobat després d’eliminar. Forçant promoció.\n");
                nou_principal = seleccionar_nou_principal(tipus_worker);
            }
            if (nou_principal) {
                char debug_msg[128];
                snprintf(debug_msg, sizeof(debug_msg),
                        "[REASSIGN] Nou principal (socket_fd %d) del tipus %d amb id %d.",
                        nou_principal->base.socket_fd, tipus_worker, nou_principal->worker_id);
                strncpy(ip_tmp, nou_principal->ip_fleck, sizeof(ip_tmp));
                ip_tmp[sizeof(ip_tmp) - 1] = '\0';
                port_tmp = nou_principal->port_fleck;
            }
            pthread_mutex_unlock(&worker_mutex);
            if (port_tmp == -1) {
                inicialitzar_trama(&resposta, 0x11, "DISTORT_KO");
            } else {
                write_message(STDOUT_FILENO, "[REASSIGN] S'envia 0x11 al fleck amb el nou principal.\n");
                char data_resposta[64];
                snprintf(data_resposta, sizeof(data_resposta), "%s&%d", ip_tmp, port_tmp);
                inicialitzar_trama(&resposta, 0x11, data_resposta);
            }
            ssize_t bytes_written = write(client_socket, &resposta, sizeof(resposta));
            if (bytes_written != sizeof(trama)) {
                log_event("Error en enviar la trama: write incomplet o fallit.");
                // opcional: tancar socket, marcar worker com caigut, etc.
            }
        } else {
            // Tipus desconegut de trama
            char missatge[64];
            snprintf(missatge, sizeof(missatge), "Trama desconeguda rebuda de Fleck: 0x%02X\n", trama.type);
            write_message(STDOUT_FILENO, missatge);
            log_event(missatge);

            Trama resposta_error;
            inicialitzar_trama(&resposta_error, 0x09, "");
            write(client_socket, &resposta_error, sizeof(resposta_error));
        }
    }

    close(client_socket);
    return NULL;
}

/**
 * Thread que escolta les connexions dels clients Fleck.
 * Accepta connexions entrants i crea un nou thread per gestionar cada client.
 */
void *listener_fleck(void *arg) {
    (void)arg;
    int server_fd;
    struct sockaddr_in address;
    socklen_t addrlen = sizeof(address);

    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        exit(EXIT_FAILURE);
    }

    address.sin_family = AF_INET;
    address.sin_addr.s_addr = inet_addr(config.base.ip);
    address.sin_port = htons(config.base.port);

    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        exit(EXIT_FAILURE);
    }

    if (listen(server_fd, 10) < 0) {
        exit(EXIT_FAILURE);
    }

    while (1) {
        int *client_socket = malloc(sizeof(int));
        if (!client_socket) {
            continue;
        }

        *client_socket = accept(server_fd, (struct sockaddr *)&address, &addrlen);
        if (*client_socket >= 0) {
            pthread_create(&fleck_thread, NULL, gestionar_client_fleck, client_socket);
            pthread_detach(fleck_thread);
        } else {
            free(client_socket);
        }
    }

    return NULL;
}

/**
 * Funció principal del programa.
 * Inicialitza la configuració, crea els threads i espera que acabin.
 * @param argc Nombre d'arguments de la línia de comandes.
 * @param argv Arguments de la línia de comandes.
 * @return Retorna 0 si tot ha anat bé, 1 si hi ha hagut un error.
 */
int main(int argc, char *argv[]) {
    if (argc != 2) {
        fprintf(stderr, "Us: ./gotham_mini <fitxer_configuracio>\n");
        return 1;
    }

    int fd = open(argv[1], O_RDONLY);
    if (fd < 0 || parse_gotham_config(fd, &config) < 0) {
        return 1;
    }
    close(fd);


    signal(SIGINT, exit);
    pthread_create(&fleck_thread, NULL, listener_fleck, NULL);
    pthread_create(&worker_thread, NULL, listener_worker, NULL);
    
    pthread_create(&monitor_thread, NULL, monitoritzar_heartbeats, NULL);


    pthread_join(fleck_thread, NULL);
    pthread_join(worker_thread, NULL);
    return 0;
}
