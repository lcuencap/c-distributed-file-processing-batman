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
    char timestamp[64];
    strftime(timestamp, sizeof(timestamp), "[%Y-%m-%d %H:%M:%S]", localtime(&now));

    char log_msg[512];
    snprintf(log_msg, sizeof(log_msg), "%s %s\n", timestamp, event);
    write(log_pipe_fd[1], log_msg, strlen(log_msg));
    pthread_mutex_unlock(&arkham_mutex);
}


/**
 * Selecciona un nou worker principal del mateix tipus si el principal actual es desconnecta.
 * @param tipus_worker Tipus de worker a buscar (ENIGMA o HARLEY).
 * @return Retorna un punter al nou worker seleccionat com a principal, o NULL si no n'hi ha cap.
 */
ConfigWorker* obtenir_principal(int tipus_worker) {
    for (int i = 0; i < num_workers; i++) {
        if (workers[i].base.type == tipus_worker && workers[i].is_primary_worker) {
            return &workers[i];
        }
    }
    return NULL;
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

    // Comprovar si ja hi ha un worker principal d'aquest tipus
    int ja_te_principal = 0;
    for (int i = 0; i < num_workers; i++) {
        if (workers[i].base.type == new_worker.base.type && workers[i].is_primary_worker) {
            ja_te_principal = 1;
            break;
        }
    }

    // Redimensionar l’array
    ConfigWorker *temp = realloc(workers, (num_workers + 1) * sizeof(ConfigWorker));
    if (!temp) {
        log_event("Error d'allocació de memòria per a un nou worker.");
        pthread_mutex_unlock(&worker_mutex);
        return;
    }

    workers = temp;

    // Assignar i copiar dades del nou worker
    workers[num_workers] = new_worker;
    workers[num_workers].base.ip = strdup(new_worker.base.ip);
    workers[num_workers].ip_fleck = strdup(new_worker.ip_fleck);
    workers[num_workers].base.port = new_worker.base.port;
    workers[num_workers].port_fleck = new_worker.port_fleck;
    workers[num_workers].worker_id = num_workers + 1;
    workers[num_workers].last_heartbeat = time(NULL);
    workers[num_workers].is_primary_worker = (ja_te_principal == 0);

    // Log de confirmació
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


void *handle_worker(void *arg) {
    int worker_socket = *(int *)arg;
    free(arg);

    Trama trama;

    while (read(worker_socket, &trama, sizeof(trama)) == sizeof(trama)) {
        if (trama.type == 0x02) {
            char *workerType = strtok(trama.data, "&");
            char *workerIP = strtok(NULL, "&");
            char *workerPortStr = strtok(NULL, "&");

            if (!workerType || !workerIP || !workerPortStr) {
                write_message(STDOUT_FILENO, "Trama de worker mal formada.\n");
                close(worker_socket);
                continue;
            }

            ConfigWorker worker;
            worker.base.socket_fd = worker_socket;
            worker.base.ip = strdup(config.base.ip);
            worker.base.port = config.base.port;
            worker.ip_fleck = strdup(workerIP);
            worker.port_fleck = atoi(workerPortStr);
            worker.base.type = (strcasecmp(workerType, "TEXT") == 0) ? ENIGMA : HARLEY;

            int ha_de_ser_principal = 1;
            pthread_mutex_lock(&worker_mutex);
            for (int i = 0; i < num_workers; i++) {
                if (workers[i].base.type == worker.base.type && workers[i].is_primary_worker) {
                    ha_de_ser_principal = 0;
                    break;
                }
            }
            worker.is_primary_worker = ha_de_ser_principal;

            ConfigWorker *temp = realloc(workers, (num_workers + 1) * sizeof(ConfigWorker));
            if (!temp) {
                pthread_mutex_unlock(&worker_mutex);
                close(worker_socket);
                return NULL;
            }
            workers = temp;
            workers[num_workers] = worker;
            num_workers++;
            pthread_mutex_unlock(&worker_mutex);

            // Mensaje tipo
            const char *tipus = (worker.base.type == ENIGMA) ? "ENIGMA" : "HARLEY";
            int len1 = snprintf(NULL, 0, "Worker registrat com a tipus: %s\n", tipus) + 1;
            char *msg1 = malloc(len1);
            if (msg1) {
                snprintf(msg1, len1, "Worker registrat com a tipus: %s\n", tipus);
                write_message(STDOUT_FILENO, msg1);
                free(msg1);
            }

            // Envia CON_OK
            Trama resposta;
            inicialitzar_trama(&resposta, 0x02, "");
            write(worker_socket, &resposta, sizeof(resposta));

            // Mensaje registro
            int len2 = snprintf(NULL, 0, "Worker registrat: %s:%d (type: %s)\n", workerIP, worker.port_fleck, workerType) + 1;
            char *msg2 = malloc(len2);
            if (msg2) {
                snprintf(msg2, len2, "Worker registrat: %s:%d (type: %s)\n", workerIP, worker.port_fleck, workerType);
                write_message(STDOUT_FILENO, msg2);
                free(msg2);
            }

            // Promocionar a principal
            if (worker.is_primary_worker) {
                Trama promotora;
                inicialitzar_trama(&promotora, 0x08, "");
                write(worker_socket, &promotora, sizeof(promotora));
                write_message(STDOUT_FILENO, "Trama 0x08 enviada al worker per promoure'l a principal\n");
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

            // Aquí se puede añadir la lógica de eliminarlo y reasignar nou principal

        } else if (trama.type == 0x12) {
            int len5 = snprintf(NULL, 0, "Worker %s ha enviat un heartbeat\n", trama.data) + 1;
            char *msg5 = malloc(len5);
            if (msg5) {
                snprintf(msg5, len5, "Worker %s ha enviat un heartbeat\n", trama.data);
                write_message(STDOUT_FILENO, msg5);
                free(msg5);
            }

            pthread_mutex_lock(&worker_mutex);
            for (int i = 0; i < num_workers; i++) {
                if (strcmp(workers[i].ip_fleck, trama.data) == 0) {
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

void *gestionar_client_fleck(void *arg) {
    int client_socket = *(int *)arg;
    free(arg);  // Liberar la memoria del socket pasado por puntero

    Trama trama;
    while (read(client_socket, &trama, sizeof(trama)) == sizeof(trama)) {

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
            char *ext = strrchr(fileName, '.');
            if (!ext) {
                inicialitzar_trama(&resposta, 0x10, "MEDIA_KO");
                write(client_socket, &resposta, sizeof(resposta));
                continue;
            }

            int tipus_correcte = 0;
            if (strcmp(mediaType, "media") == 0) {
                const char *media_exts[] = {".wav", ".mp3", ".jpg", ".png"};
                for (size_t i = 0; i < sizeof(media_exts)/sizeof(media_exts[0]); i++) {
                    if (strcasecmp(ext, media_exts[i]) == 0) {
                        tipus_correcte = 1;
                        break;
                    }
                }
            } else if (strcmp(mediaType, "text") == 0) {
                if (strcasecmp(ext, ".txt") == 0) tipus_correcte = 1;
            }

            if (!tipus_correcte) {
                inicialitzar_trama(&resposta, 0x10, "MEDIA_KO");
                write(client_socket, &resposta, sizeof(resposta));
                continue;
            }

            // Buscar un worker disponible del tipus sol·licitat
            pthread_mutex_lock(&worker_mutex);
            ConfigWorker *found = NULL;
            for (int i = 0; i < num_workers; i++) {
                int tipus_worker = workers[i].base.type;
                if ((strcmp(mediaType, "media") == 0 && tipus_worker == HARLEY) ||
                    (strcmp(mediaType, "text") == 0 && tipus_worker == ENIGMA)) {
                    found = &workers[i];
                    break;
                }
            }
            pthread_mutex_unlock(&worker_mutex);

            if (found) {
                char resposta_data[64];
                snprintf(resposta_data, sizeof(resposta_data), "%s&%d", found->ip_fleck, found->port_fleck);
                inicialitzar_trama(&resposta, 0x10, resposta_data);
            } else {
                inicialitzar_trama(&resposta, 0x10, "DISTORT_KO");
            }
            write(client_socket, &resposta, sizeof(resposta));

        }

    }

    close(client_socket);
    return NULL;
}

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

    pthread_join(fleck_thread, NULL);
    pthread_join(worker_thread, NULL);
    return 0;
}
