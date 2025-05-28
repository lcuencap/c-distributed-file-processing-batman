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
ConfigWorker *workers = NULL;
int num_workers = 0;
pthread_mutex_t worker_mutex = PTHREAD_MUTEX_INITIALIZER;

void afegir_worker(ConfigWorker new_worker) {
    pthread_mutex_lock(&worker_mutex);
    int ja_te_principal = 0;
    for (int i = 0; i < num_workers; i++) {
        if (workers[i].base.type == new_worker.base.type && workers[i].is_primary_worker) {
            ja_te_principal = 1;
            break;
        }
    }

    ConfigWorker *temp = realloc(workers, (num_workers + 1) * sizeof(ConfigWorker));
    if (!temp) {
        pthread_mutex_unlock(&worker_mutex);
        return;
    }
    
    workers = temp;
    workers[num_workers] = new_worker;
    workers[num_workers].is_primary_worker = (ja_te_principal == 0);  
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
                const char *msg = "Trama de worker mal formada.\n";
                write(STDOUT_FILENO, msg, strlen(msg));
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

            // Promoció a principal si no n’hi ha cap del mateix tipus
            int ha_de_ser_principal = 1;

            pthread_mutex_lock(&worker_mutex);
            for (int i = 0; i < num_workers; i++) {
                if (workers[i].base.type == worker.base.type && workers[i].is_primary_worker) {
                    ha_de_ser_principal = 0;
                    break;
                }
            }
            worker.is_primary_worker = ha_de_ser_principal;

            // Afegeix a la llista
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
            char buf[64];
            snprintf(buf, sizeof(buf), "Worker registrat com a tipus: %s\n", worker.base.type == ENIGMA ? "ENIGMA" : "HARLEY");
            write(STDOUT_FILENO, buf, strlen(buf));


            // Envia CON_OK
            Trama resposta;
            inicialitzar_trama(&resposta, 0x02, "");
            write(worker_socket, &resposta, sizeof(resposta));
            char buf[128];
            snprintf(buf, sizeof(buf), "Worker registrat: %s:%d (type: %s)\n", workerIP, worker.port_fleck, workerType);
            write(STDOUT_FILENO, buf, strlen(buf));

            // Envia 0x08 si és principal
            if (worker.is_primary_worker) {
                Trama promotora;
                inicialitzar_trama(&promotora, 0x08, "");
                write(worker_socket, &promotora, sizeof(promotora));
                const char *msg = "Trama 0x08 enviada al worker per promoure'l a principal\n";
                write(STDOUT_FILENO, msg, strlen(msg));
            }
        } else if (trama.type == 0x07) {
            char buf[64];
            snprintf(buf, sizeof(buf), "Worker %s desconnectat\n", trama.data);
            write(STDOUT_FILENO, buf, strlen(buf));
            printf("Worker %s desconnectat\n", trama.data);
            char buf[64];
            snprintf(buf, sizeof(buf), "Desconnectant worker %d\n", trama.type);
            write(STDOUT_FILENO, buf, strlen(buf));

            // Eliminar worker del tipus corresponent
            // Si era principal → assignar nou principal i enviar 0x08 al nou
        } else if (trama.type == 0x12) {
            char buf[64];
            snprintf(buf, sizeof(buf), "Worker %s ha enviat un heartbeat\n", trama.data);
            write(STDOUT_FILENO, buf, strlen(buf));
            // Actualitzar timestamp de l'últim heartbeat
            pthread_mutex_lock(&worker_mutex);
            for (int i = 0; i < num_workers; i++) {
                if (strcmp(workers[i].ip_fleck, trama.data) == 0) {
                    workers[i].last_heartbeat = time(NULL);
                    break;
                }
            }
            pthread_mutex_unlock(&worker_mutex);
        } else{
            char buf[64];
            snprintf(buf, sizeof(buf), "Trama desconeguda rebuda del worker: 0x%02X\n", trama.type);
            write(STDOUT_FILENO, buf, strlen(buf));
            // Trama desconeguda → enviar 0x09
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
