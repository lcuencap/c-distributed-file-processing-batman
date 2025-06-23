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

#define INITIAL_CAPACITY 32
#define TIMEOUT_SECONDS 20

ConfigGotham config;
pthread_t fleck_thread, worker_thread;
pthread_t task_thread;


ConfigWorker *workers = NULL;
int num_workers = 0;
int next_worker_id = 0;
pthread_mutex_t worker_mutex = PTHREAD_MUTEX_INITIALIZER;

ConfigFleck *flecks = NULL;
int num_flecks = 0;
int next_fleck_id = 0;
pthread_mutex_t fleck_mutex = PTHREAD_MUTEX_INITIALIZER;


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


void inicialitzar_arkham_logger() {
    if (pipe(log_pipe_fd) == -1) {
        write_message(STDERR_FILENO, "Error creant la pipe per Arkham\n");
        exit(EXIT_FAILURE);
    }

    pid_t pid = fork();
    if (pid < 0) {
        write_message(STDERR_FILENO, "Error en fork per Arkham\n");
        exit(EXIT_FAILURE);
    }

    if (pid == 0) { // Procés fill: Arkham
        close(log_pipe_fd[1]); // Tanca escriptura

        int log_fd = open("logs.txt", O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (log_fd < 0) {
            write_message(STDERR_FILENO, "Error obrint logs.txt\n");
            exit(EXIT_FAILURE);
        }

        signal(SIGINT, SIG_IGN);
        signal(SIGTERM, SIG_IGN);

        char buffer[512];
        ssize_t bytes_read;
        while ((bytes_read = read(log_pipe_fd[0], buffer, sizeof(buffer))) > 0) {
            write(log_fd, buffer, bytes_read);
        }

        close(log_fd);
        close(log_pipe_fd[0]);
        exit(0);
    }

    // Procés pare: Gotham
    arkham_pid = pid;
    close(log_pipe_fd[0]); // Tanca lectura
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
 * @param id ID del worker a eliminar.   
 * @return Retorna 1 si s'ha eliminat correctament, 0 si no s'ha trobat el worker.
 */

void eliminar_worker_per_id(int id) {
    for (int i = 0; i < num_workers; i++) {
        if (workers[i].worker_id == id) {
            int tipus_worker = workers[i].base.type;
            int era_principal = workers[i].is_primary_worker;
            free(workers[i].base.ip);
            free(workers[i].ip_fleck);

            for (int j = i; j < num_workers - 1; j++) {
                workers[j] = workers[j + 1];
            }

            num_workers--;
            // Redimensionar l'array de workers
            ConfigWorker *tmp = realloc(workers, num_workers * sizeof(ConfigWorker));
            if (tmp || num_workers == 0) {
                workers = tmp;
            } else {
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
    workers[num_workers].worker_id = new_worker.worker_id;
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
    next_worker_id++;

    pthread_mutex_unlock(&worker_mutex);
}


/**
 * Thread que escolta les connexions dels workers.
 * Accepta connexions entrants i crea un nou thread per gestionar cada worker.
 */
void *listener_worker(void *arg) {
    (void)arg;
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        write_message(STDERR_FILENO, "[WORKER] Failed to create server socket.\n");
        log_event("[WORKER] Failed to create server socket.");
        exit(EXIT_FAILURE);
    }

    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_addr.s_addr = inet_addr(config.worker_ip),
        .sin_port = htons(config.worker_port)
    };

    if (bind(server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        write_message(STDERR_FILENO, "[WORKER] Failed to bind server socket.\n");
        log_event("[WORKER] Failed to bind server socket.");
        exit(EXIT_FAILURE);
    }

    if (listen(server_fd, 10) < 0) {
        write_message(STDERR_FILENO, "[WORKER] Failed to listen on server socket.\n");
        log_event("[WORKER] Failed to listen on server socket.");
        exit(EXIT_FAILURE);
    }

    size_t capacity = INITIAL_CAPACITY;
    struct pollfd *poll_fds = malloc(capacity * sizeof(struct pollfd));
    time_t *last_activity = malloc(capacity * sizeof(time_t));
    if (!poll_fds || !last_activity) {
        write_message(STDERR_FILENO, "[WORKER] Failed to allocate poll structures.\n");
        log_event("[WORKER] Failed to allocate poll structures.");
        exit(EXIT_FAILURE);
    }

    poll_fds[0].fd = server_fd;
    poll_fds[0].events = POLLIN;
    last_activity[0] = time(NULL);
    int nfds = 1;

    while (1) {
        int ready = poll(poll_fds, nfds, 5000);
        if (ready < 0) {
            write_message(STDERR_FILENO, "[WORKER] poll() system call failed.\n");
            log_event("[WORKER] poll() system call failed.");
            continue;
        }

        time_t now = time(NULL);
        for (int i = 1; i < nfds; i++) {
            if (difftime(now, last_activity[i]) > TIMEOUT_SECONDS) {
                close(poll_fds[i].fd);
                pthread_mutex_lock(&worker_mutex);
                for (int w = 0; w < num_workers; w++) {
                    if (workers[w].base.socket_fd == poll_fds[i].fd) {
                        eliminar_worker_per_id(workers[w].worker_id);
                        break;
                    }
                }
                pthread_mutex_unlock(&worker_mutex);
                poll_fds[i] = poll_fds[nfds - 1];
                last_activity[i] = last_activity[nfds - 1];
                nfds--;
                i--;
            }
        }

        for (int i = 0; i < nfds && ready > 0; i++) {
            if (!(poll_fds[i].revents & POLLIN)) continue;
            ready--;

            if (poll_fds[i].fd == server_fd) {
                int client_fd = accept(server_fd, NULL, NULL);
                if (client_fd >= 0) {
                    if (nfds >= (int)capacity) {
                        capacity *= 2;
                        poll_fds = realloc(poll_fds, capacity * sizeof(struct pollfd));
                        last_activity = realloc(last_activity, capacity * sizeof(time_t));
                        if (!poll_fds || !last_activity) {
                            write_message(STDERR_FILENO, "[WORKER] Failed to reallocate poll structures.\n");
                            log_event("[WORKER] Failed to allocate poll structures.");
                            exit(EXIT_FAILURE);
                        }
                    }
                    poll_fds[nfds].fd = client_fd;
                    poll_fds[nfds].events = POLLIN;
                    last_activity[nfds] = time(NULL);
                    nfds++;
                }
                continue;
            }

            int client_fd = poll_fds[i].fd;
            Trama trama;
            ssize_t len = read(client_fd, &trama, sizeof(trama));
            if (len != sizeof(trama)) {
                close(client_fd);
                poll_fds[i] = poll_fds[nfds - 1];
                last_activity[i] = last_activity[nfds - 1];
                nfds--;
                i--;
                continue;
            }

            last_activity[i] = time(NULL);

            if (!validar_trama(&trama)) {
                respondre_trama_erronia(client_fd, "Worker");
                continue;
            }

            if (trama.type == 0x02) {
                char *workerType = strtok(trama.data, "&");
                char *workerIP = strtok(NULL, "&");
                char *workerPortStr = strtok(NULL, "&");

                if (!workerType || !workerIP || !workerPortStr || atoi(workerPortStr) <= 0) {
                    Trama resposta;
                    inicialitzar_trama(&resposta, 0x02, "CON_KO");
                    write(client_fd, &resposta, sizeof(resposta));
                    close(client_fd);
                    poll_fds[i] = poll_fds[nfds - 1];
                    last_activity[i] = last_activity[nfds - 1];
                    nfds--;
                    i--;
                    continue;
                }

                ConfigWorker new_worker = {
                    .base.socket_fd = client_fd,
                    .base.ip = strdup(config.base.ip),
                    .base.port = config.base.port,
                    .ip_fleck = strdup(workerIP),
                    .port_fleck = atoi(workerPortStr),
                    .base.type = (strcasecmp(workerType, "TEXT") == 0) ? ENIGMA : HARLEY,
                    .worker_id = next_worker_id++,
                    .last_heartbeat = time(NULL)
                };
                afegir_worker(new_worker);

            } else if (trama.type == 0x07) {
                pthread_mutex_lock(&worker_mutex);
                for (int w = 0; w < num_workers; w++) {
                    if (workers[w].base.socket_fd == client_fd) {
                        eliminar_worker_per_id(workers[w].worker_id);
                        break;
                    }
                }
                pthread_mutex_unlock(&worker_mutex);
                close(client_fd);
                poll_fds[i] = poll_fds[nfds - 1];
                last_activity[i] = last_activity[nfds - 1];
                nfds--;
                i--;
            } else if (trama.type == 0x12) {
                pthread_mutex_lock(&worker_mutex);
                for (int w = 0; w < num_workers; w++) {
                    if (workers[w].base.socket_fd == client_fd) {
                        workers[w].last_heartbeat = time(NULL);
                        break;
                    }
                }
                pthread_mutex_unlock(&worker_mutex);
            } else {
                Trama error_trama;
                inicialitzar_trama(&error_trama, 0x09, "");
                write(client_fd, &error_trama, sizeof(error_trama));
            }
        }
    }

    free(poll_fds);
    free(last_activity);
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
 * Afegeix un nou fleck a la llista global de flecks.
 * @param fleck Punter al ConfigFleck on s'ompliran les dades.
 * @param socket Socket del fleck.
 * @param data Dades del fleck en format "<username>&<ip>&<port>".
 * @return Retorna 0 si s'ha afegit correctament, -1 si hi ha errors.
 */
int addFleck(ConfigFleck *fleck, int socket, const char *data) {
    if (!data || socket <= 0 || !fleck) {
        write_message(STDERR_FILENO, "Error: dades, socket o punter fleck no vàlids.\n");
        return -1;
    }

    char data_copy[256];
    strncpy(data_copy, data, sizeof(data_copy));
    data_copy[sizeof(data_copy) - 1] = '\0';

    char *username = strtok(data_copy, "&");
    char *ip = strtok(NULL, "&");
    char *port_str = strtok(NULL, "&");

    if (!username || !ip || !port_str) {
        write_message(STDERR_FILENO, "Error: format de dades de Fleck incorrecte.\n");
        return -1;
    }

    int port = atoi(port_str);
    if (port <= 0) {
        write_message(STDERR_FILENO, "Error: port de Fleck invàlid.\n");
        return -1;
    }

    // Omplim el punter passat per paràmetre
    fleck->base.socket_fd = socket;
    fleck->base.type = FLECK;
    fleck->fleck_id = next_fleck_id++;
    fleck->base.ip = strdup(ip);
    fleck->base.port = port;
    fleck->username = strdup(username);
    fleck->work_dir = NULL;
    fleck->last_heartbeat = time(NULL);

    // S'afegeix a la llista global
    pthread_mutex_lock(&fleck_mutex);
    flecks = realloc(flecks, (num_flecks + 1) * sizeof(ConfigFleck));
    if (!flecks) {
        pthread_mutex_unlock(&fleck_mutex);
        write_message(STDERR_FILENO, "Error reservant memòria per a Fleck.\n");
        return -1;
    }

    flecks[num_flecks] = *fleck; // copiem el contingut del fleck al nou espai
    num_flecks++;
    pthread_mutex_unlock(&fleck_mutex);

    return 0;
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
 * Funció de neteja que s'executa quan es rep un senyal SIGINT (Ctrl+C).
 * Tanca tots els sockets oberts i allibera la memòria.
 * @param sig Senyal rebut (no s'utilitza aquí, però és necessari per a la signatura).
 */
void cleanup(int sig) {
    (void)sig;
    log_event("S’ha rebut SIGINT. Tancant Gotham netament.");

    // Tancar sockets de workers
    pthread_mutex_lock(&worker_mutex);
    for (int i = 0; i < num_workers; i++) {
        close(workers[i].base.socket_fd);
        free(workers[i].base.ip);
        free(workers[i].ip_fleck);
    }
    free(workers);
    pthread_mutex_unlock(&worker_mutex);

    // Tancar sockets de flecks
    pthread_mutex_lock(&fleck_mutex);
    for (int i = 0; i < num_flecks; i++) {
        close(flecks[i].base.socket_fd);
        free(flecks[i].base.ip);
        free(flecks[i].username);
    }
    free(flecks);
    pthread_mutex_unlock(&fleck_mutex);

    kill(arkham_pid, SIGTERM);
    close(log_pipe_fd[1]);  // Tanca la pipe

    log_event("Tots els recursos s’han alliberat. Adeu.");
    exit(0);
}

/**
 * Thread que escolta les connexions dels clients Fleck.
 * Accepta connexions entrants i crea un nou thread per gestionar cada client.
 */
void *listener_fleck(void *arg) {
    (void)arg;
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        write_message(STDERR_FILENO, "[FLECK] Failed to create server socket.\n");
        log_event("[FLECK] Failed to create server socket.");
        exit(EXIT_FAILURE);
    }

    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_addr.s_addr = inet_addr(config.base.ip),
        .sin_port = htons(config.base.port)
    };

    if (bind(server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        write_message(STDERR_FILENO, "[FLECK] Failed to bind server socket.\n");
        log_event("[FLECK] Failed to bind server socket.");
        exit(EXIT_FAILURE);
    }

    if (listen(server_fd, 10) < 0) {
        write_message(STDERR_FILENO, "[FLECK] Failed to listen on server socket.\n");
        log_event("[FLECK] Failed to listen on server socket.");
        exit(EXIT_FAILURE);
    }

    size_t capacity = INITIAL_CAPACITY;
    struct pollfd *poll_fds = malloc(capacity * sizeof(struct pollfd));
    if (!poll_fds) {
        write_message(STDERR_FILENO, "[FLECK] Failed to allocate poll_fds array.\n");
        log_event("[FLECK] Failed to allocate poll_fds array.");
        exit(EXIT_FAILURE);
    }

    poll_fds[0].fd = server_fd;
    poll_fds[0].events = POLLIN;
    int nfds = 1;

    while (1) {
        int ready = poll(poll_fds, nfds, 5000);
        if (ready < 0) {
            write_message(STDERR_FILENO, "[FLECK] poll() system call failed.\n");
            log_event("[FLECK] poll() system call failed.");
            continue;
        }

        for (int i = 0; i < nfds && ready > 0; i++) {
            if (!(poll_fds[i].revents & POLLIN)) continue;
            ready--;

            if (poll_fds[i].fd == server_fd) {
                int client_fd = accept(server_fd, NULL, NULL);
                if (client_fd >= 0) {
                    if (nfds >= (int)capacity) {
                        capacity *= 2;
                        struct pollfd *new_fds = realloc(poll_fds, capacity * sizeof(struct pollfd));
                        if (!new_fds) {
                            write_message(STDERR_FILENO, "[FLECK] Failed to reallocate poll_fds array.\n");
                            log_event("[FLECK] Failed to reallocate poll_fds array.");
                            close(client_fd);
                            continue;
                        }
                        poll_fds = new_fds;
                    }
                    poll_fds[nfds].fd = client_fd;
                    poll_fds[nfds].events = POLLIN;
                    nfds++;
                }
                continue;
            }

            int client_fd = poll_fds[i].fd;
            Trama trama;
            ssize_t len = read(client_fd, &trama, sizeof(trama));
            if (len != sizeof(trama)) {
                close(client_fd);
                poll_fds[i] = poll_fds[nfds - 1];
                nfds--;
                i--;
                continue;
            }

            if (!validar_trama(&trama)) {
                Trama resposta;
                inicialitzar_trama(&resposta, 0x09, "");
                write(client_fd, &resposta, sizeof(resposta));
                continue;
            }

            if (trama.type == 0x01) {
                ConfigFleck newFleck;
                if (addFleck(&newFleck, client_fd, trama.data) < 0) {
                    Trama resposta_ko;
                    inicialitzar_trama(&resposta_ko, 0x01, "CON_KO");
                    write(client_fd, &resposta_ko, sizeof(resposta_ko));
                    close(client_fd);
                    poll_fds[i] = poll_fds[nfds - 1];
                    nfds--;
                    i--;
                    continue;
                }

                Trama resposta_ok;
                inicialitzar_trama(&resposta_ok, 0x01, "");
                write(client_fd, &resposta_ok, sizeof(resposta_ok));

                char log_msg[256];
                snprintf(log_msg, sizeof(log_msg), "Fleck connectat: %s (Socket:%d)", newFleck.username, newFleck.base.socket_fd);
                log_event(log_msg);

                char msg[256];
                snprintf(msg, sizeof(msg), "%s connected to Mr. J System. Let the chaos begin!:)", newFleck.username);
                write(STDOUT_FILENO, msg, strlen(msg));
            } else if (trama.type == 0x07) {
                log_event("Fleck ha sol·licitat la desconnexió.");
                close(client_fd);
                poll_fds[i] = poll_fds[nfds - 1];
                nfds--;
                i--;
            } else if (trama.type == 0x12) {
                pthread_mutex_lock(&fleck_mutex);
                for (int f = 0; f < num_flecks; f++) {
                    if (flecks[f].base.socket_fd == client_fd) {
                        flecks[f].last_heartbeat = time(NULL);
                        break;
                    }
                }
                pthread_mutex_unlock(&fleck_mutex);
            } else {
                Trama resposta_error;
                inicialitzar_trama(&resposta_error, 0x09, "");
                write(client_fd, &resposta_error, sizeof(resposta_error));
            }
        }
    }

    free(poll_fds);
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
        write_message(STDERR_FILENO, "Error en llegir el fitxer de configuració.\n");
        return 1;
    }
    close(fd);

    inicialitzar_arkham_logger();
    log_event("Logger Arkham inicialitzat correctament.");



    signal(SIGINT, cleanup);

    if (pthread_create(&fleck_thread, NULL, listener_fleck, NULL) == 0) {
    } else {
        write_message(STDOUT_FILENO, "[ERROR] No s'ha pogut crear el thread listener_fleck.\n");
    }

    if (pthread_create(&worker_thread, NULL, listener_worker, NULL) == 0) {
    } else {
        write_message(STDOUT_FILENO, "[ERROR] No s'ha pogut crear el thread listener_worker.\n");
    }


    pthread_join(fleck_thread, NULL);
    pthread_join(worker_thread, NULL);
    return 0;
}
