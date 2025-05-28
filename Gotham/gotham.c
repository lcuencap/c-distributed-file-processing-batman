#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <semaphore.h>
#include <signal.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/wait.h>
#include "../Utils/file_utils.h"
#include "../Utils/sys_params.h"
#include "../Utils/trames.h"
#include "../Utils/task_manager.h"


// Variables globals
ConfigWorker *workers = NULL; // Llista dinàmica de workers
int num_workers = 0;          // Nombre de workers actius
pthread_mutex_t worker_mutex = PTHREAD_MUTEX_INITIALIZER;

TaskManager task_manager;     // Gestor de tasques centralitzat
int arkham_pid;
int log_pipe_fd[2];           // Pipe per al logger Arkham
pthread_mutex_t arkham_mutex = PTHREAD_MUTEX_INITIALIZER;

ConfigGotham config;          // Configuració de Gotham
int server_fd = -1;           // Descriptor del socket del servidor

pthread_t task_thread;
pthread_t monitor_thread;
pthread_t fleck_thread;
pthread_t worker_thread;


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
 * Afegeix un nou worker a la llista i estableix un worker principal si és el primer del seu tipus.
 * @param new_worker Configuració del nou worker.
 */
void afegir_worker(ConfigWorker new_worker) {
    pthread_mutex_lock(&worker_mutex);


    // Validar que new_worker.ip_fleck no sea NULL antes de hacer strdup
    if (!new_worker.ip_fleck) {
        log_event("Error: Worker intentant registrar-se amb IP Fleck NULL.");
        pthread_mutex_unlock(&worker_mutex);
        return;
    }


    // Comprobar si ya existe un worker con el mismo socket (reconexión)
    for (int i = 0; i < num_workers; i++) {
        if (workers[i].base.socket_fd == new_worker.base.socket_fd) {
            log_event("Intent d'afegir un worker duplicat. No es permet.");
            pthread_mutex_unlock(&worker_mutex);
            return;
        }
    }

    
    // Reservar espacio para el nuevo worker
    ConfigWorker *temp = realloc(workers, (num_workers + 1) * sizeof(ConfigWorker));
    if (!temp) {
        log_event("Error d'allocació de memòria per a un nou worker.");
        pthread_mutex_unlock(&worker_mutex);
        return;
    }

    workers = temp;
    workers[num_workers] = new_worker;
    workers[num_workers].base.ip = strdup(new_worker.base.ip);  // IP de Gotham
    workers[num_workers].base.port = new_worker.base.port;      // Puerto de Gotham
    workers[num_workers].ip_fleck = strdup(new_worker.ip_fleck);  // IP donde Fleck se conectará
    workers[num_workers].port_fleck = new_worker.port_fleck;    // Puerto donde Fleck se conectará
    workers[num_workers].worker_id = num_workers + 1;           // Generar un ID único
    workers[num_workers].last_heartbeat = time(NULL);

    // Verificar si es el primer worker del tipo y asignarlo como principal
    int is_first_of_type = 1;
    for (int i = 0; i < num_workers; i++) {
        if (workers[i].base.type == new_worker.base.type && workers[i].is_primary_worker) {
            is_first_of_type = 0;
            break;
        }
    }
    workers[num_workers].is_primary_worker = is_first_of_type;

    num_workers++;

    char log_msg[256];
    snprintf(log_msg, sizeof(log_msg), 
             "Worker ID: %d, Tipus: %d, IP Gotham: %s, IP Fleck: %s, Port Fleck: %d, Principal: %d",
             workers[num_workers - 1].worker_id, new_worker.base.type, 
             new_worker.base.ip, new_worker.ip_fleck, new_worker.port_fleck, is_first_of_type);
    log_event(log_msg);

    pthread_mutex_unlock(&worker_mutex);
}




/**
 * Elimina un worker de la llista.
 * @param socket_fd Socket del worker a eliminar.
 */
void eliminar_worker(int socket_fd) {
    pthread_mutex_lock(&worker_mutex);

    int found = -1;
    for (int i = 0; i < num_workers; i++) {
        if (workers[i].base.socket_fd == socket_fd) {
            found = i;
            break;
        }
    }

    if (found == -1) {
        pthread_mutex_unlock(&worker_mutex);
        log_event("No s'ha trobat el worker a eliminar.");
        return;
    }

    close(workers[found].base.socket_fd);
    
    if (workers[found].base.ip) free(workers[found].base.ip);
    if (workers[found].ip_fleck) free(workers[found].ip_fleck);

    for (int j = found; j < num_workers - 1; j++) {
        workers[j] = workers[j + 1];
    }
    

    num_workers--;

    if (num_workers > 0) {
        ConfigWorker *temp = realloc(workers, num_workers * sizeof(ConfigWorker));
        if (temp) workers = temp;
    } else {
        free(workers);
        workers = NULL;
    }

    log_event("Worker eliminat correctament.");
    pthread_mutex_unlock(&worker_mutex);
}



/**
 * Afegeix una tasca a la cua i actualitza l'arxiu de persistència.
 * @param task Tasca a afegir.
 */
void afegir_tasca(Task task) {
    addTask(&task_manager, task);
    log_event("Tasca afegida a la llista de tasques");

    // Guardar en fitxer temporal abans de substituir l'arxiu principal
    int temp_fd = open("tasks_backup.tmp", O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (temp_fd >= 0) {
        Task *current = task_manager.head;
        while (current) {
            write(temp_fd, current, sizeof(Task));
            current = current->next;
        }
        close(temp_fd);
        rename("tasks_backup.tmp", "tasks_backup.dat");  // Substituir fitxer original
    } else {
        log_event("Error obrint tasks_backup.tmp per a actualitzar tasques.");
    }
}


/**
 * Elimina totes les tasques associades a un client (Fleck) que s'ha desconnectat i actualitza l'arxiu de persistència.
 * @param client_socket Descriptor del socket del client desconnectat.
 */
void eliminar_tasques_fleck(int client_socket) {
    pthread_mutex_lock(&task_manager.mutex);
    Task *current = task_manager.head;

    while (current) {
        Task *next = current->next;
        if (current->client_socket == client_socket) {
            removeTask(&task_manager, current);
        }
        current = next;
    }
    pthread_mutex_unlock(&task_manager.mutex);

    char msg[128];
    snprintf(msg, sizeof(msg), "Tasques eliminades per client desconnectat (socket: %d)", client_socket);
    log_event(msg);

    // Actualitzar el fitxer `tasks_backup.dat`
    int task_backup_fd = open("tasks_backup.dat", O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (task_backup_fd >= 0) {
        current = task_manager.head;
        while (current) {
            write(task_backup_fd, current, sizeof(Task));
            current = current->next;
        }
        close(task_backup_fd);
    } else {
        log_event("Error obrint tasks_backup.dat per a actualitzar després d'eliminar tasques.");
    }
}

/**
 * Notifica a un worker que ara és el principal del seu tipus.
 * @param worker_socket Socket del nou worker principal.
 */
void notificar_worker_principal(int worker_socket) {
    Trama notify_trama;
    inicialitzar_trama(&notify_trama, 0x08, "PRINCIPAL_ASSIGNED");

    if (write(worker_socket, &notify_trama, sizeof(notify_trama)) != sizeof(notify_trama)) {
        log_event("Error enviant notificació de worker principal");
    } else {
        log_event("Nou worker principal notificat correctament");
    }
}

/**
 * Gestiona la caiguda d'un worker i designa un nou worker principal si és necessari.
 * Si no hi ha cap worker disponible, es notifica a Fleck amb DISTORT_KO i s'actualitza l'arxiu de persistència.
 * @param socket_fd Socket del worker caigut.
 */
void redistribuir_tasques_worker(int socket_fd) {
    pthread_mutex_lock(&worker_mutex);

    int found = -1;
    int was_primary = 0;
    InstanceType worker_type;

    for (int i = 0; i < num_workers; i++) {
        if (workers[i].base.socket_fd == socket_fd) {
            was_primary = workers[i].is_primary_worker;
            worker_type = workers[i].base.type;
            found = i;
            break;
        }
    }

    if (found != -1) {
        close(workers[found].base.socket_fd);
        free(workers[found].base.ip);

        for (int j = found; j < num_workers - 1; j++) {
            workers[j] = workers[j + 1];
        }
        num_workers--;

        ConfigWorker *temp = realloc(workers, num_workers * sizeof(ConfigWorker));
        if (temp || num_workers == 0) workers = temp;
    }

    if (was_primary) {
        int new_primary_found = 0;
        for (int i = 0; i < num_workers; i++) {
            if (workers[i].base.type == worker_type) {
                workers[i].is_primary_worker = 1;
                new_primary_found = 1;
                notificar_worker_principal(workers[i].base.socket_fd);
                char log_msg[128];
                snprintf(log_msg, sizeof(log_msg), "Nou worker principal assignat per tipus %d", worker_type);
                log_event(log_msg);
                break;
            }
        }

        if (!new_primary_found) {
            pthread_mutex_lock(&task_manager.mutex);
            int task_eliminated = 0;
            Task *current = task_manager.head;
            while (current) {
                if (current->worker_socket == socket_fd) {
                    Trama notify_trama;
                    inicialitzar_trama(&notify_trama, 0x12, "DISTORT_KO");
                    write(current->client_socket, &notify_trama, sizeof(notify_trama));
                    close(current->client_socket);
                    task_eliminated = 1;
                }
                current = current->next;
            }
            pthread_mutex_unlock(&task_manager.mutex);

            if (task_eliminated) {
                int temp_fd = open("tasks_backup.tmp", O_WRONLY | O_CREAT | O_TRUNC, 0644);
                if (temp_fd >= 0) {
                    current = task_manager.head;
                    while (current) {
                        write(temp_fd, current, sizeof(Task));
                        current = current->next;
                    }
                    close(temp_fd);
                    rename("tasks_backup.tmp", "tasks_backup.dat");
                }
            }
        }
    }

    log_event("Worker desconnectat i gestió completada.");
    pthread_mutex_unlock(&worker_mutex);
}


/**
 * Inicialitza la llista dinàmica de workers.
 */
void inicialitzar_workers() {
    workers = NULL;
    num_workers = 0;
}


/**
 * Obté la següent tasca de la cua.
 * @return Tasca següent.
 */
Task obtenir_tasca() {
    pthread_mutex_lock(&task_manager.mutex);

    Task *task = task_manager.head;
    if (task) {
        removeTask(&task_manager, task);
    }

    pthread_mutex_unlock(&task_manager.mutex);

    return task ? *task : (Task){ 0 }; 
}

/**
 * Verifica si un socket (Fleck) està desconnectat.
 * @param socket_fd Descriptor del socket del Fleck.
 * @return 1 si està desconnectat, 0 si està actiu.
 */
int verificar_fleck(int socket_fd) {
    char buffer[1];
    int result = recv(socket_fd, buffer, sizeof(buffer), MSG_PEEK | MSG_DONTWAIT);
    if (result == 0 || (result < 0 && errno != EAGAIN && errno != EWOULDBLOCK)) {
        return 1; // Fleck desconnectat
    }
    return 0; // Fleck actiu
}


/**
 * Processa una tasca enviada per un Fleck.
 * Assigna només a un worker principal del tipus correcte. Si no hi ha principal, assigna un nou.
 * Si no hi ha cap worker disponible, envia un DISTORT_KO a Fleck.
 * @param task Tasca a processar.
 */
void processar_tasca(Task task) {
    pthread_mutex_lock(&worker_mutex);
    int worker_index = -1;

    // Determinar el tipo de media basándose en la extensión del archivo
    int es_text = (strstr(task.file_name, ".txt") != NULL);
    int es_media = (strstr(task.file_name, ".wav") || strstr(task.file_name, ".mp3") ||
                    strstr(task.file_name, ".jpg") || strstr(task.file_name, ".png"));

    // Buscar un worker principal del tipo adecuado
    for (int i = 0; i < num_workers; i++) {
        if (workers[i].is_primary_worker && ((es_text && workers[i].base.type == ENIGMA) ||
             (es_media && workers[i].base.type == HARLEY))) {
            worker_index = i;
            break;
        }
    }

    // Si no hay worker principal disponible, enviar DISTORT_KO
    if (worker_index == -1) {
        pthread_mutex_unlock(&worker_mutex);
        Trama notify_trama;
        inicialitzar_trama(&notify_trama, 0x10, "DISTORT_KO");
        write(task.client_socket, &notify_trama, sizeof(notify_trama));
        close(task.client_socket);
        log_event("No hi ha workers disponibles. Enviat DISTORT_KO a Fleck.");
        return;
    }

    // Construir la resposta amb la IP i el port del worker assignat
    char resposta_data[128];
    snprintf(resposta_data, sizeof(resposta_data), "%s&%d", 
             workers[worker_index].ip_fleck, workers[worker_index].port_fleck);

    Trama resposta;
    inicialitzar_trama(&resposta, 0x10, resposta_data);

    // Enviar la resposta a Fleck
    if (write(task.client_socket, &resposta, sizeof(resposta)) != sizeof(resposta)) {
        log_event("Error enviant la IP i el port del worker a Fleck.");
    } else {
        log_event("Enviada la informació del worker a Fleck.");
    }

    pthread_mutex_unlock(&worker_mutex);
}



/**
 * Inicialitza el logger Arkham com un procés separat mitjançant fork().
 */
void inicialitzar_arkham_logger() {
    if (pipe(log_pipe_fd) == -1) {
        perror("Error creant la pipe per Arkham");
        exit(EXIT_FAILURE);
    }
    log_event("[ARKHAM] Pipe creada correctament.");

    pid_t pid = fork();
    if (pid < 0) {
        perror("Error en fork per Arkham");
        exit(EXIT_FAILURE);
    }

    if (pid == 0) { // Child process (Arkham)
        log_event("[ARKHAM] Procés fill iniciat.");
        
        close(log_pipe_fd[1]); // Tanca el costat d'escriptura del pipe en Arkham
        log_event("[ARKHAM] Costat d'escriptura del pipe tancat.");

        int log_fd = open("logs.txt", O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (log_fd < 0) {
            perror("Error obrint logs.txt");
            exit(EXIT_FAILURE);
        }
        log_event("[ARKHAM] Fitxer logs.txt obert correctament.");

        // Ignorar SIGINT i SIGTERM per evitar terminació accidental
        signal(SIGINT, SIG_IGN);
        signal(SIGTERM, SIG_IGN);
        signal(SIGUSR1, SIG_IGN);  // Ignora SIGUSR1

        char buffer[512];
        ssize_t bytes_read;
        while ((bytes_read = read(log_pipe_fd[0], buffer, sizeof(buffer) - 1)) > 0) {
            buffer[bytes_read] = '\0';
            write(log_fd, buffer, bytes_read);
        }

        log_event("[ARKHAM] Sortint del bucle de lectura...");
        close(log_fd);
        close(log_pipe_fd[0]);
        log_event("[ARKHAM] Fitxers tancats. Acabant procés.");
        exit(0);
    }

    // Parent process (Gotham)
    arkham_pid = pid;
    close(log_pipe_fd[0]); // Tanca el costat de lectura del pipe en Gotham
    log_event("[ARKHAM] Logger inicialitzat correctament.");
}



int verificar_worker(int socket_fd) {
    char buffer[1];
    int result = recv(socket_fd, buffer, sizeof(buffer), MSG_PEEK | MSG_DONTWAIT);
    if (result == 0 || (result < 0 && errno != EAGAIN && errno != EWOULDBLOCK)) {
        return 1; // Worker desconnectat
    }
    return 0; // Worker actiu
}

void *monitor_workers(void *arg) {
    (void)arg;
    int old_state;
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, &old_state);

    while (1) {
        sleep(5); // Check every 5 seconds
        if (errno == EINTR) break;

        pthread_mutex_lock(&worker_mutex);
        time_t now = time(NULL);
        int workers_actius = 0;

        for (int i = 0; i < num_workers; i++) {
            // If worker is disconnected or doesn't send heartbeats for 15s, remove it
            if (verificar_worker(workers[i].base.socket_fd) || difftime(now, workers[i].last_heartbeat) > 15) {
                char msg[128];
                snprintf(msg, sizeof(msg), "Worker %d desconnectat o sense heartbeat. Reassignant tasques.", workers[i].base.socket_fd);
                log_event(msg);
                redistribuir_tasques_worker(workers[i].base.socket_fd);
            } else {
                workers_actius++;
                // Log number of active workers
                char msg[36];
                snprintf(msg, sizeof(msg), "Workers actius: %d", workers_actius);
                log_event(msg);
            }
        }
        pthread_mutex_unlock(&worker_mutex);

        // Check if Arkham is unresponsive
        if (arkham_pid > 0 && (kill(arkham_pid, 0) == -1 || kill(arkham_pid, SIGUSR1) == -1)) {
            log_event("Arkham ha deixat de respondre. Reiniciant...");
            inicialitzar_arkham_logger(); // Restart Arkham and update PID
        }
    }
    return NULL;
}


/**
 * Processa les tasques de la cua en un fil separat.
 */
void *task_processor(void *arg) {
    (void)arg;
    int old_state;
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, &old_state);

    while (1) {
        if (errno == EINTR) break;
        Task task = obtenir_tasca();
        if (task.task_id != 0) { 
            processar_tasca(task);
        } else {
            usleep(100000); 
        }
    }

    return NULL;
}

void sigint_handler(int sig) {
    (void)sig;
    
    pthread_mutex_unlock(&worker_mutex);
    pthread_mutex_unlock(&arkham_mutex);
    pthread_mutex_unlock(&task_manager.mutex);

    close(server_fd);

    pthread_mutex_lock(&arkham_mutex);
    
    close(log_pipe_fd[1]); 

    if (arkham_pid > 0) {
        kill(arkham_pid, SIGTERM); 
        waitpid(arkham_pid, NULL, 0); 
    }

    pthread_mutex_unlock(&arkham_mutex);

    pthread_cancel(task_thread);
    pthread_cancel(monitor_thread);
    pthread_cancel(fleck_thread);
    pthread_cancel(worker_thread);

    pthread_join(task_thread, NULL);
    pthread_join(monitor_thread, NULL);
    pthread_join(fleck_thread, NULL);
    pthread_join(worker_thread, NULL);

    // Guardar l'estat de les tasques en curs
    int task_backup_fd = open("tasks_backup.dat", O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (task_backup_fd >= 0) {
        pthread_mutex_lock(&task_manager.mutex);
        Task *current = task_manager.head;
        while (current) {
            write(task_backup_fd, current, sizeof(Task));
            current = current->next;
        }
        pthread_mutex_unlock(&task_manager.mutex);
        close(task_backup_fd);
    } 

    // Notificar clients amb tasques pendents
    pthread_mutex_lock(&task_manager.mutex);
    Task *current = task_manager.head;
    while (current) {
        Trama notify_trama;
        inicialitzar_trama(&notify_trama, 0x07, "TASK_ABORTED");
        write(current->client_socket, &notify_trama, sizeof(notify_trama));
        close(current->client_socket);
        current = current->next;
    }
    pthread_mutex_unlock(&task_manager.mutex);

    free(config.base.ip);
    free(config.worker_ip);

    for (int i = 0; i < num_workers; i++) {
        free(workers[i].base.ip);
        free(workers[i].ip_fleck);
    }
    free(workers);
    destroyTaskManager(&task_manager);
    

    exit(0);
}

void gestionar_reassignacio_worker(int client_socket, char *data) {
    char *separator = strchr(data, '&');
    if (!separator) {
        Trama resposta;
        inicialitzar_trama(&resposta, 0x11, "MEDIA_KO");
        write(client_socket, &resposta, sizeof(resposta));
        log_event("MEDIA_KO: Format de la petició incorrecte.");
        return;
    }

    //Reservar memòria exacta per a media_type i file_name
    size_t media_type_len = separator - data;
    char *media_type = (char *)malloc(media_type_len + 1);
    if (!media_type) {
        log_event("Error d'allocació de memòria per a media_type.");
        return;
    }
    memcpy(media_type, data, media_type_len);
    media_type[media_type_len] = '\0';

    char *file_name = strdup(separator + 1);
    if (!file_name) {
        free(media_type);
        log_event("Error d'allocació de memòria per a file_name.");
        return;
    }

    //Comprovar si el tipus de media coincideix amb l'extensió del fitxer
    int es_text = (strstr(file_name, ".txt") != NULL);
    int es_media = (strstr(file_name, ".wav") || strstr(file_name, ".mp3") ||
                    strstr(file_name, ".jpg") || strstr(file_name, ".png"));

    if ((es_text && strcmp(media_type, "TEXT") != 0) || (es_media && strcmp(media_type, "MEDIA") != 0)) {
        Trama resposta;
        inicialitzar_trama(&resposta, 0x11, "MEDIA_KO");
        write(client_socket, &resposta, sizeof(resposta));
        log_event("MEDIA_KO: Extensió del fitxer no coincideix amb mediaType.");
        free(media_type);
        free(file_name);
        return;
    }

    pthread_mutex_lock(&worker_mutex);

    //Buscar i eliminar el worker caigut
    int found_worker = -1;
    InstanceType worker_type = es_text ? ENIGMA : HARLEY;
    for (int i = 0; i < num_workers; i++) {
        if (workers[i].base.type == worker_type) {
            found_worker = i;
            eliminar_worker(workers[i].base.socket_fd);
            break;
        }
    }

    //Si no hi ha cap worker del tipus corresponent, enviar DISTORT_KO
    if (found_worker == -1) {
        pthread_mutex_unlock(&worker_mutex);
        Trama resposta;
        inicialitzar_trama(&resposta, 0x11, "DISTORT_KO");
        write(client_socket, &resposta, sizeof(resposta));
        log_event("DISTORT_KO: No hi ha workers disponibles per a reassignació.");
        free(media_type);
        free(file_name);
        return;
    }

    //Assignar nou worker principal si hi ha algun disponible
    int new_primary_index = -1;
    for (int i = 0; i < num_workers; i++) {
        if (workers[i].base.type == worker_type) {
            workers[i].is_primary_worker = 1;
            new_primary_index = i;
            notificar_worker_principal(workers[i].base.socket_fd);
            break;
        }
    }

    //Enviar resposta a Fleck amb la IP i port del nou worker principal
    Trama resposta;
    if (new_primary_index != -1) {
        char resposta_data[128];
        snprintf(resposta_data, sizeof(resposta_data), "%s&%d", workers[new_primary_index].ip_fleck, workers[new_primary_index].port_fleck);
        inicialitzar_trama(&resposta, 0x11, resposta_data);
        log_event("Nou worker principal assignat i notificat a Fleck.");
    } else {
        inicialitzar_trama(&resposta, 0x11, "DISTORT_KO");
        log_event("DISTORT_KO: Cap worker disponible per continuar la distorsió.");
    }

    pthread_mutex_unlock(&worker_mutex);
    write(client_socket, &resposta, sizeof(resposta));

    free(media_type);
    free(file_name);
}

void *listener_fleck(void *arg) {
    (void) arg;
    int old_state;
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, &old_state);
    struct sockaddr_in address;
    socklen_t addrlen = sizeof(address);
    
    int server_fd_fleck = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd_fleck < 0) {
        log_event("Error creando socket Fleck");
        exit(EXIT_FAILURE);
    }
    
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = inet_addr(config.base.ip);
    address.sin_port = htons(config.base.port);
    
    if (bind(server_fd_fleck, (struct sockaddr *)&address, sizeof(address)) < 0) {
        log_event("Error en bind() de Fleck");
        exit(EXIT_FAILURE);
    }
    
    if (listen(server_fd_fleck, 10) < 0) {
        log_event("Error en listen() de Fleck");
        exit(EXIT_FAILURE);
    }

    while (1) {
        int client_socket = accept(server_fd_fleck, (struct sockaddr *)&address, &addrlen);
        if (client_socket >= 0) {
            if (errno == EINTR) break; // Check for SIGINT
            Trama trama;
            if (read(client_socket, &trama, sizeof(trama)) == sizeof(trama)) {
                printf("Trama type: %02X\n", trama.type);
                if (trama.type == 0x01) {
                    // Fleck connection request
                    log_event("Connexió inicial de Fleck detectada");
                    Trama resposta;
                    inicialitzar_trama(&resposta, 0x01, "");
                    write(client_socket, &resposta, sizeof(resposta));
                    log_event("Resposta enviada a Fleck");
                    printf("Fleck connectat\n");
    
                } else if (trama.type == 0x10) {
                    printf("Petició de distorsió de Fleck rebuda\n");
                    if (!verificar_fleck(client_socket)) {
                        char *media_type = strtok(trama.data, "&");
                        char *file_name = strtok(NULL, "&");
    
                        if (!media_type || !file_name) {
                            Trama resposta;
                            inicialitzar_trama(&resposta, 0x10, "MEDIA_KO");
                            write(client_socket, &resposta, sizeof(resposta));
                            log_event("MEDIA_KO: Trama mal formada (sense media_type o file_name).");
                            close(client_socket);
                            continue;
                        }
    
                        int es_text = (strstr(file_name, ".txt") != NULL);
                        int es_media = (strstr(file_name, ".wav") || strstr(file_name, ".mp3") ||
                                        strstr(file_name, ".jpg") || strstr(file_name, ".png"));
    
                        if ((es_text && strcmp(media_type, "TEXT") != 0) ||
                            (es_media && strcmp(media_type, "MEDIA") != 0)) {
                            Trama resposta;
                            inicialitzar_trama(&resposta, 0x10, "MEDIA_KO");
                            write(client_socket, &resposta, sizeof(resposta));
                            log_event("MEDIA_KO: El media_type no coincideix amb l'extensió del fitxer.");
                            close(client_socket);
                            continue;
                        }
    
                        pthread_mutex_lock(&worker_mutex);
                        int index_worker = -1;
                        InstanceType tipus = (es_text) ? ENIGMA : HARLEY;
                        for (int i = 0; i < num_workers; i++) {
                            if (workers[i].base.type == tipus && workers[i].is_primary_worker) {
                                index_worker = i;
                                break;
                            }
                        }
    
                        if (index_worker == -1) {
                            Trama resposta;
                            inicialitzar_trama(&resposta, 0x10, "DISTORT_KO");
                            write(client_socket, &resposta, sizeof(resposta));
                            log_event("DISTORT_KO: No hi ha cap worker disponible del tipus demanat.");
                            close(client_socket);
                            pthread_mutex_unlock(&worker_mutex);
                            continue;
                        }
    
                        char resposta_data[128];
                        snprintf(resposta_data, sizeof(resposta_data), "%s&%d",
                                 workers[index_worker].ip_fleck, workers[index_worker].port_fleck);
    
                        Trama resposta;
                        inicialitzar_trama(&resposta, 0x10, resposta_data);
                        write(client_socket, &resposta, sizeof(resposta));
    
                        Task new_task = {
                            .client_socket = client_socket,
                            .worker_socket = workers[index_worker].base.socket_fd
                        };
                        strncpy(new_task.file_name, file_name, sizeof(new_task.file_name));
                        afegir_tasca(new_task);
                        log_event("Tasca de Fleck afegida correctament.");
                        pthread_mutex_unlock(&worker_mutex);
    
                    } else {
                        const char *disconnect_msg = "Fleck desconnectat abans d'afegir la tasca. Eliminant connexio.\n";
                        write(STDOUT_FILENO, disconnect_msg, strlen(disconnect_msg));
                        eliminar_tasques_fleck(client_socket);
                        close(client_socket);
                    }
                } else if (trama.type == 0x11) {
                    gestionar_reassignacio_worker(client_socket, trama.data);
                } else if (trama.type == 0x07) {
                    eliminar_tasques_fleck(client_socket);
                    close(client_socket);
                } else {
                    Trama response;
                    inicialitzar_trama(&response, 0x09, "");
                    write(client_socket, &response, sizeof(response));
                    close(client_socket);
                }
    
            } else {
                const char *error_msg = "Error en la lectura de la trama. Eliminant client.\n";
                write(STDOUT_FILENO, error_msg, strlen(error_msg));
                eliminar_tasques_fleck(client_socket);
                close(client_socket);
            }
        }
    }    
    return NULL;
}

void *listener_worker(void *arg) {
    (void) arg;
    int old_state;
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, &old_state);

    struct sockaddr_in address;
    socklen_t addrlen = sizeof(address);
    
    int server_fd_worker = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd_worker < 0) {
        log_event("Error creando socket Workers");
        exit(EXIT_FAILURE);
    }
    
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = inet_addr(config.worker_ip);
    address.sin_port = htons(config.worker_port);
    
    if (bind(server_fd_worker, (struct sockaddr *)&address, sizeof(address)) < 0) {
        log_event("Error en bind() de Workers");
        exit(EXIT_FAILURE);
    }
    
    if (listen(server_fd_worker, 10) < 0) {
        log_event("Error en listen() de Workers");
        exit(EXIT_FAILURE);
    }
    
    while (1) {
        int worker_socket = accept(server_fd_worker, (struct sockaddr *)&address, &addrlen);
        if (worker_socket >= 0) {
            if (errno == EINTR) break; // Check for SIGINT
            Trama trama;
            if (read(worker_socket, &trama, sizeof(trama)) == sizeof(trama)) {
                if (trama.type == 0x02) { // Worker registration
                    char *workerType, *workerIP, *workerPortStr;
                    int workerPort;

                    workerType = strtok(trama.data, "&");
                    workerIP = strtok(NULL, "&");
                    workerPortStr = strtok(NULL, "&");

                    if (!workerType || !workerIP || !workerPortStr) {
                        log_event("Error: Trama de worker mal formada. Falten params.");
                        close(worker_socket);
                        continue;
                    }

                    workerPort = atoi(workerPortStr);
                    if (workerPort <= 0 || workerPort > 65535) {
                        log_event("Error: Port de worker invalid.");
                        close(worker_socket);
                        continue;
                    }

                    ConfigWorker worker;
                    worker.base.socket_fd = worker_socket;
                    worker.base.ip = strdup(config.base.ip);  // IP de Gotham
                    worker.base.port = config.base.port;      // Puerto de Gotham
                    worker.ip_fleck = strdup(workerIP);       // IP donde el worker escuchará conexiones de Fleck
                    worker.port_fleck = workerPort;           // Puerto donde el worker escuchará conexiones de Fleck
                    worker.base.type = (strcmp(workerType, "TEXT") == 0) ? ENIGMA : HARLEY;

                    afegir_worker(worker);

                    Trama response;
                    inicialitzar_trama(&response, 0x02, "");
                    write(worker_socket, &response, sizeof(response));
                    log_event("Worker registrat correctament.");
                    printf("Worker registrat\n");
                } else if (trama.type == 0x12) { // Heartbeat
                    pthread_mutex_lock(&worker_mutex);
                    for (int i = 0; i < num_workers; i++) {
                        if (workers[i].base.socket_fd == worker_socket) {
                            workers[i].last_heartbeat = time(NULL); // Guardar timestamp del último heartbeat
                            log_event("Heartbeat rebut de worker.");
                            break;
                        }
                    }
                    pthread_mutex_unlock(&worker_mutex);
                    continue;
                } else if (trama.type == 0x07) { // Worker disconnection
                    eliminar_worker(worker_socket);
                    close(worker_socket);
                } else {
                    Trama response;
                    inicialitzar_trama(&response, 0x09, ""); // Invalid trama response
                    write(worker_socket, &response, sizeof(response));
                    close(worker_socket);
                }
            }
        }
    }
    return NULL;
}

/**
 * Mostra la configuració carregada per pantalla.
 * @param config Configuració de Gotham.
 */
void mostrar_config() {
    char buffer[200];
    int len = snprintf(buffer, sizeof(buffer),
        "Configuració de Gotham carregada:\n"
        "  IP Base: %s\n"
        "  Port Base: %d\n"
        "  IP Workers: %s\n"
        "  Port Workers: %d\n",
        config.base.ip, config.base.port, config.worker_ip, config.worker_port);

    write(STDOUT_FILENO, buffer, len);
}


/**
 * Funció principal.
 */
int main(int argc, char *argv[]) {
    if (argc != 2) {
        write(STDOUT_FILENO, "Us: ./gotham <fitxer_configuracio>\n", 36);
        return 1;
    }

    int config_fd = open(argv[1], O_RDONLY);
    if (config_fd < 0 || parse_gotham_config(config_fd, &config) < 0) {
        perror("Error carregant la configuració de Gotham");
        return 1;
    }
    close(config_fd);

    mostrar_config();

    // Restaurar tasques des del fitxer (usant `open()` i `close()`)
    int task_backup_fd = open("tasks_backup.dat", O_RDONLY);
    if (task_backup_fd >= 0) {
        Task temp;
        ssize_t bytes_read;
        while ((bytes_read = read(task_backup_fd, &temp, sizeof(Task))) == sizeof(Task)) {
            afegir_tasca(temp);
        }
        close(task_backup_fd);
        log_event("Tasques restaurades després d'un reinici de Gotham.");
    }

    inicialitzar_workers();
    initTaskManager(&task_manager);
    inicialitzar_arkham_logger(); 
    
    if (pthread_create(&task_thread, NULL, task_processor, NULL) != 0) {
        perror("Error creant el fil del processador de tasques");
        exit(EXIT_FAILURE);
    }
    
    if (pthread_create(&monitor_thread, NULL, monitor_workers, NULL) != 0) {
        log_event("Error: No s'ha pogut crear el fil de monitoratge de workers.");
        exit(EXIT_FAILURE);
    }
    
    if (pthread_create(&fleck_thread, NULL, listener_fleck, NULL) != 0) {
        log_event("Error creant el fil del listener de Fleck.");
        exit(EXIT_FAILURE);
    }
    
    if (pthread_create(&worker_thread, NULL, listener_worker, NULL) != 0) {
        log_event("Error creant el fil del listener de Workers.");
        exit(EXIT_FAILURE);
    }

    signal(SIGINT, sigint_handler);

    write(STDOUT_FILENO, "Gotham en execució. Prem Ctrl+C per sortir.\n", 45);
    while (1) {
        pause(); 
    }

    // Aquest codi no s’executarà mai normalment, però el deixem per seguretat
    pthread_join(task_thread, NULL);
    pthread_join(monitor_thread, NULL);
    pthread_join(fleck_thread, NULL);
    pthread_join(worker_thread, NULL);

    

    return 0;
}
