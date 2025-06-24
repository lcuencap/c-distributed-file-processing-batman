#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <fcntl.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <errno.h>
#include "file_utils.h"
#include "sys_params.h"


/**
 * Escriu un missatge al descriptor de fitxer especificat.
 * @param fd Descriptor de fitxer on escriure el missatge.
 * @param message Missatge a escriure.
 */
void write_message(int fd, const char *message) {
    write(fd, message, strlen(message));
}

/**
 * Calcula l'MD5SUM d'un fitxer.
 * @param file_name Nom del fitxer.
 * @param md5sum Buffer per guardar l'MD5SUM.
 * @param file_size Retorna la mida del fitxer.
 * @return 0 si és correcte, -1 en cas d'error.
 */
int calcular_md5(const char *file_name, char *md5sum, uint32_t *file_size) {
    if (!file_name || !md5sum || !file_size) {
        write_message(STDOUT_FILENO, "[ERROR] Invalid parameters to calculate MD5\n");
        return -1;
    }

    int fd = open(file_name, O_RDONLY);
    if (fd < 0) {
        write_message(STDOUT_FILENO, "[ERROR] Could not open file to validate MD5.\n")
        return -1;
    }

    off_t size = lseek(fd, 0, SEEK_END);
    if (size == (off_t)-1) {
        write_message(STDOUT_FILENO, "[ERROR] Could not determine file size with lseek.\n");
        close(fd);
        return -1;
    }
    *file_size = (uint32_t)size;
    close(fd);

    int pipe_fd[2];
    if (pipe(pipe_fd) < 0) {
        write_message(STDOUT_FILENO, "[ERROR] Could not create pipe for MD5 calculation.\n");
        return -1;
    }

    pid_t pid = fork();
    if (pid < 0) {
        write_message(STDOUT_FILENO, "[ERROR] Could not fork process for MD5 calculation.\n");
        close(pipe_fd[0]);
        close(pipe_fd[1]);
        return -1;
    }

    if (pid == 0) {
        close(pipe_fd[0]);
        dup2(pipe_fd[1], STDOUT_FILENO);
        close(pipe_fd[1]);
        write_message(STDOUT_FILENO, "[ERROR] Could not exec md5sum for MD5 calculation.\n");

        execlp("md5sum", "md5sum", file_name, NULL);
        _exit(1);
    }

    close(pipe_fd[1]);
    char buffer[256];
    ssize_t bytes_read = read(pipe_fd[0], buffer, sizeof(buffer) - 1);
    if (bytes_read <= 0) {
        write_message(STDOUT_FILENO, "[ERROR] Could not read MD5 output from pipe.\n");
        close(pipe_fd[0]);
        return -1;
    }
    buffer[bytes_read] = '\0';
    close(pipe_fd[0]);

    strncpy(md5sum, buffer, 32);
    md5sum[32] = '\0';

    return 0;
}

ssize_t safe_read(int socket, void *buffer, size_t length) {
    size_t total_read = 0;
    ssize_t bytes_read;
    
    while (total_read < length) {
        bytes_read = read(socket, (char *)buffer + total_read, length - total_read);
        
        if (bytes_read < 0) {
            if (errno == EINTR) {
                continue;  // Senyal, tornar a intentar
            } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
                write_message(STDERR_FILENO, "[ERROR] Timeout reading from socket (EAGAIN)\n");
                return -1;
            } else {
                return -1;
            }
        } 
        
        if (bytes_read == 0) {
            // EOF, el client ha tancat la connexió
            write_message(STDERR_FILENO, "[INFO] Client has closed connexion (EOF)\n");
            return -1;
        }
        
        total_read += bytes_read;
    }

    return total_read;
}


/**
 * Verifica si el MD5SUM d'un fitxer coincideix amb el valor esperat.
 * @param file_name Nom del fitxer.
 * @param expected_md5 MD5 esperat.
 * @return 0 si és correcte, -1 en cas d'error.
 */
int verificar_md5sum(const char *file_name, const char *expected_md5) {
    char calculated_md5[33];
    uint32_t file_size;

    if (calcular_md5(file_name, calculated_md5, &file_size) < 0) {
        return -1;
    }

    return (strcmp(calculated_md5, expected_md5) == 0) ? 0 : -1;
}


/**
 * Llegeix una línia del fitxer.
 * @param fd Descriptor del fitxer.
 * @param buffer Buffer dinàmic per emmagatzemar la línia.
 * @return Longitud de la línia o -1 en cas d'error.
 */
ssize_t leer_linea(int fd, char **buffer) {
    size_t pos = 0;
    char c;
    char *temp_buffer = NULL;
    ssize_t leido;

    *buffer = NULL;

    while ((leido = read(fd, &c, 1)) > 0) {
        if (c == '\r') continue;
        if (c == '\n') break;

        temp_buffer = realloc(*buffer, (pos + 2) * sizeof(char)); // +2 para el '\0'
        if (!temp_buffer) {
            free(*buffer);
            *buffer = NULL;
            return -1;
        }
        *buffer = temp_buffer;
        (*buffer)[pos++] = c;
    }

    if (leido == -1) {
        free(*buffer);
        return -1;
    }

    if (*buffer) {
        (*buffer)[pos] = '\0'; 
    }
    return (leido == 0 && pos == 0) ? 0 : pos;
}



/**
 * Elimina espais en blanc inicials i finals.
 * @param str Cadena a processar.
 */
void trim_whitespace(char *str) {
    if (!str) return;
    char *start = str;
    while (isspace((unsigned char)*start)) start++;
    char *end = start + strlen(start) - 1;
    while (end > start && isspace((unsigned char)*end)) end--;
    *(end + 1) = '\0';
    memmove(str, start, end - start + 2);
}

/**
 * Valida si una adreça IP és vàlida.
 * @param ip Cadena que conté l'adreça IP.
 * @return 1 si és vàlida, 0 altrament.
 */
int validate_ip(const char *ip) {
    struct sockaddr_in sa;
    return inet_pton(AF_INET, ip, &(sa.sin_addr)) != 0;
}

int validate_port(int port) {
    return port > 0 && port <= 65535;
}


/**
 * Analitza la configuració de Gotham.
 * @param fd Descriptor del fitxer de configuració.
 * @param config Punter a la configuració de Gotham.
 * @return 0 si és correcte, -1 en cas d'error.
 */
int parse_gotham_config(int fd, ConfigGotham *config) {
    char *line = NULL;
    ssize_t len;

    // Leer y validar IP base
    if ((len = leer_linea(fd, &line)) <= 0) {
        free(line);
        return -1;
    }
    trim_whitespace(line);
    if (!validate_ip(line)) {
        size_t error_msg_len = snprintf(NULL, 0, "IP base invàlida: %s\n", line) + 1;
        char *error_msg = malloc(error_msg_len);
        if (error_msg) {
            snprintf(error_msg, error_msg_len, "IP base invàlida: %s\n", line);
            write(STDERR_FILENO, error_msg, strlen(error_msg));
            free(error_msg);
        }
        free(line);
        return -1;
    }
    config->base.ip = strdup(line);
    free(line);

    // Leer y validar puerto base
    if ((len = leer_linea(fd, &line)) <= 0) {
        free(line);
        return -1;
    }

    trim_whitespace(line);
    config->base.port = atoi(line);
    if (!validate_port(config->base.port)) {
        size_t error_msg_len = snprintf(NULL, 0, "Port base invàlid: %d\n", config->base.port) + 1;
        char *error_msg = malloc(error_msg_len);
        if (error_msg) {
            snprintf(error_msg, error_msg_len, "Port base invàlid: %d\n", config->base.port);
            write(STDERR_FILENO, error_msg, strlen(error_msg));
            free(error_msg);
        }
        free(line);
        return -1;
    }
    free(line);

    // Leer y validar IP de workers
    if ((len = leer_linea(fd, &line)) <= 0) {
        free(line);
        return -1;
    }
    trim_whitespace(line);
    if (!validate_ip(line)) {
        size_t error_msg_len = snprintf(NULL, 0, "IP de worker invàlida: %s\n", line) + 1;
        char *error_msg = malloc(error_msg_len);
        if (error_msg) {
            snprintf(error_msg, error_msg_len, "IP de worker invàlida: %s\n", line);
            write(STDERR_FILENO, error_msg, strlen(error_msg));
            free(error_msg);
        }
        free(line);
        return -1;
    }
    config->worker_ip = strdup(line);
    free(line);

    // Leer y validar puerto de workers
    if ((len = leer_linea(fd, &line)) <= 0) {
        free(line);
        return -1;
    }
    trim_whitespace(line);
    config->worker_port = atoi(line);
    if (!validate_port(config->worker_port)) {
        size_t error_msg_len = snprintf(NULL, 0, "Port de worker invàlid: %d\n", config->worker_port) + 1;
        char *error_msg = malloc(error_msg_len);
        if (error_msg) {
            snprintf(error_msg, error_msg_len, "Port de worker invàlid: %d\n", config->worker_port);
            write(STDERR_FILENO, error_msg, strlen(error_msg));
            free(error_msg);
        }
        free(line);
        return -1;
    }
    free(line);

    // Verificar que no haya más líneas inesperadas
    if (leer_linea(fd, &line) > 0) {
        char *error_msg = strdup("Línies addicionals al fitxer de configuració.\n");
        if (error_msg) {
            write(STDERR_FILENO, error_msg, strlen(error_msg));
            free(error_msg);
        }
        free(line);
        return -1;
    }    

    return 0;
}


/**
 * Analitza la configuració de Fleck des d'un fitxer.
 * @param fd Descriptor del fitxer de configuració.
 * @param config Punter a la configuració de Fleck.
 * @return 0 si és correcte, -1 en cas d'error.
 */
int parse_fleck_config(int fd, ConfigFleck *config) {
    char *line = NULL;
    ssize_t len;

    // Llegeix el nom d'usuari
    if ((len = leer_linea(fd, &line)) <= 0) {
        write_message(STDOUT_FILENO, "Error llegint el nom d'usuari\n");
        return -1;
    }
    trim_whitespace(line);
    config->username = strdup(line);
    free(line);

    // Llegeix el directori de l'usuari i li afegeix "Fleck/"
    if ((len = leer_linea(fd, &line)) <= 0) {
        write_message(STDOUT_FILENO, "Error llegint el directori de l'usuari\n");
        free(config->username);
        return -1;
    }
    trim_whitespace(line);

    size_t path_len = strlen("Fleck") + strlen(line) + 1;
    config->work_dir = malloc(path_len);
    if (!config->work_dir) {
        write_message(STDOUT_FILENO, "Error assignant memòria per al directori de treball\n");
        free(config->username);
        free(line);
        return -1;
    }
    snprintf(config->work_dir, path_len, "Fleck%s", line);
    free(line);

    // Llegeix la IP de Gotham
    if ((len = leer_linea(fd, &line)) <= 0) {
        write_message(STDOUT_FILENO, "Error llegint la IP de Gotham\n");
        free(config->username);
        free(config->work_dir);
        return -1;
    }
    trim_whitespace(line);
    if (!validate_ip(line)) {
        char buffer[40];
        snprintf(buffer, sizeof(buffer), "IP de Gotham invàlida: %s\n", line);
        write_message(STDOUT_FILENO, buffer);
        free(line);
        free(config->username);
        free(config->work_dir);
        return -1;
    }
    config->base.ip = strdup(line);
    free(line);

    // Llegeix el port de Gotham
    if ((len = leer_linea(fd, &line)) <= 0) {
        write_message(STDOUT_FILENO, "Error llegint el port de Gotham\n");
        free(config->username);
        free(config->work_dir);
        free(config->base.ip);
        return -1;
    }
    trim_whitespace(line);
    config->base.port = atoi(line);
    free(line);

    // Assignar el tipus específic de configuració per a Fleck
    config->base.type = FLECK;
    config->base.socket_fd = -1; // Inicialitza el socket com a no connectat
    config->fleck_id = -1; // Inicialitza l'ID del fleck com a no assignat
    return 0;
}

/**
 * Analitza la configuració d'un worker.
 * @param fd Descriptor del fitxer de configuració.
 * @param config Punter a la configuració del worker.
 * @return 0 si és correcte, -1 en cas d'error.
 */
int parse_worker_config(int fd, ConfigWorker *config) {
    char *line = NULL;
    ssize_t len;
    char buffer[256];

    // Llegir la IP de Gotham
    if ((len = leer_linea(fd, &line)) <= 0) {
        free(line);
        return -1;
    }
    trim_whitespace(line);
    if (!validate_ip(line)) {
        int msg_len = snprintf(buffer, sizeof(buffer), "Error: IP de Gotham invàlida -> %s\n", line);
        write(STDERR_FILENO, buffer, msg_len);
        free(line);
        return -1;
    }
    config->base.ip = strdup(line);
    free(line);

    // Llegir el port de Gotham
    if ((len = leer_linea(fd, &line)) <= 0) {
        free(line);
        return -1;
    }
    trim_whitespace(line);
    config->base.port = atoi(line);
    free(line);

    // Llegir la IP on el worker escoltarà Fleck
    if ((len = leer_linea(fd, &line)) <= 0) {
        free(line);
        return -1;
    }
    trim_whitespace(line);
    if (!validate_ip(line)) {
        int msg_len = snprintf(buffer, sizeof(buffer), "Error: IP per Fleck invàlida -> %s\n", line);
        write(STDERR_FILENO, buffer, msg_len);
        free(line);
        return -1;
    }
    config->ip_fleck = strdup(line);
    free(line);

    // Llegir el port on el worker escoltarà Fleck
    if ((len = leer_linea(fd, &line)) <= 0) {
        free(line);
        return -1;
    }
    trim_whitespace(line);
    config->port_fleck = atoi(line);
    if (!validate_port(config->port_fleck)) {
        int msg_len = snprintf(buffer, sizeof(buffer), "Error: Port per Fleck invàlid -> %d\n", config->port_fleck);
        write(STDERR_FILENO, buffer, msg_len);
        return -1;
    }
    free(line);

    // Llegir el directori de treball del worker
    if ((len = leer_linea(fd, &line)) <= 0) {
        free(line);
        return -1;
    }
    trim_whitespace(line);
    config->work_dir = strdup(line);
    free(line);

    // Llegir el tipus de worker (Media o Text)
    if ((len = leer_linea(fd, &line)) <= 0) {
        free(line);
        return -1;
    }
    trim_whitespace(line);
    if (strcmp(line, "Media") == 0) {
        config->base.type = HARLEY;
    } else if (strcmp(line, "Text") == 0) {
        config->base.type = ENIGMA;
    } else {
        int msg_len = snprintf(buffer, sizeof(buffer), "Error: Tipus de worker invàlid -> %s\n", line);
        write(STDERR_FILENO, buffer, msg_len);
        free(line);
        return -1;
    }
    free(line);

    config->is_primary_worker = 0;
    config->worker_id = -1;

    return 0;
}
