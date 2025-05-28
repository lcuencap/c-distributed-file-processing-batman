#ifndef FILE_UTILS_H
#define FILE_UTILS_H

#include <signal.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <arpa/inet.h>
#include <errno.h>
#include "sys_params.h"

//Calcula l'MD5SUM d'un fitxer i la seva mida en bytes.
int calcular_md5(const char *file_name, char *md5sum, uint32_t *file_size);

int verificar_md5sum(const char *file_name, const char *expected_md5);

ssize_t safe_read(int socket, void *buffer, size_t length);

// Llegeix una línia del fitxer i la retorna al buffer
ssize_t leer_linea(int fd, char **buffer);

// Elimina espais en blanc inicials i finals d'una cadena
void trim_whitespace(char *str);

// Valida si una cadena és una adreça IP vàlida
int validate_ip(const char *ip);

// Analitza la configuració de Gotham i omple la seva estructura
int parse_gotham_config(int fd, ConfigGotham *config);

// Analitza la configuració d'un worker (Harley o Enigma) i omple la seva estructura
int parse_worker_config(int fd, ConfigWorker *config);

// Analitza la configuració de Fleck i omple la seva estructura
int parse_fleck_config(int fd, ConfigFleck *config);
#endif // FILE_UTILS_H
