#ifndef _TRAMES_H_
#define _TRAMES_H_

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <stdint.h>
#include <arpa/inet.h>
#include "file_utils.h"


#define TRAMA_SIZE sizeof(Trama)


#pragma pack(push, 1)
typedef struct {
    char type;             // 1 Byte: Tipus de trama
    uint16_t data_length;  // 2 Bytes: Longitud del camp de dades
    char data[245];        // Dades fins a 245 bytes màxim
    uint16_t checksum;     // 2 Bytes: Checksum
    uint32_t timestamp;    // 4 Bytes: Temps d'enviament
} Trama;
#pragma pack(pop)



void enviar_resposta_connexio_inicial(int socket_fd, int correcte);


void enviar_resposta_md5(int socket_fd, int correcte);

/**
 * Inicialitza una estructura de trama amb els paràmetres donats.
 * @param trama Punter a l'estructura de trama.
 * @param type Tipus de la trama.
 * @param data Dades per incloure a la trama.
 */
void inicialitzar_trama(Trama *trama, char type, const char *data);

/**
 * Valida el checksum d'una trama donada.
 * @param trama Punter a l'estructura de trama.
 * @return 1 si és vàlid, 0 en cas contrari.
 */
int validar_trama(Trama *trama);

/**
 * Genera un checksum per a la trama.
 * @param trama Punter a la trama.
 * @param length Longitud de la trama excloent els bytes del checksum.
 * @return Checksum calculat.
 */
uint16_t generate_checksum(const uint8_t *trama, size_t length);

/**
 * Envia una trama de sol·licitud de connexió al servidor Gotham.
 * @param socket_fd Descriptor de fitxer del socket.
 * @param userName Nom d'usuari.
 * @param ip Adreça IP.
 * @param port Número de port.
 * @return 0 si és correcte, -1 en cas d'error.
 */
int send_connection_request_to_gotham(int socket_fd, const char *userName, const char *ip, uint16_t port);

/**
 * Envia una trama de sol·licitud de fitxer per distorsionar a Gotham.
 * @param socket_fd Descriptor de fitxer del socket.
 * @param mediaType Tipus de mitjà.
 * @param fileName Nom del fitxer.
 * @return 0 si és correcte, -1 en cas d'error.
 */
int send_distort_file_request_to_gotham(int socket_fd, const char *mediaType, const char *fileName);

/**
 * Envia una notificació de desconnexió a Gotham o a un Worker.
 * @param socket_fd Descriptor de fitxer del socket.
 * @param identifier Identificador (nom d'usuari o tipus de mitjà).
 * @return 0 si és correcte, -1 en cas d'error.
 */
int send_disconnection_notification(int socket_fd, const char *identifier);

/**
 * Envia una notificació d'error.
 * @param socket_fd Descriptor de fitxer del socket.
 * @return 0 si és correcte, -1 en cas d'error.
 */
int send_error_notification(int socket_fd);

#endif
