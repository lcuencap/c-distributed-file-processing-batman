#include "trames.h"


// Funció auxiliar per enviar resposta CON_OK o CON_KO
void enviar_resposta_connexio_inicial(int socket_fd, int correcte) {
    Trama resposta;
    if (correcte) {
        inicialitzar_trama(&resposta, 0x03, "");
    } else {
        inicialitzar_trama(&resposta, 0x03, "CON_KO");
    }
    write(socket_fd, &resposta, sizeof(resposta));
}

// Funció auxiliar per enviar CHECK_OK o CHECK_KO
void enviar_resposta_md5(int socket_fd, int correcte) {
    Trama resposta;
    if (correcte) {
        inicialitzar_trama(&resposta, 0x06, "CHECK_OK");
    } else {
        inicialitzar_trama(&resposta, 0x06, "CHECK_KO");
    }
    write(socket_fd, &resposta, sizeof(resposta));
}

uint16_t generate_checksum(const uint8_t *trama, size_t length) {
    uint32_t sum = 0;
    //char debug_msg[512] = {0};
    //snprintf(debug_msg, sizeof(debug_msg), "Calculant checksum per trama: ");
    for (size_t i = 0; i < length; ++i) {
        /*
        char byte[8];
        snprintf(byte, sizeof(byte), "%02X ", trama[i]);
        strncat(debug_msg, byte, sizeof(debug_msg) - strlen(debug_msg) - 1);
        */
       sum += trama[i];
    }
    //write(STDOUT_FILENO, debug_msg, strlen(debug_msg));
    //write(STDOUT_FILENO, "\n", 1);

    return (uint16_t)(sum % 65536);
}

void inicialitzar_trama(Trama *trama, char type, const char *data) {
    memset(trama, 0, sizeof(Trama));
    trama->type = type;
    trama->data_length = strlen(data);
    if (trama->data_length >= sizeof(trama->data)) {
        trama->data_length = sizeof(trama->data) - 1;
    }
    strncpy(trama->data, data, trama->data_length);
    trama->timestamp = (uint32_t)time(NULL);    
    trama->checksum = generate_checksum((uint8_t *)trama, TRAMA_SIZE - 2);
}


int validar_trama(Trama *trama) {
    char debug_msg[256];

    // Verificar la longitud de les dades
    if (trama->data_length > sizeof(trama->data)) {
        return 0; // Longitud no vàlida
    }

    // Guardar el checksum original i posar-lo a 0 per al càlcul
    uint16_t original_checksum = trama->checksum;
    trama->checksum = 0;

    // Calcular el checksum basat en els altres camps
    uint16_t calculated_checksum = generate_checksum((uint8_t *)trama, TRAMA_SIZE - 2);
    trama->checksum = original_checksum; // Restaurar el valor original

    // Comparar el checksum calculat amb el rebut
    if (calculated_checksum != original_checksum) {
        snprintf(debug_msg, sizeof(debug_msg), "Checksum no coincideix: calculat=%d, rebut=%d\n",
                 calculated_checksum, original_checksum);
        write(STDOUT_FILENO, debug_msg, strlen(debug_msg));
        return 0; // Checksum no vàlid
    }

    return 1; // Trama vàlida
}


/**
 * Envia una petició de connexió a Gotham.
 */
int send_connection_request_to_gotham(int socket_fd, const char *userName, const char *ip, uint16_t port) {
    Trama trama;
    char data[245];
    snprintf(data, sizeof(data), "%s&%s&%u", userName, ip, port);
    inicialitzar_trama(&trama, 0x01, data);

    if (write(socket_fd, &trama, TRAMA_SIZE) != TRAMA_SIZE) {
        return -1;
    }
    return 0;
}


int send_distort_file_request_to_gotham(int socket_fd, const char *mediaType, const char *fileName) {
    Trama trama;
    char data[245] = {0};
    snprintf(data, sizeof(data), "%s&%s", mediaType, fileName);

    inicialitzar_trama(&trama, 0x10, data);

    if (write(socket_fd, &trama, TRAMA_SIZE) != TRAMA_SIZE) {
        return -1;
    }

    return 0;
}

int send_disconnection_notification(int socket_fd, const char *identifier) {
    Trama trama;
    inicialitzar_trama(&trama, 0x07, identifier);

    if (write(socket_fd, &trama, TRAMA_SIZE) != TRAMA_SIZE) {
        return -1;
    }

    return 0;
}

int send_error_notification(int socket_fd) {
    Trama trama;
    inicialitzar_trama(&trama, 0x09, "");

    if (write(socket_fd, &trama, TRAMA_SIZE) != TRAMA_SIZE) {
        return -1;
    }

    return 0;
}
