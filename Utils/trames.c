#include "trames.h"

// ---------- Funcions bàsiques i auxiliars ----------

/* * Genera un checksum per a una trama.
 * @param trama Punter a la trama per calcular el checksum.
 * @param length Longitud de la trama.
 * @return El checksum calculat com un uint16_t.
 */
uint16_t generate_checksum(const uint8_t *trama, size_t length) {
    uint32_t sum = 0;
    for (size_t i = 0; i < length; ++i) {
        sum += trama[i];
    }
    return (uint16_t)(sum % 65536);
}

/* Inicialitza una trama amb el tipus i les dades especificades.
 * @param trama Punter a la trama a inicialitzar.
 * @param type Tipus de trama.
 * @param data Dades a incloure a la trama.
 */
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

/* Validació de la trama.
 * @param trama Punter a la trama a validar.
 * @return 1 si la trama és vàlida, 0 altrament.
 */
int validar_trama(Trama *trama) {
    if (trama->data_length > sizeof(trama->data)) return 0;

    uint16_t original = trama->checksum;
    trama->checksum = 0;
    uint16_t calculated = generate_checksum((uint8_t *)trama, TRAMA_SIZE - 2);
    trama->checksum = original;

    if (calculated != original) {
        char *msg = malloc(128);
        if (msg) {
            snprintf(msg, 128, "Checksum mismatch: calculated=%u, received=%u\n", calculated, original);
            write(STDOUT_FILENO, msg, strlen(msg));
            free(msg);
        }
        return 0;
    }

    return 1;
}

/* * Envia una trama al socket especificat.
 * @param socket_fd Descriptor del socket on enviar la trama.
 * @param tipus Tipus de trama a enviar.
 * @param data Dades a incloure a la trama.
 * @return 0 si l'enviament és correcte, -1 si hi ha un error.
 */
int enviar_trama(int socket_fd, char tipus, const char *data) {
    Trama t;
    inicialitzar_trama(&t, tipus, data);
    return write(socket_fd, &t, TRAMA_SIZE) == TRAMA_SIZE ? 0 : -1;
}

// ---------- Funcions d’enviament de resposta ----------

/* Envia una resposta simple al socket especificat.
 * @param socket_fd Descriptor del socket on enviar la resposta.
 * @param tipus Tipus de trama a enviar.
 * @param correcte 1 si la resposta és correcta, 0 altrament.
 * @param ok_msg Missatge a enviar si la resposta és correcta.
 * @param ko_msg Missatge a enviar si la resposta no és correcta.
 */
void enviar_resposta_simple(int socket_fd, char tipus, int correcte, const char *ok_msg, const char *ko_msg) {
    const char *missatge = correcte ? ok_msg : ko_msg;
    enviar_trama(socket_fd, tipus, missatge);
}

/* Envia una resposta de connexió inicial al socket especificat.
 * @param socket_fd Descriptor del socket on enviar la resposta.
 * @param correcte 1 si la connexió és correcta, 0 altrament.
 */
void enviar_resposta_connexio_inicial(int socket_fd, int correcte) {
    enviar_resposta_simple(socket_fd, 0x03, correcte, "", "CON_KO");
}

/* Envia una resposta de distorsió de fitxer al socket especificat.
 * @param socket_fd Descriptor del socket on enviar la resposta.
 * @param correcte 1 si la distorsió és correcta, 0 altrament.
 */
void enviar_resposta_md5(int socket_fd, int correcte) {
    enviar_resposta_simple(socket_fd, 0x06, correcte, "CHECK_OK", "CHECK_KO");
}

// ---------- Verificació ----------

/* Calcula el MD5SUM d'un fitxer i retorna si és l'esperat.
 * @param file_name Nom del fitxer a calcular el MD5SUM.
 * @param md5sum Punter al buffer on emmagatzemar el checksum calculat.
 * @param file_size Punter a una variable on emmagatzemar la mida del fitxer.
 * @return 0 si el càlcul és correcte, -1 si hi ha un error.
 */
int verificar_checksum(const char *file_name, const char *received_checksum) {
    char calculated_checksum[MD5SUM_LENGTH];
    uint32_t file_size;

    if (calcular_md5(file_name, calculated_checksum, &file_size) < 0) {
        write_message(STDERR_FILENO, "Error calculant MD5SUM.\n");
        return -1;
    }

    return strncmp(calculated_checksum, received_checksum, MD5SUM_LENGTH) == 0 ? 0 : -1;
}

// ---------- Funcions d’enviament de peticions ----------

/* Envia una petició de connexió a Gotham al socket especificat.
 * @param socket_fd Descriptor del socket on enviar la petició.
 * @param userName Nom d'usuari per a la connexió.
 * @param ip Adreça IP del client.
 * @param port Port del client.
 * @return 0 si l'enviament és correcte, -1 si hi ha un error.
 */
int send_connection_request_to_gotham(int socket_fd, const char *userName, const char *ip, uint16_t port) {
    char data[245];
    snprintf(data, sizeof(data), "%s&%s&%u", userName, ip, port);
    return enviar_trama(socket_fd, 0x01, data);
}

/* Envia una petició de distorsió de fitxer al socket especificat.
 * @param socket_fd Descriptor del socket on enviar la petició.
 * @param mediaType Tipus de media ("media", "text").
 * @param fileName Nom del fitxer a distorsionar.
 * @return 0 si l'enviament és correcte, -1 si hi ha un error.
 */
int send_distort_file_request_to_gotham(int socket_fd, const char *mediaType, const char *fileName) {
    char data[245];
    snprintf(data, sizeof(data), "%s&%s", mediaType, fileName);
    return enviar_trama(socket_fd, 0x10, data);
}

/*Envia notificació de desconnexió al socket especificat.
 * @param socket_fd Descriptor del socket on enviar la notificació.
 * @param identifier Identificador del worker o client que es desconnecta.
 * @return 0 si l'enviament és correcte, -1 si hi ha un error.
 */
int send_disconnection_notification(int socket_fd, const char *identifier) {
    return enviar_trama(socket_fd, 0x07, identifier);
}

/* * Envia una notificació d'error al socket especificat.
 * @param socket_fd Descriptor del socket on enviar la notificació.
 * @return 0 si l'enviament és correcte, -1 si hi ha un error.
 */
int send_error_notification(int socket_fd) {
    return enviar_trama(socket_fd, 0x09, "");
}
