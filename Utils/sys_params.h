#ifndef _SYS_PARAMS_H_
#define _SYS_PARAMS_H_

#include <stdint.h>


#define MD5SUM_LENGTH 33
#define SO_NO_ERROR 0
#define SO_ERROR_UNSUPPORTED_FORMAT 1


// Enumeració per als estats de les tasques
typedef enum {
    TASK_EMPTY,            // Entrada buida disponible per reutilitzar
    TASK_PENDING,           // Tasca pendent d'inici
    TASK_IN_PROGRESS,       // En progrés (per Gotham)
    TASK_RECEIVING,         // Tasca en procés de recepció del fitxer
    TASK_RECEIVED,          // Fitxer completament rebut, però sense processar
    TASK_PROCESSING,        // Fitxer en procés de distorsió
    TASK_PROCESSED,         // Fitxer distorsionat, preparat per enviar
    TASK_SENDING,           // Fitxer distorsionat en procés d'enviament
    TASK_COMPLETED,         // Tasca completada amb èxit
    TASK_INTERRUPTED,
    TASK_FAILED             // Tasca fallida
} TaskStatus;

// Estructura per gestionar metadades d'una tasca
typedef struct {
    char file_name[256];      // Nom del fitxer associat a la tasca
    uint32_t total_size;      // Mida total del fitxer
    uint32_t processed_offset; // Posició processada fins ara
    uint32_t bytes_sent;      // Bytes enviats fins ara
    TaskStatus status;        // Estat actual de la tasca
    char checksum[33];        // Checksum del fitxer original
    int factor;               // Factor de distorsió
} TaskMetadata;

typedef enum { ENVIANT, ESPERANT, REBENT, FINALITZAT } EstatFase;

typedef struct DistorsioStatus {
    char file_name[256];
    int progress;
    EstatFase estat;
    uint32_t mida_original;
    uint32_t mida_distorsionat;
    uint32_t bytes_enviats;
    uint32_t bytes_rebuts;
    int error; // 1 si hi ha hagut un error, 0 si no
    struct DistorsioStatus *next;
} DistorsioStatus;


typedef struct {
    char worker_ip[16];
    int worker_port;
    char file_name[256];
    int factor;
    int worker_fd;
} DistorsioArgs;

// Tipus d’instàncies al sistema
typedef enum {
    GOTHAM,   // Servei central
    ENIGMA,   // Worker per distorsionar text
    HARLEY,   // Worker per distorsionar media
    FLECK     // Client del sistema
} InstanceType;

// Estructura base per configuracions
typedef struct {
    InstanceType type;            // Tipus d’instància
    char *ip;                     // Adreça IP
    int port;                     // Port
    int socket_fd;                // Descriptor del socket
} ConfigBase;

// Estructura de configuració per Gotham
typedef struct {
    ConfigBase base;              // Configuració base
    char *worker_ip;              // Adreça IP dels workers
    int worker_port;              // Port dels workers
} ConfigGotham;

// Estructura de configuració per Fleck
typedef struct {
    ConfigBase base;              // Configuració base
    int fleck_id;        // Identificador únic del worker
    char *username;              // nom d'usuari del Fleck
    char *work_dir;       // Directori de treball del Fleck
    time_t last_heartbeat;  // Timestamp de l'últim heartbeat rebut
} ConfigFleck;

// Estructura de configuració per un worker
typedef struct {
    ConfigBase base;      // Configuració base (IP, port, tipus d'instància)
    int worker_id;        // Identificador únic del worker
    char *work_dir;       // Directori de treball del worker
    char *ip_fleck;       // IP on el worker escolta connexions de Fleck
    int port_fleck;       // Port on el worker escolta connexions de Fleck
    int is_primary_worker; // Indica si és el worker principal
    time_t last_heartbeat; // Timestamp de l'últim heartbeat rebut
} ConfigWorker;




#endif
