#ifndef WORKER_H
#define WORKER_H

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <dirent.h>
#include <errno.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <signal.h>
#include <sys/mman.h>
#include "../Utils/trames.h"
#include "../Utils/sys_params.h"
#include "../Utils/file_utils.h"
#include "../Utils/task_manager.h"

#define SHARED_MEM_SIZE 1024 * 1024 // 1 MB de memoria compartida
#define MAX_QUEUE_SIZE 10


// Funcions generals
Task *get_next_task(TaskManager *manager);
void remove_task(TaskManager *manager, int client_socket, pthread_mutex_t *mutex);
void mark_incomplete_tasks(TaskManager *manager, void *shared_mem, size_t shared_mem_size, pthread_mutex_t *shared_mem_mutex);
int gestionar_recepcio_fitxer(int client_socket, TaskMetadata *metadata, pthread_mutex_t *mutex);
void assegurar_working_directory(char *work_dir, int harley);
int detectar_desconnexio_fleck(int client_socket);
void finalitzar_tasca_per_desconnexio(TaskMetadata *metadata, const char *file_name, int socket,
                                       TaskManager *task_manager, void *shared_mem,
                                       size_t current_mem_size, pthread_mutex_t *shared_mem_mutex);
void gestionar_desconnexio_fleck(const char *file_name, int client_socket, void *shared_mem, size_t shared_mem_size, pthread_mutex_t *mutex);
int enviar_fitxer_processat(int client_socket, TaskMetadata *metadata, pthread_mutex_t *mutex,
                             int gotham_socket_fd);
int expandir_memoria_compartida(void **shared_mem, size_t *current_mem_size, int shared_mem_fd);
TaskMetadata *alloc_task_metadata(const char *file_name, uint32_t file_size, const char *checksum,
                                   void **shared_mem_ptr, size_t *mem_size,
                                  int shared_mem_fd, pthread_mutex_t *mutex);
void free_task_metadata(TaskMetadata *metadata, pthread_mutex_t *mutex);
void mostrar_estat_fitxer(const char *nom);
void netejar_connexio_per_error(int client_socket, TaskMetadata *metadata, pthread_mutex_t *shared_mem_mutex, int *active_threads, pthread_mutex_t *threads_mutex);


// Funcions de gestió de memòria compartida
int inicialitzar_memoria_compartida(const char *shm_name, int *shared_mem_fd, void **shared_mem, size_t mem_size, pthread_mutex_t *mutex);
TaskMetadata *buscar_tasca_per_nom(void *shared_mem, size_t current_mem_size, const char *file_name, pthread_mutex_t *mutex);
void netejar_memoria_tasques(void *shared_mem, size_t mem_size, pthread_mutex_t *shared_mem_mutex);
void volcar_memoria_tasques(void *shared_mem, size_t mem_size);

// Funcions de comunicació amb Gotham
int inicialitzar_connexio_gotham(ConfigWorker *config);
void gestionar_desconnexio_gotham(int socket_fd, const char *workerType);
int reconnectar_gotham(ConfigWorker *config, int keep_running);

// Funcions per la gestió de senyals
void gestionar_signal_interrupt_worker(int sig, TaskManager *task_manager, ConfigWorker *config, 
                                void *shared_mem, int *shared_mem_fd, const char *shm_name, 
                                pthread_mutex_t *task_mutex, size_t shared_mem_size, pthread_mutex_t *shared_mem_mutex);



void enqueue_connection(int client_socket, int* connection_queue, int* queue_rear, int* queue_count, pthread_mutex_t* queue_mutex, pthread_cond_t *queue_not_empty, pthread_cond_t *queue_not_full);    
int dequeue_connection(int* connection_queue, int* queue_front, int* queue_count,
    pthread_mutex_t* queue_mutex, pthread_cond_t* queue_not_empty, pthread_cond_t* queue_not_full, int keep_running);
void add_task_to_manager(TaskManager *manager, int client_socket, const char *file_name, ConfigWorker *config);
void marcar_tasca_com_fallida(TaskMetadata *metadata);

#endif // WORKER_H
