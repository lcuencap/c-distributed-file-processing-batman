#include "task_manager.h"


// Inicialitza el gestor de tasques
void initTaskManager(TaskManager *manager) {
    manager->head = NULL;
    manager->tail = NULL;
    pthread_mutex_init(&manager->mutex, NULL);
}

// Afegeix una tasca al gestor
void addTask(TaskManager *manager, Task task) {
    Task *new_task = malloc(sizeof(Task));
    if (!new_task) {
        perror("Error al·locant memòria per una nova tasca");
        return;
    }
    *new_task = task;
    new_task->next = NULL;
    new_task->prev = NULL;

    pthread_mutex_lock(&manager->mutex);
    if (!manager->tail) {
        manager->head = manager->tail = new_task;
    } else {
        manager->tail->next = new_task;
        new_task->prev = manager->tail;
        manager->tail = new_task;
    }
    pthread_mutex_unlock(&manager->mutex);
}

// Elimina una tasca del gestor
void removeTask(TaskManager *manager, Task *task) {
    pthread_mutex_lock(&manager->mutex);

    if (task->prev) task->prev->next = task->next;
    if (task->next) task->next->prev = task->prev;

    if (task == manager->head) manager->head = task->next;
    if (task == manager->tail) manager->tail = task->prev;

    free(task);

    pthread_mutex_unlock(&manager->mutex);
}

// Actualitza l'estat d'una tasca
void updateTaskStatus(TaskManager *manager, int task_id, TaskStatus new_status) {
    pthread_mutex_lock(&manager->mutex);

    Task *current = manager->head;
    while (current) {
        if (current->task_id == task_id) {
            current->status = new_status;
            break;
        }
        current = current->next;
    }

    pthread_mutex_unlock(&manager->mutex);
}

// Cerca una tasca pel seu ID
Task *findTaskById(TaskManager *manager, int task_id) {
    pthread_mutex_lock(&manager->mutex);

    Task *current = manager->head;
    while (current) {
        if (current->task_id == task_id) {
            pthread_mutex_unlock(&manager->mutex);
            return current;
        }
        current = current->next;
    }

    pthread_mutex_unlock(&manager->mutex);
    return NULL;
}

// Destrueix el gestor de tasques
void destroyTaskManager(TaskManager *manager) {
    pthread_mutex_lock(&manager->mutex);
    Task *current = manager->head;

    while (current) {
        Task *next = current->next;
        free(current);
        current = next;
    }

    manager->head = manager->tail = NULL;
    pthread_mutex_unlock(&manager->mutex);
    pthread_mutex_destroy(&manager->mutex);
}