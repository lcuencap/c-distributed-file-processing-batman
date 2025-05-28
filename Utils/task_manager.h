#ifndef TASK_MANAGER_H
#define TASK_MANAGER_H

#include <pthread.h>
#include "sys_params.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>

// Inicialitza el gestor de tasques
void initTaskManager(TaskManager *manager);

// Afegeix una tasca al gestor
void addTask(TaskManager *manager, Task task);

// Elimina una tasca del gestor
void removeTask(TaskManager *manager, Task *task);

// Actualitza l'estat d'una tasca
void updateTaskStatus(TaskManager *manager, int task_id, TaskStatus new_status);

// Cerca una tasca pel seu identificador únic
Task *findTaskById(TaskManager *manager, int task_id);

// Destrueix el gestor de tasques i allibera tots els recursos
void destroyTaskManager(TaskManager *manager);

// Destrueix el gestor de tasques
void destroyTaskManager(TaskManager *manager);

#endif
