#ifndef _LOCK_MANAGER_H__
#define _LOCK_MANAGER_H__

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <pthread.h>
#include <unistd.h>
#include "concurrency_manager.h"

int init_lock_table();
lock_t* lock_acquire(int table_id, int64_t key, int trx_id, int lock_mode);
int lock_release(lock_t* lock_obj);
void lock_manager_mutex_lock();
void lock_manager_mutex_unlock();
void init_lock(lock_t* new_lock,lock_t* prev, lock_t* next, lock_hash_node* sentinel, int lock_mode, int is_acquire);

#endif
