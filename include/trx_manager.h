#ifndef _TRX_MANAGER_H__
#define _TRX_MANAGER_H__

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <pthread.h>
#include <unistd.h>
#include "concurrency_manager.h"

int init_trx_table();
int trx_begin();
void trx_abort(int trx_id);
int trx_commit(int trx_id);
void set_trx_hash_node(int trx_id, lock_t* new_lock);
int trx_remove_wait_lock(lock_t* old_wait_lock, int trx_id);
void trx_add_wait_lock(lock_t* new_wait_lock, int trx_id);
int detect_dead_lock(int start, int end);
trx_hash_node* trx_find_hash_node(int trx_id);

#endif
