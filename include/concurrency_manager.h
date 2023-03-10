#ifndef _CONCURRENCY_MANAGER_H__
#define _CONCURRENCY_MANAGER_H__

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <pthread.h>

#define HASH_SIZE 10000
#define LOCK_HASH HASH_SIZE/10

typedef struct lock_t lock_t;
typedef struct lock_hash_node lock_hash_node;
typedef struct trx_hash_node trx_hash_node;

struct lock_t {
    int lock_mode;
    int owner_trx_id;
    int is_acquire;
	pthread_cond_t cond;
	lock_t* prev;
	lock_t* next;
    lock_t* trx_next_lock;
    lock_t* next_wait_lock;
    lock_t* prev_wait_lock;
	lock_hash_node* sentinel;
    char* original;
};

struct lock_hash_node {
    int table_id;
    int record_id;
	lock_t* head;
	lock_t* tail;
    lock_hash_node* next_hash;
};

struct trx_hash_node {
    int trx_id;
    int is_visit;
    int is_abort;
    pthread_mutex_t mutex;
    lock_t* lock_head;
    lock_t* wait_head;
    trx_hash_node* next_hash;
};

#endif
