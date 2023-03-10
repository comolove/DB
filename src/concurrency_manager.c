#include "concurrency_manager.h"

struct lock_t {
	lock_t* prev;
	lock_t* next;
	lock_hash_node* sentinel;
	pthread_cond_t cond;
    int lock_mode;
    lock_t* trx_next_lock;
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
    lock_t* next_lock;
    trx_hash_node* next_hash;
};