#include "lock_manager.h"
#include "trx_manager.h"

lock_hash_node* lock_hash_table[10][LOCK_HASH];

pthread_mutex_t lock_mutex;

int init_lock_table(){
    int i,j;
    for(i=0;i<10;i++) {
        for(j=0;j<LOCK_HASH;j++){
            lock_hash_table[i][j] = NULL;
        }
    }
    lock_mutex = (pthread_mutex_t)PTHREAD_MUTEX_INITIALIZER;
}

lock_t* lock_acquire(int table_id, int64_t key, int trx_id, int lock_mode) {
    if(trx_id == 0) return 1;
    pthread_mutex_lock(&lock_mutex);

    int is_dead_lock = 0;
    lock_t* ret = NULL;
    lock_hash_node* cur_node = lock_hash_table[table_id%10][key%LOCK_HASH];
    //find lock in lock_hash_table
    while(cur_node != NULL && !(cur_node->table_id == table_id && cur_node->record_id == key)) {
        cur_node = cur_node->next_hash;
    }

    lock_t* new_lock = (lock_t*)malloc(sizeof(lock_t));
    //acquire
    if(cur_node == NULL || (lock_mode == 0 && cur_node->tail->lock_mode == 0 && cur_node->tail->is_acquire == 1)) {
        if(cur_node == NULL) {
            lock_hash_node* new_node = (lock_hash_node*)malloc(sizeof(lock_hash_node));
            init_lock(new_lock,NULL,NULL,new_node,lock_mode,1);
            new_node->table_id = table_id;
            new_node->record_id = key;
            new_node->head = new_lock;
            new_node->tail = new_lock;
            new_node->next_hash = lock_hash_table[table_id%10][key%LOCK_HASH];
            lock_hash_table[table_id%10][key%LOCK_HASH] = new_node;
        } else {
            init_lock(new_lock,cur_node->tail,NULL,cur_node,lock_mode,1);
            cur_node->tail->next = new_lock;
            cur_node->tail = new_lock;
        }
        set_trx_hash_node(trx_id,new_lock);
    } else {
        init_lock(new_lock,cur_node->tail,NULL,cur_node,lock_mode,0);
        lock_t* first_x_lock = cur_node->tail;
        int is_same = 1;
        while(first_x_lock != NULL && first_x_lock->lock_mode == 0)
            first_x_lock = first_x_lock->prev;
        if(lock_mode == 0 || cur_node->tail->lock_mode == 1) {
            if(first_x_lock->owner_trx_id != trx_id) {
                is_dead_lock = detect_dead_lock(first_x_lock->owner_trx_id,trx_id);
                if(is_dead_lock) {
                    free(new_lock);
                    trx_abort(trx_id,1);
                    pthread_mutex_unlock(&lock_mutex);
                    return NULL;
                }
                is_same = 0;
                trx_add_wait_lock(first_x_lock,trx_id);
            } else {
                if(lock_mode == 0) {
                    free(new_lock);
                    pthread_mutex_unlock(&lock_mutex);
                    return 1;
                }
            }
        } else {
            lock_t* cur_lock = cur_node->tail;
            while(cur_lock != first_x_lock) {
                if(cur_lock->owner_trx_id != trx_id) {
                    is_dead_lock = detect_dead_lock(cur_lock->owner_trx_id,trx_id);
                    if(is_dead_lock) {
                        free(new_lock);
                        trx_abort(trx_id,1);
                        pthread_mutex_unlock(&lock_mutex);
                        return NULL;
                    }
                } 
                cur_lock = cur_lock->prev;
            }
            cur_lock = cur_node->tail;
            while(cur_lock != first_x_lock) {
                if(cur_lock->owner_trx_id != trx_id) {
                    is_same = 0;
                    trx_add_wait_lock(cur_lock,trx_id);
                } 
                cur_lock = cur_lock->prev;
            }
        }

        cur_node->tail->next = new_lock;
        cur_node->tail = new_lock;
        set_trx_hash_node(trx_id,new_lock);

        if(is_same) {
            new_lock->is_acquire = 1;
        } else {
            pthread_cond_wait(&new_lock->cond,&lock_mutex);
        }

    }
    new_lock->is_acquire = 1;

    pthread_mutex_unlock(&lock_mutex);

    return new_lock;
}

int lock_release(lock_t* lock_obj) {
    lock_hash_node* sentinel = lock_obj->sentinel;
    //acquire
    if(lock_obj->next) {
        lock_obj->next->prev = lock_obj->prev;
    }
    if(lock_obj->prev) {
        lock_obj->prev->next = lock_obj->next;
    }
    if(sentinel->head == lock_obj) {
        sentinel->head = lock_obj->next;
    }
    if(sentinel->tail == lock_obj) {
        sentinel->tail = lock_obj->prev;
    }
    if(sentinel->head == NULL && sentinel->tail == NULL) {
        lock_hash_node* cur_node = lock_hash_table[sentinel->table_id%10][sentinel->record_id%LOCK_HASH];
        lock_hash_node* before_node = NULL;
        while(!(cur_node->table_id == sentinel->table_id && cur_node->record_id == sentinel->record_id)) {
            before_node = cur_node;
            cur_node = cur_node->next_hash;
        }
        if(before_node != NULL) {
            before_node->next_hash = cur_node->next_hash;
        } else {
            lock_hash_table[sentinel->table_id%10][sentinel->record_id%LOCK_HASH] = cur_node->next_hash;
        }
        free(sentinel);
    }
    if(lock_obj->is_acquire && lock_obj->next != NULL) {
        lock_t* next_lock = lock_obj->next;
        if(lock_obj->lock_mode == 1 && next_lock->lock_mode == 0) {
            lock_t* next_s_lock = next_lock;
            while(next_s_lock != NULL && next_s_lock->lock_mode != 1) {
                if(trx_remove_wait_lock(lock_obj,next_s_lock->owner_trx_id)) {
                    pthread_cond_signal(&next_s_lock->cond);
                    next_s_lock->is_acquire = 1;
                }
                next_s_lock = next_s_lock->next;
            }
        } 
        else {
            lock_t* first_x_lock = next_lock;
            while(first_x_lock != NULL && first_x_lock->lock_mode != 1) {
                first_x_lock = first_x_lock->next;
            }
            if(first_x_lock) {
                if(trx_remove_wait_lock(lock_obj,first_x_lock->owner_trx_id)) {
                    pthread_cond_signal(&first_x_lock->cond);
                    first_x_lock->is_acquire = 1;
                }
            }
        }
    }
    free(lock_obj->original);
    free(lock_obj);
    return 0;
}

void lock_manager_mutex_lock() {
    pthread_mutex_lock(&lock_mutex);
}

void lock_manager_mutex_unlock() {
    pthread_mutex_unlock(&lock_mutex);
}

void init_lock(lock_t* new_lock,lock_t* prev, lock_t* next, lock_hash_node* sentinel, int lock_mode, int is_acquire) {
    new_lock->prev = prev;
    new_lock->next = next;
    new_lock->sentinel = sentinel;
    new_lock->cond = (pthread_cond_t)PTHREAD_COND_INITIALIZER;
    new_lock->lock_mode = lock_mode;
    new_lock->next_wait_lock = NULL;
    new_lock->prev_wait_lock = NULL;
    new_lock->is_acquire = is_acquire;
    new_lock->original = (char*)malloc(sizeof(char)*120);
}
