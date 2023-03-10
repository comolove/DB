#include "trx_manager.h"
#include "lock_manager.h"
#include "dbbpt.h"

int trx_num = 1;

trx_hash_node* trx_hash_table[HASH_SIZE];

pthread_mutex_t trx_manager_mutex;

int init_trx_table() {
    int i;
    for(i=0;i<HASH_SIZE;i++) {
        trx_hash_table[i] = NULL;
    }
    trx_manager_mutex = (pthread_mutex_t)PTHREAD_MUTEX_INITIALIZER;
}

//trx_hash_table에 추가함
int trx_begin() {
    pthread_mutex_lock(&trx_manager_mutex);
    trx_hash_node* new_trx = (trx_hash_node*)malloc(sizeof(trx_hash_node));
    new_trx->next_hash = trx_hash_table[trx_num%HASH_SIZE];
    new_trx->is_visit = 0;
    new_trx->is_abort = 0;
    new_trx->lock_head = NULL;
    new_trx->wait_head = NULL;
    new_trx->trx_id = trx_num;
    new_trx->mutex = (pthread_mutex_t)PTHREAD_MUTEX_INITIALIZER;
    trx_hash_table[trx_num%HASH_SIZE] = new_trx;
    // printf("%d is started\n",trx_num);
    int ret = trx_num++;
    pthread_mutex_unlock(&trx_manager_mutex);
    return ret;
}

void trx_abort(int trx_id, int is_locked) {
    if(!is_locked) lock_manager_mutex_lock();
    pthread_mutex_lock(&trx_manager_mutex);
    // printf("%d is aborted!!!!!!!!!!!!!!!!!!!!!!!!!!\n",trx_id);
    trx_hash_node* cur_trx = trx_hash_table[trx_id%HASH_SIZE];
    trx_hash_node* before_trx = NULL;
    while(cur_trx != NULL && cur_trx->trx_id != trx_id) {
        before_trx = cur_trx;
        cur_trx = cur_trx->next_hash;
    }
    if(before_trx) {
        before_trx->next_hash = cur_trx->next_hash;
    } else {
        trx_hash_table[trx_id%HASH_SIZE] = NULL;
    }
    lock_t* cur_lock = cur_trx->lock_head;
    lock_t* next_lock = NULL;
    while(cur_lock != NULL) {
        next_lock = cur_lock->trx_next_lock;
        if(cur_lock->lock_mode == 1)
            roll_back(cur_lock);
        cur_lock = next_lock;
    }
    cur_lock = cur_trx->lock_head;
    while(cur_lock != NULL) {
        next_lock = cur_lock->trx_next_lock;
        pthread_mutex_unlock(&trx_manager_mutex);
        lock_release(cur_lock);
        pthread_mutex_lock(&trx_manager_mutex);
        cur_lock = next_lock;
    }
    free(cur_trx);

    pthread_mutex_unlock(&trx_manager_mutex);
    if(!is_locked) lock_manager_mutex_unlock();
}

//trx_hash_table에서 제거하면서 걸린 lock을 모두 release함
int trx_commit(int trx_id) {
    lock_manager_mutex_lock();
    pthread_mutex_lock(&trx_manager_mutex);
    // printf("%d is commited\n",trx_id);
    
    trx_hash_node* cur_trx = trx_hash_table[trx_id%HASH_SIZE];
    trx_hash_node* before_trx = NULL;
    while(cur_trx != NULL && cur_trx->trx_id != trx_id) {
        before_trx = cur_trx;
        cur_trx = cur_trx->next_hash;
    }
    if(!cur_trx) {
        // printf("%d is already aborted",trx_id);
        pthread_mutex_unlock(&trx_manager_mutex);
        lock_manager_mutex_unlock();
        return 0;
    }
    else if(before_trx) {
        before_trx->next_hash = cur_trx->next_hash;
    } else {
        trx_hash_table[trx_id%HASH_SIZE] = NULL;
    }
    lock_t* cur_lock = cur_trx->lock_head;
    lock_t* next_lock = NULL;
    while(cur_lock != NULL) {
        next_lock = cur_lock->trx_next_lock;
        pthread_mutex_unlock(&trx_manager_mutex);
        lock_release(cur_lock);
        pthread_mutex_lock(&trx_manager_mutex);
        cur_lock = next_lock;
    }
    free(cur_trx);
    pthread_mutex_unlock(&trx_manager_mutex);
    lock_manager_mutex_unlock();
    return trx_id;
}

void set_trx_hash_node(int trx_id, lock_t* new_lock) {
    trx_hash_node* cur_trx = trx_find_hash_node(trx_id);
    pthread_mutex_lock(&cur_trx->mutex);
    new_lock->trx_next_lock = cur_trx->lock_head;
    cur_trx->lock_head = new_lock;
    new_lock->owner_trx_id = trx_id;
    pthread_mutex_unlock(&cur_trx->mutex);
}

int trx_remove_wait_lock(lock_t* old_wait_lock, int trx_id) {
    trx_hash_node* cur_trx = trx_find_hash_node(trx_id);
    pthread_mutex_lock(&cur_trx->mutex);
    // printf("%d is removed to %d's wait list\n",old_wait_lock->owner_trx_id ,trx_id);

    lock_t* new_wait_head = NULL;
    if(old_wait_lock->prev_wait_lock) {
        old_wait_lock->prev_wait_lock->next_wait_lock = old_wait_lock->next_wait_lock;
        new_wait_head = old_wait_lock->prev_wait_lock;
    }
    if(old_wait_lock->next_wait_lock) {
        old_wait_lock->next_wait_lock->prev_wait_lock = old_wait_lock->prev_wait_lock;
        new_wait_head = old_wait_lock->next_wait_lock;
    }
    if(old_wait_lock == cur_trx->wait_head) {
        cur_trx->wait_head = new_wait_head;
    }

    int ret = cur_trx->wait_head == NULL;    

    // lock_t* cur_lock = cur_trx->wait_head;

    // while(cur_lock!=NULL) {
    //     printf("%d ",cur_lock->owner_trx_id);
    //     cur_lock = cur_lock->next_wait_lock;
    // }
    // printf("\n");

    pthread_mutex_unlock(&cur_trx->mutex);

    return ret;
}

void trx_add_wait_lock(lock_t* new_wait_lock, int trx_id) {
    trx_hash_node* cur_trx = trx_find_hash_node(trx_id);
    pthread_mutex_lock(&cur_trx->mutex);
    // printf("%d is added to %d's wait list\n",new_wait_lock->owner_trx_id ,trx_id);

    new_wait_lock->next_wait_lock = cur_trx->wait_head;
    if(cur_trx->wait_head) cur_trx->wait_head->prev_wait_lock = new_wait_lock;
    cur_trx->wait_head = new_wait_lock;

    // lock_t* cur_lock = cur_trx->wait_head;

    // while(cur_lock!=NULL) {
    //     printf("%d ",cur_lock->owner_trx_id);
    //     cur_lock = cur_lock->next_wait_lock;
    // }
    // printf("\n");

    pthread_mutex_unlock(&cur_trx->mutex);
}

int detect_dead_lock(int start, int end) {
    trx_hash_node* cur_trx = trx_find_hash_node(start);

    pthread_mutex_lock(&cur_trx->mutex);
    lock_t* cur_lock = cur_trx->wait_head;

    cur_trx->is_visit = 1;

    int ret = 0;

    while(cur_lock != NULL) {
        if(cur_lock->owner_trx_id == end) {
            ret = 1;
        }
        else {
            int next = cur_lock->owner_trx_id;
            trx_hash_node* next_trx = trx_find_hash_node(next);
            // printf("next %d is visitied %d\n",next,next_trx->is_visit);
            if(!next_trx->is_visit) ret = detect_dead_lock(next,end);
        } 
        if(ret) break;
        cur_lock = cur_lock->next_wait_lock;
    }

    cur_trx->is_visit = 0;

    pthread_mutex_unlock(&cur_trx->mutex);
    
    return ret;
}

trx_hash_node* trx_find_hash_node(int trx_id) {
    pthread_mutex_lock(&trx_manager_mutex);
    trx_hash_node* cur_trx = trx_hash_table[trx_id%HASH_SIZE];
    while(cur_trx != NULL && cur_trx->trx_id != trx_id) {
        cur_trx = cur_trx->next_hash;
    }
    pthread_mutex_unlock(&trx_manager_mutex);
    return cur_trx;
}