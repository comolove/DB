#ifndef __BUFFER_H_
#define __BUFFER_H_

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "file.h"
#include "lock_manager.h"

#pragma pack(push,1)

typedef struct Buffer {
    page_t frame;
    int table_id;
    long long page_num;
    int is_dirty;
    int is_lock;
    pthread_mutex_t mutex;
    long long next_of_LRU;
    long long prev_of_LRU;
    long long next_chain;
    long long prev_chain;
    long long next_empty_buffer;
} Buffer;

int init_buffer(int num_buf);
int shutdown_buffer(int num_buf);

void print_hash(int size);
void print_buffer(int size);
void print_cur_status();
void print_LRU_list();

page_t* buffer_read_page(int table_id, long long page_num);
void buffer_close_page(int table_id, long long page_num,int is_dirty);
void buffer_close_table(int table_id);
int find_page(int table_id, long long page_num);

#pragma pack(pop)

#endif
