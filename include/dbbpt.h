#ifndef _DBBPT_H__
#define _DBBPT_H__

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include "file.h"
#include "buffer.h"

#define LEAF_ORDER 32
#define INTERNAL_ORDER 249

int init_db(int num_buf);
int shutdown_db();

int open_table(char *pathname);
int close_table(int table_id);
void print_page(int table_id,int64_t pagenum);
void db_insert_parent(int table_id,int64_t pagenum, int64_t newKey, int64_t newPage, int64_t oldPage);
void db_set_parent_from_child(int table_id,int64_t childPage, int64_t curPage);
int db_insert(int table_id,int64_t key, char* value);
void roll_back(lock_t* lock_obj);
int db_update(int table_id, int64_t key, char* values, int trx_id);
int db_find(int table_id,int64_t key, char* ret_val,int trx_id);
int db_find_with_hp(int table_id,int64_t key, char* ret_val,page_t* head, int mode,lock_t* lock);
int64_t db_find_left_sibling(int table_id,int64_t key,int64_t curPageNum);
int64_t db_find_leaf_page(int table_id,int64_t key,int64_t curPageNum);
void db_delete_parent(int table_id,int64_t key,int64_t curPageNum);
int db_delete(int table_id,int64_t key);

#endif
