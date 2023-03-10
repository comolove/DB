#ifndef __FILE_H__
#define __FILE_H__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

#define PAGE_SIZE 4096

typedef unsigned long long pagenum_t;
typedef struct page_t {
    char read_buffer[PAGE_SIZE];
// in-memory page structure
} page_t;
int file_open_data(int table_id, char* pathname);
int file_close_data(int table_id);
// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page(int table_id,page_t* head);
// Free an on-disk page to the free page list
void file_free_page(int table_id, page_t* page,pagenum_t pagenum,page_t* head);
// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(int table_id, pagenum_t pagenum, page_t* dest);
// Write an in-memory page(src) to the on-disk page
void file_write_page(int table_id, pagenum_t pagenum, const page_t* src);

#endif
