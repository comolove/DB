#include "file.h"
#include "page.h"
#include "dbbpt.h"
#include "buffer.h"

int fd[11];

int file_open_data(int table_id, char* pathname) {
    fd[table_id] = open(pathname, O_SYNC | O_CREAT | O_RDWR, S_IRWXU | S_IRGRP | S_IROTH);
    if(fd[table_id] == -1) {
        return -1;
    }
    return lseek(fd[table_id],0,SEEK_END);
}

int file_close_data(int table_id) {
	buffer_close_table(table_id);
	return close(fd[table_id]);
}
	
// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page(int table_id, page_t* head){
    pagenum_t ret;
    FreePage fp;
    
    HeaderPage* hp = (HeaderPage*)head;
    if(!hp->freePage) {
	    fp.nextFreePage = 0;
        file_write_page(table_id,hp->numOfPages,(page_t*)&fp);
        
        ret = hp->numOfPages++;
    } else {
        ret = hp->freePage;
        FreePage* curfp;
        curfp = (FreePage*)buffer_read_page(table_id,ret);
        hp->freePage = curfp->nextFreePage;
        buffer_close_page(table_id,ret,1);
    }
    return ret;
}
// Free an on-disk page to the free page list
void file_free_page(int table_id, page_t* page,pagenum_t pagenum,page_t* head){
    HeaderPage* hp;
    hp = (HeaderPage*)head;
    FreePage* rfp;
    rfp = (FreePage*)page;
    rfp->nextFreePage = hp->freePage;
    hp->freePage = pagenum;
}
// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(int table_id, pagenum_t pagenum, page_t* dest){
    lseek(fd[table_id],pagenum*PAGE_SIZE,SEEK_SET);
    read(fd[table_id],dest->read_buffer,PAGE_SIZE);
}
// Write an in-memory page(src) to the on-disk page
void file_write_page(int table_id, pagenum_t pagenum, const page_t* src){
    lseek(fd[table_id],pagenum*PAGE_SIZE,SEEK_SET);
    write(fd[table_id],src->read_buffer,PAGE_SIZE);
}
