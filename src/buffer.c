#include "buffer.h"
#include "file.h"
#include "page.h"
#include "dbbpt.h"

long long hash_table[HASH_SIZE];
long long first_empty_buffer;
long long LRU_head;
long long LRU_tail;
long long read_count, close_count, pinned_count;
Buffer* buffers;

pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

void print_hash(int size) {
	int i;
	for(i = 0 ; i < size ; i++) {
		printf("hash_table[%d] : %lld\t",i,hash_table[i]);
		int hash_index = hash_table[i];
		while(hash_index != -1) {
			
			printf("hash_index[%d] : %d\t",i,hash_index);
			hash_index = buffers[hash_index].next_chain;
		}
		printf("\n");
	}
}

void print_LRU_list() {
	int buffer_index = LRU_tail;
	while(buffer_index!=-1) {
		printf("buffer_index : %d\t",buffer_index);
		buffer_index = buffers[buffer_index].next_of_LRU;
	}
}

void print_buffer(int size) {
	int i;
	for(i = 0 ; i < size ; i++) {
		printf("buffers[%d] LRU n, p : %lld, %lld\n",i,buffers[i].next_of_LRU,buffers[i].prev_of_LRU);
		printf("page, tid : %lld, %d\n",buffers[i].page_num,buffers[i].table_id);
		printf("is_dirty : %d\n",buffers[i].is_dirty);
		printf("next empty, chain n, p : %lld, %lld, %lld\n\n",buffers[i].next_empty_buffer,buffers[i].next_chain,buffers[i].prev_chain);	
	}	
}

void print_cur_status() {
	printf("first_empty_bufer : %lld\nLRU_head, LRU_tail :  %lld, %lld\npinned_count : %lld",first_empty_buffer,LRU_head,LRU_tail,pinned_count);
}

int shutdown_buffer(int num_buf) {
	int i;
	int buffer_index = LRU_tail;
	while(buffer_index != -1) {
		int next_index = buffers[buffer_index].next_of_LRU;
		file_write_page(buffers[buffer_index].table_id,buffers[buffer_index].page_num,&buffers[buffer_index].frame);
		buffer_index = next_index;
	}
	free(buffers);
	return 0;
}

int init_buffer(int num_buf) {
    int i;
    buffers = (Buffer*)malloc(num_buf * sizeof(Buffer));
    for( i = 0 ; i < num_buf ; i++) {
        buffers[i].is_dirty = 0;
		buffers[i].is_lock = 0;
        pthread_mutex_init(&buffers[i].mutex, NULL);
        buffers[i].next_of_LRU = -1;
        buffers[i].prev_of_LRU = -1;
        buffers[i].next_chain = -1;
        buffers[i].prev_chain = -1;
        buffers[i].next_empty_buffer = i+1;
        if(i == num_buf-1) buffers[i].next_empty_buffer = -1;
    }
    first_empty_buffer = 0;
    LRU_head = -1;
    LRU_tail = -1;
    memset(hash_table,-1,sizeof(hash_table));

	// init_lock_table();

    return 0;
}

int find_page(int table_id, long long page_num) {
    int buffer_index = hash_table[page_num%HASH_SIZE];
    int is_exist = 1;
    if(buffer_index == -1) {
        //buffers에 존재하지 않는 페이지
        is_exist = 0;
    } else {
        while(buffers[buffer_index].table_id != table_id) {
            if(buffers[buffer_index].next_chain == -1) {
                //buffers에 존재하지 않는 페이지
                is_exist = 0;
                break;
            }
            buffer_index = buffers[buffer_index].next_chain;
        }
    }
    if(is_exist) {
        return buffer_index;
    }
    return -1; 
}

page_t* buffer_read_page(int table_id, long long page_num) {
	pthread_mutex_lock(&mutex);
    int buffer_index = find_page(table_id, page_num);
    if(buffer_index == -1) {
        //does not exist
        if(first_empty_buffer == -1) {
            //reaplace
            buffer_index = LRU_tail;
            int next_chain = buffers[buffer_index].next_chain;
            int prev_chain = buffers[buffer_index].prev_chain;
			while(buffers[buffer_index].is_lock) {
				buffer_index = buffers[buffer_index].next_of_LRU;
			}
            if(buffers[buffer_index].is_dirty) {
                file_write_page(table_id,buffers[buffer_index].page_num,(page_t*)&buffers[buffer_index].frame);
            }
            file_read_page(table_id,page_num, (page_t*)&buffers[buffer_index].frame);
            if(next_chain == -1 && prev_chain == -1) {
				hash_table[buffers[buffer_index].page_num%HASH_SIZE] = -1;
			} 
			if(next_chain != -1) {
				buffers[next_chain].prev_chain = prev_chain;
			}
			if(prev_chain != -1) {
				buffers[prev_chain].next_chain = next_chain;
			}
        } else {
            //read from disk
            buffer_index = first_empty_buffer;
            file_read_page(table_id,page_num, (page_t*)&buffers[buffer_index].frame);
			first_empty_buffer = buffers[buffer_index].next_empty_buffer;
        }
        //init buffer_index buffer
		buffers[buffer_index].prev_chain = -1;
		buffers[buffer_index].next_chain = -1;
        buffers[buffer_index].is_dirty = 0;
        buffers[buffer_index].page_num = page_num;
        buffers[buffer_index].table_id = table_id;
        buffers[buffer_index].next_empty_buffer = -1;
        
		//update hash_table 
		if(hash_table[page_num%HASH_SIZE] == -1) {
			hash_table[page_num%HASH_SIZE] = buffer_index;
		} else {
			buffers[hash_table[page_num%HASH_SIZE]].prev_chain = buffer_index;
			buffers[buffer_index].next_chain = hash_table[page_num%HASH_SIZE];
			hash_table[page_num%HASH_SIZE] = buffer_index;
		}
    }

	pthread_mutex_unlock(&mutex);
	pthread_mutex_lock(&buffers[buffer_index].mutex);
	buffers[buffer_index].is_lock = 1;
	pthread_mutex_lock(&mutex);
	
    if(LRU_head == -1 && LRU_tail == -1) {
        LRU_head = 0;
        LRU_tail = 0;
    }
    else {
		if(buffer_index != LRU_head) {
			if (buffers[buffer_index].next_of_LRU != -1) {
				if (buffers[buffer_index].prev_of_LRU == -1) {
					buffers[buffers[buffer_index].next_of_LRU].prev_of_LRU = -1;
					LRU_tail = buffers[buffer_index].next_of_LRU;

				} else {
					buffers[buffers[buffer_index].next_of_LRU].prev_of_LRU = buffers[buffer_index].prev_of_LRU;
					buffers[buffers[buffer_index].prev_of_LRU].next_of_LRU = buffers[buffer_index].next_of_LRU;
				}
			}
			buffers[buffer_index].next_of_LRU = -1;
			buffers[buffer_index].prev_of_LRU = LRU_head;
			buffers[LRU_head].next_of_LRU = buffer_index;
			LRU_head = buffer_index;
			
        }
    }

	page_t* ret = &buffers[buffer_index].frame;

	pthread_mutex_unlock(&mutex);


    return ret;
}

void buffer_close_page(int table_id, long long page_num,int is_dirty) {
    int buffer_index = find_page(table_id, page_num);
    if(!buffers[buffer_index].is_dirty) buffers[buffer_index].is_dirty = is_dirty;
	buffers[buffer_index].is_lock = 0;
	pthread_mutex_unlock(&buffers[buffer_index].mutex);
}

void buffer_close_table(int table_id) {
	int buffer_index = LRU_tail;
	while(buffer_index != -1) {
		int next_index = buffers[buffer_index].next_of_LRU;
		int prev_index = buffers[buffer_index].prev_of_LRU;
		int next_chain, prev_chain;
		if(buffers[buffer_index].table_id == table_id) {
			next_chain = buffers[buffer_index].next_chain;
			prev_chain = buffers[buffer_index].prev_chain;
			if(buffers[buffer_index].is_dirty) {
				file_write_page(table_id,buffers[buffer_index].page_num,&buffers[buffer_index].frame);
				buffers[buffer_index].is_dirty = 0;
			}
			if(next_chain == -1 && prev_chain == -1) {
				hash_table[buffers[buffer_index].page_num%HASH_SIZE] = -1;
			} 
			if(next_chain != -1) {
				buffers[next_chain].prev_chain = prev_chain;
			}
			if(prev_chain != -1) {
				buffers[prev_chain].next_chain = next_chain;
			}
			buffers[buffer_index].next_empty_buffer = first_empty_buffer;
			first_empty_buffer = buffer_index;
			if(next_index == -1 && prev_index == -1) {
				LRU_head = -1;
				LRU_tail = -1;
			} else if(next_index == -1) {
				buffers[prev_index].next_of_LRU = -1;
				LRU_head = prev_index;
			} else if(prev_index == -1) {
				buffers[next_index].prev_of_LRU = -1;
				LRU_tail = next_index;
			} else {
				buffers[next_index].prev_of_LRU = prev_index;
				buffers[prev_index].next_of_LRU = next_index;
			}
		}
		buffer_index = next_index;
	}
}
