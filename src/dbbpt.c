#include "dbbpt.h"
#include "lock_manager.h"
#include "trx_manager.h"
#include "buffer.h"
#include "file.h"
#include "page.h"

int tableID = 1;
int num_of_buffers;
int isInit = 0;
char path_names[11][21];

int init_db(int num_buf) {
	if(isInit) {
		printf("already init db\n");
		return -1;
	}
	int i;
	for(i = tableID ; i<=10 ;i++) path_names[i][0] = '\0';
	num_of_buffers = num_buf;
    init_buffer(num_of_buffers);
    init_lock_table();
    init_trx_table();
    isInit = 1;
    return 0;
}

int shutdown_db() {
	if(!isInit) {
		printf("init_db first\n");
		return -1;
	}
	shutdown_buffer(num_of_buffers);
	isInit = 0;
	return 0;
}

int open_table(char* pathname){
	int table_id = -1;
	int i;
	if(!isInit) {
		printf("init_db first\n");
		return -1;
	}
    for(i = 1;i<=10;i++) {
        if(path_names[i][0] == '\0') {
            table_id = tableID++;
            break;
        }
        if(!strcmp(pathname,path_names[i])) {
            table_id = i;
            break;
        }
    }
    if(table_id == -1) return -1;
	int ret = file_open_data(table_id,pathname);
	char path_name[21];
    if(ret == -1) return -1;
    if(!ret) {
        HeaderPage hp;
		hp.freePage = 0;
		hp.rootPage = 0;
		hp.numOfPages = 1;
        file_write_page(table_id,0,(page_t*)&hp);
    }
    strcpy(path_names[table_id],pathname);
    return table_id;
}

int close_table(int table_id){
	if(path_names[table_id] == NULL) return -1;
	if(file_close_data(table_id) == -1) {
		printf("fail file close\n");
		return -1;
	}	
	return 0;
}

void print_page(int table_id,int64_t pagenum) {
    int i;
    if(pagenum == 0){
        HeaderPage* hp;
        hp = (HeaderPage*)buffer_read_page(table_id,0);
        printf("freepage : %lld\nrootPage : %lld\nnumOfPage : %lld\n",hp->freePage,hp->rootPage,hp->numOfPages);
        buffer_close_page(table_id,0,0);
    } else {
        InternalPage* ip;
        ip = (InternalPage*)buffer_read_page(table_id,pagenum);
        if(ip->isLeaf) {
            LeafPage* lp = (LeafPage*)ip;
            printf("parentPage : %lld\nisLeaf : %d\nnumOfKeys : %d\nrightSibling : %lld\n",lp->parentPage,lp->isLeaf,lp->numOfKeys,lp->rightSibling);
            for(i = 0 ; i < lp->numOfKeys ; i++) printf("[%lld : %s]  ",lp->records[i].key,lp->records[i].value);
            printf("\n");
        } else {
            printf("parentPage : %lld\nisLeaf : %d\nnumOfKeys : %d\nleftMostPage : %lld\n",ip->parentPage,ip->isLeaf,ip->numOfKeys,ip->leftMostPage);
            for(i = 0 ; i < ip->numOfKeys ; i++) printf("[%lld : %lld]  ",ip->records[i].key,ip->records[i].pageNum);
            printf("\n");
        }
        buffer_close_page(table_id,pagenum,0);
    }
}

void db_set_parent_from_child(int table_id,int64_t childPage, int64_t curPage) {
    LeafPage* lp;
    lp = (LeafPage*)buffer_read_page(table_id,childPage);
    lp->parentPage = curPage;
    buffer_close_page(table_id,childPage,1);
}

void db_insert_parent(int table_id,int64_t pagenum, int64_t newKey, int64_t newPageNum, int64_t oldPageNum) {
    int left,right,mid,i;
    HeaderPage* hp;
    KeyPageNum newRecord, rightMostRecord;
    newRecord.key = newKey;
    newRecord.pageNum = newPageNum;
    hp = (HeaderPage*)buffer_read_page(table_id,0);
    if(pagenum == 0) {
        InternalPage* newRootPage;
        pagenum_t newRoot = file_alloc_page(table_id,(page_t*)hp);
        newRootPage = (InternalPage*)buffer_read_page(table_id,newRoot);
        hp->rootPage = newRoot;
        newRootPage->parentPage = 0;
        newRootPage->isLeaf = 0;
        newRootPage->numOfKeys = 1;
        newRootPage->leftMostPage = oldPageNum;
        db_set_parent_from_child(table_id,oldPageNum,newRoot);
        newRootPage->records[0] = newRecord;
        db_set_parent_from_child(table_id,newRootPage->records[0].pageNum,newRoot);
        
        buffer_close_page(table_id,0,1);
        buffer_close_page(table_id,newRoot,1);
        
        return;
    }
    
    InternalPage* curPage;
    curPage = (InternalPage*)buffer_read_page(table_id,pagenum);
    
    if(curPage->numOfKeys == INTERNAL_ORDER - 1) {
        int leftLength, rightLength;
        leftLength = INTERNAL_ORDER / 2;
        rightLength = INTERNAL_ORDER - leftLength - 1;
        InternalPage* newInternalPage;
        pagenum_t newPage = file_alloc_page(table_id,(page_t*)hp);
        newInternalPage = (InternalPage*)buffer_read_page(table_id,newPage); 
        
        left = 0;
        right = curPage->numOfKeys++ - 1;
        if(newKey < curPage->records[left].key) mid = left;
        else if(newKey > curPage->records[right].key) mid = right+1;
        else {
            while(right-left > 1) {
                mid = (left + right)/2;
                if(newKey > curPage->records[mid].key) left = mid;
                else right = mid;
            }
            mid=right;
        }

        for(i = curPage->numOfKeys - 1; i > mid ; i--) {
            if(i == curPage->numOfKeys - 1) rightMostRecord = curPage->records[i-1];
            else curPage->records[i] = curPage->records[i-1];
        }
        if(mid == INTERNAL_ORDER - 1) rightMostRecord = newRecord;
        else if(oldPageNum == -1) {
            curPage->records[mid].key = newRecord.key;
            curPage->records[mid].pageNum = curPage->leftMostPage;
            curPage->leftMostPage = newRecord.pageNum;
            db_set_parent_from_child(table_id,curPage->leftMostPage,pagenum);
        } else {
            curPage->records[mid] = newRecord;    
        }
        for(i = leftLength + 1 ; i < INTERNAL_ORDER - 1 ; i++) {
            newInternalPage->records[i-leftLength-1] = curPage->records[i];
            db_set_parent_from_child(table_id,newInternalPage->records[i-leftLength-1].pageNum,newPage);
        }
        newInternalPage->records[rightLength-1] = rightMostRecord;
        db_set_parent_from_child(table_id,newInternalPage->records[rightLength-1].pageNum,newPage);
        newInternalPage->leftMostPage = curPage->records[leftLength].pageNum;
        db_set_parent_from_child(table_id,newInternalPage->leftMostPage,newPage);
        
        curPage->numOfKeys = leftLength;
        newInternalPage->numOfKeys = rightLength;
        newInternalPage->isLeaf = 0;
        newInternalPage->parentPage = curPage->parentPage;
        
        buffer_close_page(table_id,pagenum,1);
        buffer_close_page(table_id,newPage,1);
        buffer_close_page(table_id,0,1);
        
        db_insert_parent(table_id,curPage->parentPage,curPage->records[leftLength].key,newPage,pagenum);
    }
    
    else {
        left = 0;
        right = curPage->numOfKeys++ - 1;
        if(newKey < curPage->records[left].key) mid = left;
        else if(newKey > curPage->records[right].key) mid = right+1;
        else {
            while(right-left > 1) {
                mid = (left + right)/2;
                if(newKey > curPage->records[mid].key) left = mid;
                else right = mid;
            }
            mid=right;
        }
        for(i = curPage->numOfKeys - 1; i > mid ; i--) curPage->records[i] = curPage->records[i-1];
        if(oldPageNum == -1) {
            curPage->records[mid].key = newRecord.key;
            curPage->records[mid].pageNum = curPage->leftMostPage;
            curPage->leftMostPage = newRecord.pageNum;
            db_set_parent_from_child(table_id,curPage->leftMostPage,pagenum);
        } else {
            curPage->records[mid] = newRecord;
            db_set_parent_from_child(table_id,curPage->records[mid].pageNum,pagenum);
        }
        buffer_close_page(table_id,0,0);
        buffer_close_page(table_id,pagenum,1);
    }
}

int db_insert(int table_id,int64_t key, char* value){
    int left,right,mid,i;
    HeaderPage* hp;
    hp = (HeaderPage*)buffer_read_page(table_id,0);
    pagenum_t curPage = 0;
    char* ret_val = (char*)malloc(120);
    KeyValue record;
    record.key = key;
    strcpy(record.value,value);
    
    pagenum_t root = hp->rootPage;
    curPage = root;
    if(!root) {
        root = file_alloc_page(table_id,(page_t*)hp);
        hp->rootPage = root;
        
        LeafPage* lp;
        lp = (LeafPage*)buffer_read_page(table_id,root);
        lp->parentPage = 0;
        lp->isLeaf = 1;
        lp->numOfKeys = 1;
        lp->rightSibling = 0;
        lp->records[0] = record;
        //이상할수도
        buffer_close_page(table_id,root,1);
        buffer_close_page(table_id,0,1);
        return 0;
    }
    
    if(db_find_with_hp(table_id,key,ret_val,(page_t*)hp,0,NULL) == 0) {
        buffer_close_page(table_id,0,0);
        return -1;
    }
    free(ret_val);
    InternalPage* ip;
    ip = (InternalPage*)buffer_read_page(table_id,root);
    
    while(!ip->isLeaf){
        left = 0;
        right = ip->numOfKeys-1;
        if(key < ip->records[left].key) {
            buffer_close_page(table_id,curPage,0);
            curPage = ip->leftMostPage;
            ip = (InternalPage*)buffer_read_page(table_id,ip->leftMostPage);
        }
        else if(key >= ip->records[right].key) {
            buffer_close_page(table_id,curPage,0);
            curPage = ip->records[right].pageNum;
            ip = (InternalPage*)buffer_read_page(table_id,ip->records[right].pageNum);
        }
        else {
            while(right-left > 1) {
                mid = (left + right)/2;
                if(key >= ip->records[mid].key) left = mid;
                else right = mid;
            }
            buffer_close_page(table_id,curPage,0);
            curPage = ip->records[left].pageNum;
            ip = (InternalPage*)buffer_read_page(table_id,ip->records[left].pageNum);
        }
    }
    LeafPage* lp = (LeafPage*)ip;
    //split
    if(lp->numOfKeys == LEAF_ORDER-1) {
        LeafPage* newlp;
        pagenum_t newPage;
        KeyValue rightMostRecord;
        int leftLength = LEAF_ORDER/2;
        int rightLength = LEAF_ORDER - leftLength;
        newPage = file_alloc_page(table_id,(page_t*)hp);
        newlp = (LeafPage*)buffer_read_page(table_id,newPage);
        // insert
        left = 0;
        right = lp->numOfKeys++ - 1;
        if(key < lp->records[left].key) mid = left;
        else if(key > lp->records[right].key) mid = right+1;
        else {
            while(right-left > 1) {
                mid = (left + right)/2;
                if(key > lp->records[mid].key) left = mid;
                else right = mid;
            }
            mid=right;
        }
        
        for(i = lp->numOfKeys-1 ; i > mid ; i--) {
            if(i == lp->numOfKeys-1) {
                rightMostRecord = lp->records[i-1];
                continue;
            }
            lp->records[i] = lp->records[i-1];
        }
        if(mid == LEAF_ORDER - 1) rightMostRecord = record;
        else lp->records[mid] = record;
        
        //split
        for(i = leftLength ; i<LEAF_ORDER-1 ; i++) newlp->records[i-leftLength] = lp->records[i];
        newlp->records[rightLength-1] = rightMostRecord;
        
        lp->numOfKeys = leftLength;
        pagenum_t temp = lp->rightSibling;
        lp->rightSibling = newPage;
        newlp->rightSibling = temp;
        newlp->numOfKeys = rightLength;
        newlp->parentPage = lp->parentPage;
        newlp->isLeaf = 1;
        
        buffer_close_page(table_id,0,1);
        buffer_close_page(table_id,curPage,1);
        buffer_close_page(table_id,newPage,1);
        
        db_insert_parent(table_id,lp->parentPage,newlp->records[0].key,newPage,curPage);
    }
    
    else {
        left = 0;
        right = lp->numOfKeys++ - 1;
        if(key < lp->records[left].key) mid = left;
        else if(key > lp->records[right].key) mid = right+1;
        else {
            while(right-left > 1) {
                mid = (left + right)/2;
                if(key > lp->records[mid].key) left = mid;
                else right = mid;
            }
            mid=right;
        }
        for(i = lp->numOfKeys-1 ; i > mid ; i--) lp->records[i] = lp->records[i-1];
        lp->records[mid] = record;
        buffer_close_page(table_id,0,0);
        buffer_close_page(table_id,curPage,1);
    }
    
    return 0;
}

void roll_back(lock_t* lock_obj) {
    HeaderPage* hp = (HeaderPage*)buffer_read_page(lock_obj->sentinel->table_id,0);
	db_find_with_hp(lock_obj->sentinel->table_id,lock_obj->sentinel->record_id,lock_obj->original,(page_t*)hp,2,NULL);
	buffer_close_page(lock_obj->sentinel->table_id,0,0);
}

int db_update(int table_id, int64_t key, char* values, int trx_id) {
    HeaderPage* hp = (HeaderPage*)buffer_read_page(table_id,0);
    char* ret_val = (char*)malloc(sizeof(char)*120);
    if(db_find_with_hp(table_id,key,ret_val,(page_t*)hp,0,NULL) == -1) {
        buffer_close_page(table_id,0,0);
        trx_abort(trx_id,0);
        free(ret_val);
        return -1;
    }
    free(ret_val);
    buffer_close_page(table_id,0,0);
    lock_t* lock = lock_acquire(table_id,key,trx_id,1);
    if(lock == NULL) return -1;
    hp = (HeaderPage*)buffer_read_page(table_id,0);
	int ret = db_find_with_hp(table_id,key,values,(page_t*)hp,1,lock);
	buffer_close_page(table_id,0,0);
    return ret;
}

int db_find(int table_id,int64_t key, char* ret_val,int trx_id) {
    HeaderPage* hp = (HeaderPage*)buffer_read_page(table_id,0);
    if(db_find_with_hp(table_id,key,ret_val,(page_t*)hp,0,NULL) == -1) {
        buffer_close_page(table_id,0,0);
        trx_abort(trx_id,0);
        return -1;
    }
    buffer_close_page(table_id,0,0);
    if(lock_acquire(table_id,key,trx_id,0) == NULL) return -1;
	hp = (HeaderPage*)buffer_read_page(table_id,0);
	int ret = db_find_with_hp(table_id,key,ret_val,(page_t*)hp,0,NULL);
	buffer_close_page(table_id,0,0);
	return ret;
}

int db_find_with_hp(int table_id,int64_t key, char* ret_val,page_t* head, int mode,lock_t* lock){
    
    int left,right,mid;
    HeaderPage* hp;
    hp = (HeaderPage*)head;
    
    pagenum_t root = hp->rootPage;
    
    if(!root){
        return -1;
    }
    
    InternalPage* ip;
    ip = (InternalPage*)buffer_read_page(table_id,root);
    pagenum_t cur = root;
    
    while(!ip->isLeaf){
        left = 0;
        right = ip->numOfKeys-1;
        if(key < ip->records[left].key) {
            buffer_close_page(table_id,cur,0);
            cur = ip->leftMostPage;
            ip = (InternalPage*)buffer_read_page(table_id,cur); 
        }
        else if(key >= ip->records[right].key) {
            buffer_close_page(table_id,cur,0);
            cur = ip->records[right].pageNum;
            ip = (InternalPage*)buffer_read_page(table_id,cur);
        }
        else {
            while(right-left > 1) {
                mid = (left + right)/2;
                if(key >= ip->records[mid].key) left = mid;
                else right = mid;
            }
            buffer_close_page(table_id,cur,0);
            cur = ip->records[left].pageNum;
            ip = (InternalPage*)buffer_read_page(table_id,cur);
        }
    }
    LeafPage* lp = (LeafPage*)ip;
    left = 0;
    right = lp->numOfKeys-1;
    while(left <= right) {
        mid = (left + right)/2;
        if(key == lp->records[mid].key){
            //update
            if(mode == 1) {
                if(lock != 1) strcpy(lock->original,lp->records[mid].value);
                strcpy(lp->records[mid].value,ret_val);
                buffer_close_page(table_id,cur,1);
            }
            //roll_back 
            else if(mode == 2) {
                strcpy(lp->records[mid].value,ret_val);
                buffer_close_page(table_id,cur,1);
            } 
            //find
            else {
                strcpy(ret_val,lp->records[mid].value);
                buffer_close_page(table_id,cur,0);
            }
            return 0;
        } else if(key < lp->records[mid].key) right = mid-1;
        else left = mid+1;
    }
    buffer_close_page(table_id,cur,0);
    return -1;
}

//re
int64_t db_find_left_sibling(int table_id,int64_t key,int64_t curPageNum) {
    int left,right,mid,i;
    InternalPage* curPage;
    curPage = (InternalPage*)buffer_read_page(table_id,curPageNum);
    left = 0;
    right = curPage->numOfKeys - 1;
    if(key < curPage->records[left].key) mid = left-1;
    else if(key > curPage->records[right].key) mid = right;
    else {
        while(right-left > 1) {
            mid = (left + right)/2;
            if(key > curPage->records[mid].key) left = mid;
            else right = mid;
        }
        mid=left;
    }
    if(mid == -1) {
        buffer_close_page(table_id,curPageNum,0);
        if(curPage->parentPage == 0) return -1;
        return db_find_left_sibling(table_id,key,curPage->parentPage);
    } else {
        buffer_close_page(table_id,curPageNum,0);
        return db_find_leaf_page(table_id,curPage->records[mid].key - 1,curPageNum);
    }
}

//re
int64_t db_find_leaf_page(int table_id,int64_t key,int64_t curPageNum){
    int left,right,mid;
    pagenum_t curPage = curPageNum;
    
    InternalPage* ip;
    ip = (InternalPage*)buffer_read_page(table_id,curPageNum);
    
    while(!ip->isLeaf){
        left = 0;
        right = ip->numOfKeys-1;
        if(key < ip->records[left].key) {
            buffer_close_page(table_id,curPage,0);
            curPage = ip->leftMostPage;
            ip = (InternalPage*)buffer_read_page(table_id,curPage);
        }
        else if(key >= ip->records[right].key) {
            buffer_close_page(table_id,curPage,0);
            curPage = ip->records[right].pageNum;
            ip = (InternalPage*)buffer_read_page(table_id,curPage);
        }
        else {
            while(right-left > 1) {
                mid = (left + right)/2;
                if(key >= ip->records[mid].key) left = mid;
                else right = mid;
            }
            buffer_close_page(table_id,curPage,0);
            curPage = ip->records[left].pageNum;
            ip = (InternalPage*)buffer_read_page(table_id,curPage);
        }
    }
    buffer_close_page(table_id,curPage,0);
    return curPage;
}

void db_delete_parent(int table_id,int64_t key,int64_t curPageNum) {
    int left,right,mid,i;
    InternalPage* curPage;
    HeaderPage* hp;
    curPage = (InternalPage*)buffer_read_page(table_id,curPageNum);
    
    if(curPage->numOfKeys == 1) {
        if(curPage->parentPage == 0){
            hp = (HeaderPage*)buffer_read_page(table_id,0);
            hp->rootPage = key >= curPage->records[0].key ? curPage->leftMostPage : curPage->records[0].pageNum;
            file_free_page(table_id,(page_t*)curPage,curPageNum,(page_t*)hp);
            db_set_parent_from_child(table_id,hp->rootPage,0);
            buffer_close_page(table_id,curPageNum,1);
            buffer_close_page(table_id,0,1);
        }
        
        else {
            if(key < curPage->records[0].key) curPage->leftMostPage = curPage->records[0].pageNum;
            InternalPage* parentPage;
            parentPage = (InternalPage*)buffer_read_page(table_id,curPage->parentPage);
            left = 0;
            right = parentPage->numOfKeys - 1;
            if(key < parentPage->records[left].key) mid = left-1;
            else if(key > parentPage->records[right].key) mid = right;
            else {
                while(right-left > 1) {
                    mid = (left + right)/2;
                    if(key > parentPage->records[mid].key) left = mid;
                    else right = mid;
                }
                mid=left;
            }
            HeaderPage* hp = (HeaderPage*)buffer_read_page(table_id,0);
			KeyPageNum parentPageRecord;
			pagenum_t parentPageNum = curPage->parentPage;
			pagenum_t leftMostPage = curPage->leftMostPage;
            if(mid == -1){
				parentPageRecord.pageNum = parentPage->records[0].pageNum;
				parentPageRecord.key = parentPage->records[0].key;
	
                file_free_page(table_id,(page_t*)curPage,curPageNum,(page_t*)hp);
				buffer_close_page(table_id,0,1);
                buffer_close_page(table_id,parentPageNum,0);
                buffer_close_page(table_id,curPageNum,1);
                
                db_insert_parent(table_id,parentPageRecord.pageNum,parentPageRecord.key,leftMostPage,-1);
                db_delete_parent(table_id,key,parentPageNum);
            }
            else {
				parentPageRecord.pageNum = parentPage->records[mid].pageNum;
				parentPageRecord.key = parentPage->records[mid].key;
                pagenum_t neighborPageNum = mid ? parentPage->records[mid - 1].pageNum : parentPage->leftMostPage;
                file_free_page(table_id,(page_t*)curPage,curPageNum,(page_t*)hp);

				buffer_close_page(table_id,0,1);
                buffer_close_page(table_id,parentPageNum,0);
                buffer_close_page(table_id,curPageNum,1);

                db_insert_parent(table_id,neighborPageNum,parentPageRecord.key,leftMostPage,0);
                db_delete_parent(table_id,key,parentPageNum);
            }
        }
    }
    
    else {
        left = 0;
        right = curPage->numOfKeys-- - 1;
        if(key < curPage->records[left].key) mid = left-1;
        else if(key > curPage->records[right].key) mid = right;
        else {
            while(right-left > 1) {
                mid = (left + right)/2;
                if(key > curPage->records[mid].key) left = mid;
                else right = mid;
            }
            mid=left;
        }
        
        if(mid == -1) {
            curPage->leftMostPage = curPage->records[0].pageNum;
            mid++;
        }
        for(i = mid ; i < curPage->numOfKeys ; i++) curPage->records[i] = curPage->records[i+1];
        buffer_close_page(table_id,curPageNum,1);
    }
}

int db_delete(int table_id,int64_t key){
    HeaderPage* hp;
    pagenum_t curPage,root;
    int left,right,mid,i;
    char* ret_val = (char*)malloc(120);
    hp = (HeaderPage*)buffer_read_page(table_id,0);
    
    if(db_find_with_hp(table_id,key,ret_val,(page_t*)hp,0,NULL)==-1) {
        buffer_close_page(table_id,0,0);
        free(ret_val);
        return -1;
    }
    free(ret_val);

    root = hp->rootPage;
    curPage = root;
    InternalPage* ip;
    ip = (InternalPage*)buffer_read_page(table_id,root);
    
    while(!ip->isLeaf){
        left = 0;
        right = ip->numOfKeys-1;
        if(key < ip->records[left].key) {
            buffer_close_page(table_id,curPage,0);
            curPage = ip->leftMostPage;
            ip = (InternalPage*)buffer_read_page(table_id,curPage);
        }
        else if(key >= ip->records[right].key) {
            buffer_close_page(table_id,curPage,0);
            curPage = ip->records[right].pageNum;
            ip = (InternalPage*)buffer_read_page(table_id,curPage);
        }
        else {
            while(right-left > 1) {
                mid = (left + right)/2;
                if(key >= ip->records[mid].key) left = mid;
                else right = mid;
            }
            buffer_close_page(table_id,curPage,0);
            curPage = ip->records[left].pageNum;
            ip = (InternalPage*)buffer_read_page(table_id,curPage);
        }
    }
    
    LeafPage* lp = (LeafPage*)ip;
    left = 0;
    right = lp->numOfKeys-1;
    while(left <= right) {
        mid = (left + right)/2;
        if(key == lp->records[mid].key){
            break;
        } else if(key < lp->records[mid].key) right = mid-1;
        else left = mid+1;
    }
    
    if(lp->numOfKeys == 1){
        if(lp->parentPage == 0){
            file_free_page(table_id,(page_t*)lp,curPage,(page_t*)hp);
            hp->rootPage = 0;
            buffer_close_page(table_id,0,1);
            buffer_close_page(table_id,curPage,1);
        } else {
            pagenum_t leftSiblingPageNum,parentPageNum,rightSibling;
            parentPageNum = lp->parentPage;
            rightSibling = lp->rightSibling;
            file_free_page(table_id,(page_t*)lp,curPage,(page_t*)hp);
            buffer_close_page(table_id,0,1);
            buffer_close_page(table_id,curPage,1);
            
            leftSiblingPageNum = db_find_left_sibling(table_id,key,parentPageNum);
            if(leftSiblingPageNum != -1) {
                LeafPage* leftSiblingPage;
                leftSiblingPage = (LeafPage*)buffer_read_page(table_id,leftSiblingPageNum);
                leftSiblingPage->rightSibling = rightSibling;
                buffer_close_page(table_id,leftSiblingPageNum,1);
            }
            db_delete_parent(table_id,key,parentPageNum);
        }
    }
    
    else {
        buffer_close_page(table_id,0,0);
        for(i = mid ; i<lp->numOfKeys-1;i++) lp->records[i] = lp->records[i+1];
        lp->numOfKeys--;
        buffer_close_page(table_id,curPage,1);
    }
    return 0;
}
