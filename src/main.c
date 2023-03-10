#include "file.h"
#include "concurrency_manager.h"
#include "lock_manager.h"
#include "trx_manager.h"
#include "buffer.h"
#include "dbbpt.h"
#include "bpt.h"
#include <time.h>
#include <pthread.h>

// MAIN

pthread_t threads[10000];

int abort_num;

void* thread_test(void* arg) {
    int trx_id = trx_begin();
    printf("trx_id : %d\n",trx_id);
    int k,input2;
    int table_id = *(int*)arg;
    char* findRetVal = (char*)malloc(120*sizeof(char));
    for(k=1;k<100;k++) {
        sprintf(findRetVal,"%d",k+1);
        int res = db_update(table_id,k,findRetVal,trx_id);
        printf("%d -> %s\n",res,findRetVal);
        if(res) {
            abort_num++;
            return NULL;
        }
        int r2 = rand()%100;
        res = db_find(table_id,r2,findRetVal,trx_id);
        printf("%d -> %s\n",res,findRetVal);
        if(res) {
            abort_num++;
            return NULL;
        }
    }
    trx_commit(trx_id);
}

int main( int argc, char ** argv ) {
    srand((unsigned int)time(NULL));

    char * input_file;
    FILE * fp;
    node * root;
    int input, range2;
    char instruction;
    char license_part;

    root = NULL;
    verbose_output = false;

    if (argc > 1) {
        order = atoi(argv[1]);
        if (order < MIN_ORDER || order > MAX_ORDER) {
            fprintf(stderr, "Invalid order: %d .\n\n", order);
            usage_3();
            exit(EXIT_FAILURE);
        }
    }

    license_notice();
    usage_1();  
    usage_2();

    if (argc > 2) {
        input_file = argv[2];
        fp = fopen(input_file, "r");
        if (fp == NULL) {
            perror("Failure  open input file.");
            exit(EXIT_FAILURE);
        }
        while (!feof(fp)) {
            fscanf(fp, "%d\n", &input);
            root = insert(root, input, input);
        }
        fclose(fp);
        print_tree(root);
    }

	char stringInput[120];
	char* findRetVal = (char*)malloc(120*sizeof(char));
	int k,input2,table_id;
    printf("> ");
    while (scanf("%c", &instruction) != EOF) {
        switch (instruction) {
        case 'a':
            //쓰레드 생성
            scanf("%d",&input);
            for (int i = 0; i < input; i++) {
                pthread_create(&threads[i], 0, thread_test, &table_id);
            }
            for (int i = 0; i < input; i++) {
                pthread_join(threads[i], NULL);
            }
            printf("abort_num : %d\n",abort_num);
            break;
		case 'b':
			scanf("%d",&input);
			init_db(input);
			break;
		case 'c':
			scanf("%d",&input);
            print_buffer(input);
            break;
        case 'd':
            scanf("%d", &input);
            printf("%d\n",db_delete(table_id,input));
            break;
    	case 'e':
    	    scanf("%d %d",&input,&input2);
            for(k=input;k<=input2;k++) printf("%d\n",db_delete(table_id,k));
            break;		
        case 'f':
        	memset(findRetVal,0,120);
        	scanf("%d", &input);
        	printf("%d -> %s\n",db_find(table_id,input,findRetVal,0),findRetVal);
        	break;
        case 'i':
        	scanf("%d %d",&input,&input2);
        	for(k=input;k<=input2;k++){
        	sprintf(findRetVal,"%d",k);
			db_insert(table_id,k,findRetVal);
			}
            break;
        case 'n':
        	print_LRU_list();
        	break;
        case 'o':
        	scanf("%s",stringInput);
        	for(k=0;k<strlen(stringInput);k++) printf("%c ",stringInput[k]);
        	table_id = open_table(stringInput);
        	printf("tid : %d\n",table_id);
        	break;
        case 'p':
            scanf("%d", &input);
            print_page(table_id,input);
            break;
        case 'q':
        	free(findRetVal);
            while (getchar() != (int)'\n');
            return EXIT_SUCCESS;
            break;
        case 'r':
            scanf("%d", &input);
            srand((unsigned int)time(NULL));
            for(k=0;k<input;k++) {
                input2 = rand()%100000000;
                sprintf(findRetVal,"%d",input2);
                db_insert(table_id,input2,findRetVal);
            }
            break;
        case 's':
        	scanf("%d",&input);
        	sprintf(findRetVal,"%d",input);
			db_insert(table_id,input,findRetVal);
			break;
        case 't':
            print_tree(root);
            break;
        case 'u':
        	scanf("%d %d",&input,&input2);
            sprintf(findRetVal,"%d",input2);
            db_update(table_id,input,findRetVal,0);
            break;
        case 'v':
        	scanf("%d",&input);
        	close_table(input);
        	break;
        case 'w':
        	shutdown_db();
        	break;
		case 'x':
			scanf("%d",&input);
            print_hash(input);
            break;
        case 'z':
            print_cur_status();
            break;
        default:
            usage_2();
            break;
        }
        while (getchar() != (int)'\n');
        printf("> ");
    }
    printf("\n");

    return EXIT_SUCCESS;
}
