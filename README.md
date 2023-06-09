# DBMS

-----------

### DBMS 설명  

#### File System
한 파일을 한 테이블로 설정  
페이지 단위로 DISK I/O가 일어남. (4096 Byte)  

캐싱을 위한 in-memory Buffer 존재  
LRU policy를 사용함  
lazy sync를 통한 buffer I/O  
  
#### File Struct  
meta data를 담고있는 file header  
record를 저장하는 file data  
  
| b+tree 구조  
insert, find, update, delete와 같은 모든 연산은 O(lon n) 시간 복잡도를 가짐  
모든 record는 leaf 노드에 존재  
AVL 트리의 성격을 갖고 있어 insert, delete, update시 tree의 depth를 조정  
  
#### Concurrency Control  
멀티 쓰레드를 지원  
transaction, mutex lock을 통한 동시성 제어  
  
------------

### 실행 순서
  
> ./main  
프로그램 실행  
  
> b (int)  
buffer를 int 크기만큼 생성  
buffer의 크기는 buffer가 저장하는 page의 수를 의미함  
  
> o (string)  
string에 이미 존재하는 파일 또는 이미 존재하는 파일을 입력하여 생성된 파일을 테이블 하나로 봄  
여러 테이블을 이용하는 경우에도 이 명령어를 사용해 테이블을 옮겨다닐 수 있음  


이후 원하는 명령어 입력


------------------

### 사용 설명
  
> s (int)  
(int)를 테이블에 넣음  
  
> i (int1) (int2)  
(int1) ~ (int2) 까지의 수를 테이블에 넣음  
  
> r (int)  
0~99999999 까지의 랜덤한 수를 (int)개만큼 테이블에 넣음  
  
> d (int)  
(int)를 테이블에서 삭제함  
  
> e (int1) (int2)  
(int1) ~ (int2) 까지의 수를 테이블에서 삭제함 

> u (int1) (int2)
(int1) key의 value를 (int2)로 변경함
  
> n  
buffer의 LRU list를 buffer index를 통해 순회하며 출력함  
  
> c (int)  
LRU list에서 (int)까지의 buffer 정보를 출력함  
  
> f (int)  
(int) record가 테이블 내에 존재하면 0 그렇지 않으면 -1을 출력함  
  
> p (int)  
(int) 페이지의 정보를 출력함  
  
> a (int)  
(int)개의 쓰레드를 생성하여 thread_test 함수를 통해 update, insert를 진행함  
  
> q  
프로그램 종료  
