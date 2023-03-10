#ifndef _PAGE_H__
#define _PAGE_H__

#include <stdio.h>

#pragma pack(push,1)


typedef long long ll;

typedef struct KeyValue{
	ll key;
	char value[120];
}KeyValue;

typedef struct KeyPageNum{
	ll key;
	ll pageNum;
}KeyPageNum;

typedef struct HeaderPage{
	ll freePage;
	ll rootPage;
	ll numOfPages;
	char reserved[4072];
}HeaderPage;

typedef struct FreePage{
	ll nextFreePage;
	char notUsed[4088];
}FreePage;

typedef struct LeafPage{
	ll parentPage;
	int isLeaf;
	int numOfKeys;
	char reserved[104];
	ll rightSibling;
	KeyValue records[31];
}LeafPage;

typedef struct InternalPage{
	ll parentPage;
	int isLeaf;
	int numOfKeys;
	char reserved[104];
	ll leftMostPage;
	KeyPageNum records[248];
}InternalPage;

#pragma pack(pop)

#endif
