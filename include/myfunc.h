#include <stdio.h>
#include "bf.h"
#include <string.h>
#include <unistd.h>
#include "stack.h"
#include <math.h>
/* Error codes */

#define PATH_SIZE 255
#define STACK_SIZE 1000

#define AME_OK 0
#define AME_EOF -1

#define EQUAL 1
#define NOT_EQUAL 2
#define LESS_THAN 3
#define GREATER_THAN 4
#define LESS_THAN_OR_EQUAL 5
#define GREATER_THAN_OR_EQUAL 6

#define CALL_OR_DIE(call){      \
    BF_ErrorCode code = call;   \
    if (code != BF_OK) {        \
        BF_PrintError(code);    \
        AM_errno = -5;           \
    }                           \
}                               \

typedef struct indexes{
  int fileDesc;
  char attrType1;
  int attrLength1;
  char attrType2;
  int attrLength2;
}indexes;

typedef struct scans{
    int fileDesc;
    int block_num;
    int record_in_block;
    int op;
    int entries_counter;
    void* value;
    void* returning_value;
}scans;

void get_metadata(char**, BF_Block**, int*, char*, int*, int*);

void store_metadata(char **, BF_Block** , int, char, int, int *);

int insert_node(char** , void* , void* , int* , int , BF_Block** , int, int, int, Stack*);

int go_down_tree(char**, void*, char , int*, int, BF_Block**, int*, int*, Stack*);

int sort_tree(BF_Block **, BF_Block **, Stack*, int , int);

int set_first_block(int , void *, void *);

int insert_index(char** , void* , int , BF_Block** , int , Stack*);

int print(int);
