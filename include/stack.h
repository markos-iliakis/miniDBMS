#include <stdio.h>
#include <stdlib.h>
#include <limits.h>

// A structure to represent a stack
typedef struct Stack
{
    int top;
    unsigned capacity;
    int* array;
}Stack;

struct Stack* createStack(unsigned);

int isFull(Stack*);

int isEmpty(Stack*);

void push(Stack* , int);

int pop(Stack* );
