#ifndef COMMON_H
#define COMMON_H

#include <cstdlib>
#include <iostream> //for cout in cpp


//#define DEBUG
const int TABLE_SIZE = 5;



enum OPN_NAME{
    INSERT = 5,
    DELETE = 6,
    LOOKUP = 7,
    WRONG_OPN = 8, //program shall not proceed
    DEFAULT_OPN_NAME = 111
};
enum STATUS{

    ABORT= 10,
    OK= 11,
    FAIL= 12,
    COMMIT= 13,
    RETRY = 14,
    BUCKET_EMPTY=100,
    VARIABLE_NULL = 101,
    WRONG_STATUS =102,  //program shall not proceed
    DEFAULT_OP_STATUS = 222
};
enum VALIDATION_TYPE{
    RV,
    TRYCOMMIT
};

enum LIST_TYPE{
RL_BL,
RL,
BL
};

#define BAD_INDEX INT_MAX
#define BAD_VALUE INT_MIN


//init values
#define DEFAULT_KEY INT_MIN
#define DEFAULT_VALUE INT_MIN
#define DEFAULT_TS 0
#define DEFAULT_MARKED 0

#define elog(_message_)  do {fprintf(stderr,			        \
				     "%s():%s:%u: %s\n",		\
				     __FUNCTION__, __FILE__, __LINE__,	\
				     _message_); fflush(stderr);}while(0);


/*
What you have declared with the following line:

Foo* fooPtrArray[4];
is an array of pointers to Foo objects (in other words an array of Foo*).

In C/C++, the name of the array is defined as a pointer to the start of the array. This is because arrays aren't "real" types in these languages but simply a contiguous sequence of values of a specific type in memory.

The name fooPtrArray in this case will be a "pointer to the first pointer" in the array, which is the start of the memory location where the array of pointers is stored.

So either function prototype you have outlined above should work. However, when passing in arrays to a function, you will always need to pass in the size as well so the function knows how many elements are in there. So you should define your method like this:

int getResult(Foo** fooPtrArray, int arraySize);
Inside this function, you can access the individual Foo pointers (and subsequently Foo objects) in the array like this:

for (int i=0; i < arraySize; i++)
{
    Foo* fooPtr = fooPtrArray[i];
    if (fooPtr)
    {
        fooPtr->memberFunction();
        fooPtr->memberVariable;
    }
}*/

//>>> 1transaction per thread


#endif //#define COMMON_H
