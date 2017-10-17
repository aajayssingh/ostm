#ifndef COMMON_H
#define COMMON_H


#include <iostream> //for cout in cpp

#include <thread>
#include <mutex>              // std::mutex, std::unique_lock
#include <condition_variable> // std::condition_variable
#include <sys/time.h>
#include <math.h>
#include <sstream>
#include <assert.h>

typedef uint64_t uint_t;
//#define DEBUG_LOGS

const int TABLE_SIZE = 5;



enum OPN_NAME{
    INSERT = 5,
    DELETE = 6,
    LOOKUP = 7,
    //error
    WRONG_OPN = 8, //program shall not proceed
    DEFAULT_OPN_NAME = 111
};
enum STATUS{

    ABORT= 10,
    OK= 11,
    FAIL= 12,
    COMMIT= 13,
    RETRY = 14,
    //error
    BUCKET_EMPTY=100,
    VARIABLE_NULL = 101,
    WRONG_STATUS =102,  //program shall not proceed,
    DEFAULT_OP_STATUS = 222
};
enum VALIDATION_TYPE{
    RV,
    TRYCOMMIT
};


#define status(x) ((x == 10)? ("**ABORT**"):( (x == 11)?("OK"): ( (x ==12)?("FAIL"): ( (x == 13)?("COMMIT"): ( (x ==14)?("RETRY"):(  (x == 102)?("WRONG_STATUS"):( (x == 222)?("DEFAULT_OP_STATUS!!!"):("***SCREW") )) ) ) )))
#define opname(x) ((x == 5)?("INSERT"):( (x==6)?("DELETE"):( (x==7)?("LOOKUP"):(( x==8)?("WRONG_OPN**"):("DEFAULT_OPN_NAME")))))

enum LIST_TYPE{
RL_BL,
RL,
BL
};

#define BAD_INDEX INT_MAX
#define BAD_VALUE INT_MIN

#define MAX_KEY 100


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

// TODO (ajay#1#): Right Now code is only for single operation (ins look del) per transaction. ...
//add that support .
//For this we need PO validation.
//not impl POval bcse of crash.
//>>> 1transaction per thread
// TODO (ajay#1#): Code still hangs for 400 threads, because of delete abort case in trycommit, probably some unlocked lock is gettingreleased. I updated the predsnCurss earlier as solution. in trycommit preds and currs are fetched again , but the same were not updated inlocal log. And I am using local log to unlock the mutex.


#endif //#define COMMON_H
