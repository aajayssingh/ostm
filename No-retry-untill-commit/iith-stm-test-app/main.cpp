/*
*DESCRIPTION :  v2 = here the linked list structure has been changed to accomodate value field as well //{ID|v,nextID}
                The code implements the Linkedlist Datastructure for multithreaded environment, using IITH STM library
*                this implementation will be used to test the performance of IITH STM against the syncrobench.
*                The operations implemented are insert(), delete(), contain()
*                 Insert() : inserts into the linklist
*                Delete() : deletes from the linklist
*                Contain() : finds the given node in a linked list.
*                 printlist(): prints the list.
*AUTHOR      :  AJAY SINGH
*COMPANY     :  IIT Hyderabad
*/
#include <iostream>
#include <pthread.h>
#include <unistd.h> // for sleep function
#include<climits>
#include <math.h>
#include <fstream>

#include <sys/time.h>
#include <time.h>

#define AJBTO
#define DEBUG 0

#ifdef AJBTO
#include "IITHSTMCode/BTO.cpp"
#endif
#ifdef AJSGT
#include "IITHSTMCode/SGT.cpp"
#endif
#ifdef AJMVTO
#include "IITHSTMCode/MVTO.cpp"
#endif // AJMVTO

#ifdef AJFOCC
#include "IITHSTMCode/FOCC.cpp"
#endif // AJFOCC



#include <mutex>
#include <sstream>
#include <assert.h>

using namespace std;

/**********************************************************************************************************************************************/
//  FUNCTION DECLARATION
/**********************************************************************************************************************************************/

void* test(void *threadData);
void print_oblist();
//void* test_add(void *threadData);
//void* test_remove(void *threadData);

/**********************************************************************************************************************************************/
//  MACROS DECLARATION
/**********************************************************************************************************************************************/
#define VAL_MIN  INT_MIN
#define VAL_MAX  INT_MAX
#define TAB_SIZE 5
#define MAX_KEY  1000//INT_MAX-(TAB_SIZE -2)
//#define DEBUG 1

/**********************************************************************************************************************************************/
//  GLOBAL DECLARATION
/**********************************************************************************************************************************************/

//node is of form
//{ID|v,nextID}

typedef struct{
int v;              //actual value field, as per it is just the ID only
int nextID;         //ID -given to node while creation
}val_t;

fstream file10runGTOD, file10runCPU, file10runTH, file10runRT, file10runCLOCK, filenumaborts;

#ifdef AJBTO
STM* lib = new BTO();
#endif // AJBTO
#ifdef AJSGT
STM* lib = new SGT(1000);
#endif // AJSGT
#ifdef AJMVTO
STM* lib = new MVTO(1000);
#endif // AJMVTO

#ifdef AJFOCC
STM* lib = new FOCC();
#endif // AJFOCC

#ifdef AJKSTM
STM* lib = new MVFOCC(30);
#endif // AJKSTM
static int i = 0;
mutex mtx, mtxc;
int num_del_aborts = 0, num_ins_aborts = 0, num_lookup_aborts = 0;

// NOTE (ajay#1#): For fair comparison contention should be same. ...
//make sure the key ranges are same.


//Barrier code
typedef struct barrier {
	pthread_cond_t complete;
	pthread_mutex_t mutex;
	int count;
	int crossing;
} barrier_t;

void barrier_init(barrier_t *b, int n)
{
	pthread_cond_init(&b->complete, NULL);
	pthread_mutex_init(&b->mutex, NULL);
	b->count = n;
	cout<<"init barrier "<<n<<endl;
	b->crossing = 0;
}

void barrier_cross(barrier_t *b)
{
	pthread_mutex_lock(&b->mutex);
	/* One more thread through */
	b->crossing++;
	printf("aj crossings %d\n",b->crossing);
	/* If not all here, wait */
	if (b->crossing < b->count) {
		pthread_cond_wait(&b->complete, &b->mutex);
	} else {
		pthread_cond_broadcast(&b->complete);
		/* Reset for next time */
		b->crossing = 0;
	}
	pthread_mutex_unlock(&b->mutex);
}


struct thread_info{
   int  thread_id;
   int thread_val;//value to be inserted for insert operation
   int thread_op;//insert = 0,   delete = 1, lookup = 2
   barrier_t *barrier;
};

int number_of_threads =200;
pthread_t *threads ;

//number of delete, insert and lookup threads
int delNum = 0, insertNum = 0, lookupNum = 0, moveNum = 0;

//default percentage of delete , insert and lookup threads
float delp = 15, insertp = 45, lookp = 30, movep = 10;;
int num_op_per_tx = 4;


//AJ
struct timespec startajcpu, startajth, startajrt, finishajcpu, finishajth, finishajrt;
double elapsedajrt;
double elapsedajcpu;
double elapsedajth, duration;
//clock_t timeaj;
struct timeval start, endaj;
//AJ

/*Each bucket of hash tab has min and max id so for hash tab of size 5 id of min+5 and MAx-5 are reserved.*/
int init_tab()
{
stringstream msg;
//for bucket 0 to size
for(int i = 0; i < TAB_SIZE; i++)
{
    msg.clear();

    int val_min = VAL_MIN + i;
    int val_max = VAL_MAX - i;
//creating to dummy min and max node of linked list
    if(lib->create_new(val_min, sizeof(val_t)) != SUCCESS)
    {
        cout<<"failed to create lid"<<endl;
        return 0;
    }
    if(lib->create_new(val_max, sizeof(val_t)) != SUCCESS)
    {
        cout<<"failed to create lid"<<endl;
        return 0;
    }

//inserting above created sentinel id into linked list
{
    long long error_id;
	int read_res=0;
	int write_res=0;
    trans_state* T1 = lib->begin();

    common_tOB* set_obj_min = new common_tOB;
    set_obj_min->size = sizeof(val_t);
    set_obj_min->ID = val_min;
    read_res = lib->read(T1, set_obj_min);

    common_tOB* set_obj_max = new common_tOB;
    set_obj_max->size = sizeof(val_t);
    set_obj_max->ID = val_max;
    read_res = lib->read(T1, set_obj_max);

    (*(val_t*)set_obj_min->value).v = val_min;// assuming v is same as IDs
    (*(val_t*)set_obj_min->value).nextID = val_max; //= VAL_MAX;//value stores the next field ID , so head node has next fields ID


    (*(val_t*)set_obj_max->value).v = val_max;
    (*(val_t*)set_obj_max->value).nextID = val_max;

    write_res=lib->write(T1, set_obj_min);
    write_res=lib->write(T1, set_obj_max);

    if(write_res == SUCCESS)
    {
        lib->try_commit(T1, error_id);
    }
    else
    {
        msg<<"[Dont proceeed] init commit failed\n";
        cout<<msg.str();
        assert(0);
    }
}
}

return 1;

}

void populate()
{

	int result, trans_succ_flag;
	long long error_id;
	int read_res=0;
	int write_res=0;
    clock_t time_beg = clock();
//    thread_info *d = (thread_info *)threadData;

    //only to insert i need to create new tob ids

	trans_state* T1 = lib->begin();

	int k = 1;
while(k--){

    //find bucket
    int thread_val = rand()%(MAX_KEY - 1) + 1; //this is the val that thread will work upon
    int bucket_id = thread_val%TAB_SIZE;
    int val_min = VAL_MIN + bucket_id;
    int val_max = VAL_MAX - bucket_id;


	if(lib->create_new(thread_val, sizeof(val_t)) != SUCCESS)
    {
        cout<<"failed to create lid : "<<thread_val<<endl;
        //assert(0);
        return; //if already existing the obj id dont overwrite.
    }

    //tob to be added
    common_tOB* set_obj = new common_tOB;
    set_obj->size = sizeof(val_t);
    set_obj->ID = thread_val;   //each thread will carry the value it wants to insert plus the unique ID of node here in this case both are same
    read_res = lib->read(T1, set_obj);//mem allocated to set_obj->value
    if(0 != read_res)
    {
        cout<<"+:read fail"<<endl;
        cout<<"+:killing : main"<<endl;
        mtxc.lock();
        num_ins_aborts++;
        mtxc.unlock();
        //read has failed further execution of thread may cause seg fault
        pthread_exit(NULL);
    }

    //since v and id are same so assignin same val to both id and next by default
    (*(val_t*)set_obj->value).v = 0; //default init value of new node
    (*(val_t*)set_obj->value).nextID = val_max;


//preparing prev and next pointers for list traversal
    common_tOB* set_obj_p = new common_tOB; //prev tob
    set_obj_p->size = sizeof(val_t);
    set_obj_p->value = operator new (sizeof(val_t));
    set_obj_p->ID = val_min;
    read_res = lib->read(T1, set_obj_p);
    if(0 != read_res)
    {
        cout<<"+p:read fail"<<endl;
        cout<<"+p:killing : main"<<endl;
        mtxc.lock();
        num_ins_aborts++;
        mtxc.unlock();
//        read has failed further execution of thread may cause seg fault
        pthread_exit(NULL);
    }

    common_tOB* set_obj_n = new common_tOB; //next tob
    set_obj_n->size = sizeof(val_t);
    set_obj_n->value = operator new (sizeof(val_t));
    set_obj_n->ID = (*(val_t*)set_obj_p->value).nextID;
    read_res = lib->read(T1, set_obj_n);

    if(0 != read_res)
    {
        cout<<"+n:read fail:"<<endl;
        cout<<"+n:killing : main"<<endl;

  //      read has failed further execution of thread may cause seg fault
        pthread_exit(NULL);
    }

	//linked list is in sorted order of val
	while ((set_obj_n->ID) < thread_val)
	{
	   set_obj_p->ID = set_obj_n->ID;
        (*(val_t*)set_obj_p->value).v = (*(val_t*)set_obj_n->value).v; // instead read through the lib API
        (*(val_t*)set_obj_p->value).nextID = (*(val_t*)set_obj_n->value).nextID;

        set_obj_n->ID = (*(val_t*)set_obj_p->value).nextID;// prev is equal to next now, (*(val_t*)set_obj_n->value).nextID is equally good.

        read_res = lib->read(T1, set_obj_n);
        if(0 != read_res)
        {
            //  mtx.lock();
            cout<<"+in while read fail :"<<set_obj_n->ID<<" "<<read_res<<endl;
            //    print_oblist();
            cout<<"+killing : main"<<endl;
            //mtx.unlock();
            //read failed no point in continuing transac , do releaselock
            pthread_exit(NULL);//or simply break ;
        }
	}

	if(0 == read_res)
	{
        result = ((set_obj_n->ID) != thread_val);

        if (result)//id node not already present insert node
        {

           (*(val_t*)set_obj_p->value).nextID = set_obj->ID;

           (*(val_t*)set_obj->value).nextID = set_obj_n->ID;

            write_res=lib->write(T1, set_obj);
            if(0 != write_res)
            {
                printf("+write fail");
            }
            write_res=lib->write(T1, set_obj_p);
            if(0 != write_res)
            {
                printf("+write fail");
            }
            write_res=lib->write(T1, set_obj_n);
            if(0 != write_res)
            {
                printf("+write fail");
            }

        }
	}
}

	if(0 == read_res)
	{
        trans_succ_flag = lib->try_commit(T1, error_id);
        if(0 != trans_succ_flag)
        {
            cout<<"+commit failed insert: main"<<endl;

        }
        else
        {
            cout<<"+commit insert: main"<<endl;
        }

        //release memory of temporary objects created after transactions commit

//        delete(set_obj);
//        delete(set_obj_p);
//        delete(set_obj_n);
    }//if(0 == read_res)

}


int main()
{


    struct thread_info *td; //to store each thread information
    int rc;
    int i;
    void *status;
    int val = 0;

    barrier_t barrier;


    /*ALLOCATING MEMORY TO THREADS FOR BOOKEEPING*/
    threads = new pthread_t [number_of_threads];//pthread_t threads[NUMTHREADS];
    td = new struct thread_info [number_of_threads];//struct thread_info td[NUMTHREADS];

    if((threads == nullptr)||(td == nullptr))
    {
        cout<<"error allocating memory";
    }

// openfile to store completion time for plotting performance graphs
    filenumaborts.open("numaborts.txt", fstream::out | fstream::app);
    file10runGTOD.open("runGTOD.txt", fstream::out | fstream::app);
   // file10runCPU.open("runCPU.txt", fstream::out | fstream::app);
   // file10runTH.open("runTH.txt", fstream::out | fstream::app);
    //file10runRT.open("runRT.txt", fstream::out | fstream::app);
   // file10runCLOCK.open("runCLOCK.txt", fstream::out | fstream::app);

    init_tab();

    for(int i = 0; i < 25; i++)
        populate();

    print_oblist();
//Now linked list is only two node and this is the initial state of linked list


//Asking user choice of percentage of insert, del, lookup threads  in TODO list

//cout<<"enter % of threads for Addition - "<<endl;
//cin>>insertp;
//cout<<"enter % of threads for deletion - "<<endl;
//cin>>delp;
//cout<<"enter % of threads for lookup   - "<<endl;
//cin>>lookp;

insertNum = (int)ceil((insertp*number_of_threads)/100);
delNum = (int)ceil((delp*number_of_threads)/100);
lookupNum = (int)ceil((lookp*number_of_threads)/100);
moveNum = (int)ceil((movep*number_of_threads)/100);

//cout<<"#threads"<<insertNum<<" "<<delNum<<" "<<lookupNum<<endl;
//exception verification
if((insertNum + delNum + lookupNum + moveNum) > number_of_threads)
   {
   cout<<"((insertNum + delNum + lookupNum + moveNum) > number_of_threads)"<<endl;
    cout<<insertNum<<" "<<delNum<<" "<<lookupNum<<endl;
    return 0;

}


barrier_init(&barrier, number_of_threads +1 );





    /*INITIATING THREADINFO */
   for( i=0; i < number_of_threads; i++ ){
      cout <<"main() : creating thread, " << i << endl;
      td[i].thread_id = i;
      td[i].barrier = &barrier;

      if(i < insertNum)//init threaddata for insert threads
      {
        td[i].thread_op = 0;
        td[i].thread_val = rand()%(TAB_SIZE);//dummy random val is generated before inserting in add()
//        val = td[i].thread_val;
//
//        //only to insert i need to create new tob ids
//        if(lib->create_new(val, sizeof(val_t)) != SUCCESS)
//        {
//            cout<<"failed to create lid"<<endl;
//            return 0;
//        }

      }
      else if(i < (insertNum + delNum ))//init threadData for delete threads
      {
        td[i].thread_op = 1;
        td[i].thread_val = rand()%(TAB_SIZE); //dummy regenerated during opn
      }
      else if(i < (insertNum + delNum + lookupNum))//init for lookup threads
      {
        td[i].thread_op = 2;
        td[i].thread_val = rand()%(TAB_SIZE);//dummy regenerated during opn
      }
      else if(i < (insertNum + delNum + lookupNum + moveNum))//init for lookup threads
      {
        td[i].thread_op = 3;
        td[i].thread_val = rand()%(TAB_SIZE);//dummy regenerated during opn
      }
      else
      {
        cout<<"something wrong in thread distribution to operations"<<endl;
      }

//creating threads for insert del and lookup operations
    {
      rc = pthread_create(&threads[i], NULL,
                          test, (void *)&td[i]);
      if (rc){
         cout << "Error:unable to create thread," << rc << endl;
         exit(-1);
      }
    }
   }//FOR


cout<<"STARTING..."<<endl;
gettimeofday(&start, NULL);
   	/* main thread awakes all threads */
	barrier_cross(&barrier);


clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &startajcpu);
clock_gettime(CLOCK_THREAD_CPUTIME_ID, &startajth);
clock_gettime(CLOCK_MONOTONIC_RAW, &startajrt);
double timeaj;
timeaj = clock();


/*WAIT FOR ALL THREADS TO FINISH*/
for( i=0; i < number_of_threads; i++ ){
      rc = pthread_join(threads[i], &status);
      //cout<<"joinin";
      if (rc){
         cout << "Error:unable to join," << rc << endl;
         exit(-1);
      }
   }


//log completion time
gettimeofday(&endaj, NULL);
//CLOCK_MONOTONIC_RAW
clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &finishajcpu);
clock_gettime(CLOCK_THREAD_CPUTIME_ID, &finishajth);
clock_gettime(CLOCK_MONOTONIC_RAW, &finishajrt);
timeaj = clock() - timeaj;

timeaj = clock() - timeaj;
timeaj = ((double)timeaj)/(CLOCKS_PER_SEC);


cout<<"STOPPING..."<<endl;


duration = (endaj.tv_sec - start.tv_sec);
duration += ( endaj.tv_usec - start.tv_usec)/ 1000000.0;

//	elapsedaj = (finishaj.tv_sec - startaj.tv_sec);
//elapsedaj += (finishaj.tv_nsec - startaj.tv_nsec) / 1000000000.0;

elapsedajcpu = (finishajcpu.tv_sec - startajcpu.tv_sec);
elapsedajcpu += (finishajcpu.tv_nsec - startajcpu.tv_nsec) / 1000000000.0;

elapsedajth = (finishajth.tv_sec - startajth.tv_sec);
elapsedajth += (finishajth.tv_nsec - startajth.tv_nsec) / 1000000000.0;
elapsedajrt = (finishajrt.tv_sec - startajrt.tv_sec);
elapsedajrt += (finishajrt.tv_nsec - startajrt.tv_nsec) / 1000000000.0;




//cout<<"clock() in (s)"<<timeaj<<endl;
//cout<<"gettimeofday() in (s) operation per sec"<<duration/3<<endl;
cout<<"#ins aborts"<<num_ins_aborts<<endl;
cout<<"#del aborts"<<num_del_aborts<<endl;
cout<<"#lookup aborts"<<num_lookup_aborts<<endl;
cout<<"gtod duration in (s)  "<<duration<<endl;
//cout<<"elapsedajcpu in (s)  "<<elapsedajcpu<<endl;
//cout<<"elapsedajth in (s)   "<<elapsedajth<<endl;
//cout<<"elapsedajrt in (s)   "<<elapsedajrt<<endl;
//printf("avg clock timeaj in(s) %lf ", timeaj/number_of_threads); //to fix precision problem
//filenumaborts<<(num_del_aborts + num_ins_aborts + num_lookup_aborts)<<endl;
//
//file10runGTOD<<duration/number_of_threads<<endl; // divided by 3 bc there are 3 opn executed by the tx
//file10runCPU<<elapsedajcpu/3<<endl;
//file10runTH<<elapsedajth/3<<endl;
//file10runRT<<elapsedajrt/3<<endl;
//file10runCLOCK<<timeaj/number_of_threads<<endl;

#ifdef DEBUG
    //to safely print the linked list
    mtx.lock();
    print_oblist();
    //cout<<i;
    mtx.unlock();
#endif // DEBUG
  //  cout << "Done!!!" << endl;
    //releasing memory
    delete[] threads;
    delete[] td;

    return 0;
}//main()


/*
*DESCRIPTION    :   Prints the linked list
*AUTHOR         :   AJAY SINGH
*COMPANY        :   IIT Hyderabad
*/
void print_oblist()
{
    int count =0;
    int read_res=0;
    stringstream msg;

    trans_state* T = lib->begin();

    cout <<"\nprint_oblist:: printing..."<<endl;
    int val_min ;
    int val_max ;

   for(int i = 0; i < TAB_SIZE; i++)
   {
        val_min = VAL_MIN + i;
        val_max = VAL_MAX - i;
        count =0;

        //get to head of linked list
        common_tOB* set_obj = new common_tOB;
        set_obj->size = sizeof(val_t);
        set_obj->ID = val_min;//temp
        lib->read(T, set_obj);
        if(0 != read_res)
        {
            //cout<<"+po:read fail"<<endl;
            //cout<<"+po:killing : "<<endl;
            //read has failed further execution of thread may cause seg fault
            //pthread_exit(NULL);
            assert(0);
        }
    //    cout << "print_oblist::read\n";

        //untill not reached the linked list end s entinel node keep iterating through the list
        do{
            count++;
            cout<<(set_obj->ID)<<" --> ";//<<(*(val_t*)set_obj->value).nextID<<" ";
            set_obj->ID = (*(val_t*)set_obj->value).nextID;
            read_res = lib->read(T, set_obj);
            if(0 != read_res)
            {
                //cout<<"+pol:read fail"<<endl;//pol = print in loop
                //cout<<"+pol:killing : "<<endl;
                //read has failed further execution of thread may cause seg fault
                //pthread_exit(NULL);
                assert(0);
            }

//            if(count==100)
//            {
//                cout<<"\nval_max : set_obj->ID "<<val_max<<" : "<<set_obj->ID<<endl;
//                break;
//            }
        }while(val_max > set_obj->ID/*(*(val_t*)set_obj->value).v*/);
        cout<<(set_obj->ID)<<"\n";
       //cout<<"count - "<<count<<endl;
        delete(set_obj);
    }
}

/*
*DESCRIPTION    :   Add a random key val to the hash table
*AUTHOR         :   AJAY SINGH
*COMPANY        :   IIT Hyderabad
*/
void add(thread_info *d)
{

	int result, trans_succ_flag;
	long long error_id;
	int read_res=0;
	int write_res=0;
    clock_t time_beg = clock();
    stringstream msg;
//    thread_info *d = (thread_info *)threadData;

    //only to insert i need to create new tob ids

	trans_state* T1 = lib->begin();

	int k = num_op_per_tx;
while(k--){

           //find bucket
    d->thread_val = rand()%(MAX_KEY - 1) + 1; //this is the val that thread will work upon
    int bucket_id = d->thread_val%TAB_SIZE;
    int val_min = VAL_MIN + bucket_id;
    int val_max = VAL_MAX - bucket_id;

	if(lib->create_new(d->thread_val, sizeof(val_t)) != SUCCESS)
    {
        msg<<"failed to create lid : "<<d->thread_val<<endl;//overwrite the key present
        cout<<msg.str();
        msg.clear();
        //assert(0);
        //continue;
    }

    msg<<"insert:: th id:: key --->"<<d->thread_id<<":"<<d->thread_val<<endl;
    cout<<msg.str();
    msg.clear();

    //tob to be added
    common_tOB* set_obj = new common_tOB;
    set_obj->size = sizeof(val_t);
    set_obj->value = operator new (sizeof(val_t));
    set_obj->ID = d->thread_val;   //each thread will carry the value it wants to insert plus the unique ID of node here in this case both are same
    read_res = lib->read(T1, set_obj);//mem allocated to set_obj->value
    if(0 != read_res)
    {
        msg<<"+:read fail"<<endl;
        msg<<"+:killing : "<<read_res<<":"<<d->thread_id<<endl;
        cout<<msg.str();
        msg.clear();

        mtxc.lock();
        num_ins_aborts++;
        mtxc.unlock();
        //read has failed further execution of thread may cause seg fault
        return;//pthread_exit(NULL);
    }

    //since v and id are same so assignin same val to both id and next by default
    (*(val_t*)set_obj->value).v = 0; //default init value of new node
    (*(val_t*)set_obj->value).nextID = val_max;


//preparing prev and next pointers for list traversal
    common_tOB* set_obj_p = new common_tOB; //prev tob
    set_obj_p->size = sizeof(val_t);
    set_obj_p->value = operator new (sizeof(val_t));
    set_obj_p->ID = val_min;
    read_res = lib->read(T1, set_obj_p);
    if(0 != read_res)
    {
        msg<<"+p:read fail"<<endl;
        msg<<"+p:killing : "<<read_res<<":"<<d->thread_id<<endl;
        cout<<msg.str();
        msg.clear();

        mtxc.lock();
        num_ins_aborts++;
        mtxc.unlock();
//        read has failed further execution of thread may cause seg fault
        return;//pthread_exit(NULL);
    }

    common_tOB* set_obj_n = new common_tOB; //next tob
    set_obj_n->size = sizeof(val_t);
    set_obj_n->value = operator new (sizeof(val_t));
    set_obj_n->ID = (*(val_t*)set_obj_p->value).nextID;
    read_res = lib->read(T1, set_obj_n);

    if(0 != read_res)
    {
        msg<<"+n:read fail:"<<endl;
        msg<<"+n:killing : "<<read_res<<":"<<d->thread_id<<endl;
        cout<<msg.str();
        msg.clear();

        mtxc.lock();
        num_ins_aborts++;
        mtxc.unlock();
  //      read has failed further execution of thread may cause seg fault
        return;//pthread_exit(NULL);
    }

	//linked list is in sorted order of val
	while ((set_obj_n->ID) < d->thread_val)
	{
	   set_obj_p->ID = set_obj_n->ID;
        (*(val_t*)set_obj_p->value).v = (*(val_t*)set_obj_n->value).v; // instead read through the lib API
        (*(val_t*)set_obj_p->value).nextID = (*(val_t*)set_obj_n->value).nextID;

        set_obj_n->ID = (*(val_t*)set_obj_p->value).nextID;// prev is equal to next now, (*(val_t*)set_obj_n->value).nextID is equally good.

        read_res = lib->read(T1, set_obj_n);
        if(0 != read_res)
        {
            //  mtx.lock();
            msg<<"+in while read fail :"<<set_obj_n->ID<<" "<<read_res<<endl;
            //    print_oblist();
            msg<<"+killing : "<<d->thread_id<<endl;
            cout<<msg.str();
            msg.clear();
            //mtx.unlock();
            mtxc.lock();
            num_ins_aborts++;
            mtxc.unlock();
            //read failed no point in continuing transac , do releaselock
            pthread_exit(NULL);//or simply break ;
        }
	}

	if(0 == read_res)
	{
        result = ((set_obj_n->ID) != d->thread_val);

        if (result)//id node not already present insert node
        {

           (*(val_t*)set_obj_p->value).nextID = set_obj->ID;

           (*(val_t*)set_obj->value).nextID = set_obj_n->ID;

            write_res=lib->write(T1, set_obj);
            if(0 != write_res)
            {
                msg<<"+write fail\n";
                cout<<msg.str();
                msg.clear();
            }
            write_res=lib->write(T1, set_obj_p);
            if(0 != write_res)
            {
                msg<<"+write fail\n";
                cout<<msg.str();
                msg.clear();

            }
            write_res=lib->write(T1, set_obj_n);
            if(0 != write_res)
            {
                msg<<"+write fail\n";
                cout<<msg.str();
                msg.clear();
            }

        }
	}
}

	if(0 == read_res)
	{
        trans_succ_flag = lib->try_commit(T1, error_id);
        if(0 != trans_succ_flag)
        {
            msg<<"+commit failed insert: "<<d->thread_id<<endl;
            cout<<msg.str();
            msg.clear();

            mtxc.lock();
            num_ins_aborts++;
            mtxc.unlock();
        }
        else
        {
            msg<<"+commit insert: "<<d->thread_id<<endl;
            cout<<msg.str();
            msg.clear();

        }

        //release memory of temporary objects created after transactions commit

//        delete(set_obj);
//        delete(set_obj_p);
//        delete(set_obj_n);
    }//if(0 == read_res)

}

/*
*DESCRIPTION    :   lookup a random key from hash table
*AUTHOR         :   AJAY SINGH
*COMPANY        :   IIT Hyderabad
*/
void lookup(thread_info *d)
{
    int result, trans_succ_flag;;
	long long error_id;
	int read_res=0;
	int write_res=0;
	stringstream msg;


    int v;

    trans_state* T = lib->begin();

int k = num_op_per_tx;
while(k--){

   //find bucket
    d->thread_val = rand()%(MAX_KEY - 1) + 1; //this is the val that thread will work upon
    int bucket_id = d->thread_val%TAB_SIZE;
    int val_min = VAL_MIN + bucket_id;
    int val_max = VAL_MAX - bucket_id;

    common_tOB* set_obj_p = new common_tOB; //prev tob
    set_obj_p->size = sizeof(val_t);
    set_obj_p->value = operator new (sizeof(val_t));
    set_obj_p->ID = val_min;
    read_res = lib->read(T, set_obj_p);
    if(0 != read_res)
    {
        msg<<"*p:read fail"<<endl;
        msg<<"*p:killing : "<<d->thread_id<<endl;
        cout<<msg.str();
        msg.clear();

        mtxc.lock();
        num_lookup_aborts++;
        mtxc.unlock();

//        read has failed further execution of thread may cause seg fault
        return;//pthread_exit(NULL);
    }

    common_tOB* set_obj_n = new common_tOB; //next tob
    set_obj_n->size = sizeof(val_t);
    set_obj_n->value = operator new (sizeof(val_t));
    set_obj_n->ID = (*(val_t*)set_obj_p->value).nextID;
    read_res = lib->read(T, set_obj_n);
    if(0 != read_res)
    {
        msg<<"*n:read fail"<<endl;
        msg<<"*n:killing : "<<d->thread_id<<endl;
        cout<<msg.str();
        msg.clear();

        mtxc.lock();
        num_lookup_aborts++;
        mtxc.unlock();

        //read has failed further execution of thread may cause seg fault
        return;//pthread_exit(NULL);
    }

    while(1)
    {
        v = (*(val_t*)set_obj_p->value).nextID;
        if (v >= d->thread_val)// as per now unique id of node and val of node are same so using threadVal casually
            break;//node is found or cannot be found anymore

        set_obj_p->ID = set_obj_n->ID;
        (*(val_t*)set_obj_p->value).v = (*(val_t*)set_obj_n->value).v; // instead read through the lib API
        (*(val_t*)set_obj_p->value).nextID = (*(val_t*)set_obj_n->value).nextID;

        set_obj_n->ID = (*(val_t*)set_obj_p->value).nextID;

        read_res = lib->read(T, set_obj_n);
        if(0 != read_res)
        {
            msg<<"*p:in while read fail"<<endl;
            msg<<"*p:killing : "<<d->thread_id<<endl;
            cout<<msg.str();
            msg.clear();

            mtxc.lock();
            num_lookup_aborts++;
            mtxc.unlock();

            //read has failed further execution of thread may cause seg fault
            return;//pthread_exit(NULL);
        }

    }

}

    trans_succ_flag = lib->try_commit(T, error_id);
    if(0 != trans_succ_flag)
    {
        msg<<"*commit failed lookup "<<d->thread_id<<endl;
        cout<<msg.str();
        msg.clear();

        mtxc.lock();
        num_lookup_aborts++;
        mtxc.unlock();
    }
    #ifdef DEBUG
    if(v == d->thread_val)
    {
        msg<<"Lookup found yeah "<<d->thread_val<<endl;
        cout<<msg.str();
        msg.clear();

    }
    else{
    msg<<"Lookup not found..."<<endl;
    cout<<msg.str();
    msg.clear();

    }
    #endif // DEBUG

}

/*
*DESCRIPTION    :   Delete a random key from the hash table
*AUTHOR         :   AJAY SINGH
*COMPANY        :   IIT Hyderabad
*/
void del(thread_info *d)
{
	int result, trans_succ_flag;;
	long long error_id;
	int read_res=0;
	int write_res=0;
	stringstream msg;
    msg.clear();

    int v;

    trans_state* T = lib->begin();
    int k = num_op_per_tx;
while(k--){

   //find bucket
    d->thread_val = rand()%(MAX_KEY - 1) + 1; //this is the val that thread will work upon
    int bucket_id = d->thread_val%TAB_SIZE;
    int val_min = VAL_MIN + bucket_id;
    int val_max = VAL_MAX - bucket_id;

    common_tOB* set_obj_p = new common_tOB; //prev tob
    set_obj_p->size = sizeof(val_t);
    set_obj_p->value = operator new (sizeof(val_t));
    set_obj_p->ID = val_min;
    read_res = lib->read(T, set_obj_p);
    if(0 != read_res)
    {
        msg<<"-p:read fail"<<endl;
        msg<<"-p:killing : "<<d->thread_id<<endl;
        cout<<msg.str();
        msg.clear();

        mtxc.lock();
        num_lookup_aborts++;
        mtxc.unlock();
        //read has failed further execution of thread may cause seg fault
        return;//pthread_exit(NULL);
    }

    common_tOB* set_obj_n = new common_tOB; //next tob
    set_obj_n->size = sizeof(val_t);
    set_obj_n->value = operator new (sizeof(val_t));
    set_obj_n->ID = (*(val_t*)set_obj_p->value).nextID;
    read_res = lib->read(T, set_obj_n);
    if(0 != read_res)
    {
        msg<<"-n:read fail"<<endl;
        msg<<"-n:killing : "<<d->thread_id<<endl;
        cout<<msg.str();
        msg.clear();

        mtxc.lock();
        num_lookup_aborts++;
        mtxc.unlock();

        //read has failed further execution of thread may cause seg fault
        return;//pthread_exit(NULL);
    }

    while(1)
    {
        v = (*(val_t*)set_obj_p->value).nextID;
        if (v >= d->thread_val)
            break;//node is found or cannot be found by further iterations sincle list is in sorted oreder

        set_obj_p->ID = set_obj_n->ID;
        (*(val_t*)set_obj_p->value).v = (*(val_t*)set_obj_n->value).v; // instead read through the lib API
        (*(val_t*)set_obj_p->value).nextID = (*(val_t*)set_obj_n->value).nextID;

        set_obj_n->ID = (*(val_t*)set_obj_p->value).nextID;

        read_res = lib->read(T, set_obj_n);
        if(0 != read_res)
        {
            msg<<"-p:in while read fail"<<endl;
            msg<<"-p:killing : "<<d->thread_id<<endl;

            cout<<msg.str();
            msg.clear();

            mtxc.lock();
            num_lookup_aborts++;
            mtxc.unlock();

            //read has failed further execution of thread may cause seg fault
            return;//pthread_exit(NULL);
        }

    }

    //currently unique ID and val of node are same
    result = (v == d->thread_val);//value to be deleted is to be supplied by thread

    if (result)//means next obj is to be deleted
    {

        (*(val_t*)set_obj_p->value).nextID = (*(val_t*)set_obj_n->value).nextID; // node to be deleted is skipped
        write_res=lib->write(T, set_obj_p);
        if(0 != write_res)
        {
            msg<<"-write fail\n";
            cout<<msg.str();
            msg.clear();

        }

        write_res=lib->write(T, set_obj_n);
        if(0 != write_res)
        {
            msg<<"-write fail\n";
            cout<<msg.str();
            msg.clear();
        }

    }
    #ifdef DEBUG
    else
    {
        msg<<"not found!cannt be deleted"<<endl;
        cout<<msg.str();
        msg.clear();

    }
    #endif // DEBUG
}

    trans_succ_flag = lib->try_commit(T, error_id);
    if(0 != trans_succ_flag)
    {
        msg<<"-commit failed delete"<<d->thread_id<<endl;
        cout<<msg.str();
        msg.clear();

        mtxc.lock();
        num_del_aborts++;
        mtxc.unlock();
    }
    else
    {
        msg<<"$$found! deleted"<<endl;
        cout<<msg.str();
        msg.clear();
    }


//    delete(set_obj_p);
//	delete(set_obj_n);

}

/*
*DESCRIPTION    :   Moves key k1 to key k2 ie delete k1 and insert as k1. The value field of each node is treated as dummy as main purpose is to achieve move functionality once we delete and add a new node,
 changing values is not difficult.
*AUTHOR         :   AJAY SINGH
*COMPANY        :   IIT Hyderabad
*/
void move(thread_info *d)
{
    int result, trans_succ_flag;;
	long long error_id;
	int read_res=0;
	int write_res=0, k1notfound = 0, retry = 0;
	stringstream msg;
    msg.clear();
    int v, k1, k2;

    trans_state* T = lib->begin();

{
            //find bucket
    k1 = rand()%(MAX_KEY - 1) + 1; //this is the val that thread will work upon
    int bucket_id = k1%TAB_SIZE;
    int val_min = VAL_MIN + bucket_id;
    int val_max = VAL_MAX - bucket_id;


    common_tOB* set_obj_p = new common_tOB; //prev tob
    set_obj_p->size = sizeof(val_t);
    set_obj_p->value = operator new (sizeof(val_t));
    set_obj_p->ID = val_min;
    read_res = lib->read(T, set_obj_p);
    if(0 != read_res)
    {
        msg<<"@p:read fail"<<endl;
        msg<<"@p:killing : "<<d->thread_id<<endl;
        cout<<msg.str();
        msg.clear();

        mtxc.lock();
        num_lookup_aborts++;
        mtxc.unlock();
        //read has failed further execution of thread may cause seg fault
        return;//pthread_exit(NULL);
    }

    common_tOB* set_obj_n = new common_tOB; //next tob
    set_obj_n->size = sizeof(val_t);
    set_obj_n->value = operator new (sizeof(val_t));
    set_obj_n->ID = (*(val_t*)set_obj_p->value).nextID;
    read_res = lib->read(T, set_obj_n);
    if(0 != read_res)
    {
        msg<<"@n:read fail"<<endl;
        msg<<"@n:killing : "<<d->thread_id<<endl;
        cout<<msg.str();
        msg.clear();

        mtxc.lock();
        num_lookup_aborts++;
        mtxc.unlock();

        //read has failed further execution of thread may cause seg fault
        return;//pthread_exit(NULL);
    }

    while(1)
    {
        v = (*(val_t*)set_obj_p->value).nextID;
        if (v >= k1)
            break;//node is found or cannot be found by further iterations sincle list is in sorted oreder

        set_obj_p->ID = set_obj_n->ID;
        (*(val_t*)set_obj_p->value).v = (*(val_t*)set_obj_n->value).v; // instead read through the lib API
        (*(val_t*)set_obj_p->value).nextID = (*(val_t*)set_obj_n->value).nextID;

        set_obj_n->ID = (*(val_t*)set_obj_p->value).nextID;

        read_res = lib->read(T, set_obj_n);
        if(0 != read_res)
        {
            msg<<"@p:in while read fail"<<endl;
            msg<<"@p:killing : "<<d->thread_id<<endl;

            cout<<msg.str();
            msg.clear();

            mtxc.lock();
            num_lookup_aborts++;
            mtxc.unlock();

            //read has failed further execution of thread may cause seg fault
            return;//pthread_exit(NULL);
        }

    }

    //currently unique ID and val of node are same
    result = (v == k1);//value to be deleted is to be supplied by thread

    if (result)//means next obj is to be deleted
    {

        (*(val_t*)set_obj_p->value).nextID = (*(val_t*)set_obj_n->value).nextID; // node to be deleted is skipped
        write_res=lib->write(T, set_obj_p);
        if(0 != write_res)
        {
            msg<<"@write fail\n";
            cout<<msg.str();
            msg.clear();

        }

        write_res=lib->write(T, set_obj_n);
        if(0 != write_res)
        {
            msg<<"@write fail\n";
            cout<<msg.str();
            msg.clear();
        }

    }
    #ifdef DEBUG
    else
    {
        msg<<"not found!cannt be deleted: "<<k1<<endl;
        k1notfound = 1;
        cout<<msg.str();
        msg.clear();

    }
    #endif // DEBUG
}

/**INSERT K2**/

if(write_res == 0 && read_res == 0 && !k1notfound){

            //find bucket
    k2 = rand()%(MAX_KEY - 1) + 1; //this is the val that thread will work upon
    int bucket_id = k2%TAB_SIZE;
    int val_min = VAL_MIN + bucket_id;
    int val_max = VAL_MAX - bucket_id;

    msg<<"move:: k1:: k2--->"<<k1<<":"<<k2<<endl;
    cout<<msg.str();
    msg.clear();

	if(lib->create_new(k2, sizeof(val_t)) != SUCCESS)
    {
        msg<<"@@failed to create lid : "<<k2<<endl;
        cout<<msg.str();
        msg.clear();
        //assert(0);
    }

    //tob to be added
    common_tOB* set_obj = new common_tOB;
    set_obj->size = sizeof(val_t);
    set_obj->ID = k2;   //each thread will carry the value it wants to insert plus the unique ID of node here in this case both are same
    read_res = lib->read(T, set_obj);//mem allocated to set_obj->value
    if(0 != read_res)
    {
        msg<<"@@:read fail"<<endl;
        msg<<"@@:killing : "<<read_res<<":"<<d->thread_id<<endl;
        cout<<msg.str();
        msg.clear();

        mtxc.lock();
        num_ins_aborts++;
        mtxc.unlock();
        //read has failed further execution of thread may cause seg fault
        return;//pthread_exit(NULL);
    }

    //since v and id are same so assignin same val to both id and next by default
    (*(val_t*)set_obj->value).v = 0; //default init value of new node
    (*(val_t*)set_obj->value).nextID = val_max;


//preparing prev and next pointers for list traversal
    common_tOB* set_obj_p = new common_tOB; //prev tob
    set_obj_p->size = sizeof(val_t);
    set_obj_p->value = operator new (sizeof(val_t));
    set_obj_p->ID = val_min;
    read_res = lib->read(T, set_obj_p);
    if(0 != read_res)
    {
        msg<<"@@p:read fail"<<endl;
        msg<<"@@p:killing : "<<read_res<<":"<<d->thread_id<<endl;
        cout<<msg.str();
        msg.clear();

        mtxc.lock();
        num_ins_aborts++;
        mtxc.unlock();
//        read has failed further execution of thread may cause seg fault
        return;//pthread_exit(NULL);
    }

    common_tOB* set_obj_n = new common_tOB; //next tob
    set_obj_n->size = sizeof(val_t);
    set_obj_n->value = operator new (sizeof(val_t));
    set_obj_n->ID = (*(val_t*)set_obj_p->value).nextID;
    read_res = lib->read(T, set_obj_n);

    if(0 != read_res)
    {
        msg<<"@@n:read fail:"<<endl;
        msg<<"@@n:killing : "<<read_res<<":"<<d->thread_id<<endl;
        cout<<msg.str();
        msg.clear();

        mtxc.lock();
        num_ins_aborts++;
        mtxc.unlock();
  //      read has failed further execution of thread may cause seg fault
        return;//pthread_exit(NULL);
    }

	//linked list is in sorted order of val
	while ((set_obj_n->ID) < k2)
	{
	   set_obj_p->ID = set_obj_n->ID;
        (*(val_t*)set_obj_p->value).v = (*(val_t*)set_obj_n->value).v; // instead read through the lib API
        (*(val_t*)set_obj_p->value).nextID = (*(val_t*)set_obj_n->value).nextID;

        set_obj_n->ID = (*(val_t*)set_obj_p->value).nextID;// prev is equal to next now, (*(val_t*)set_obj_n->value).nextID is equally good.

        read_res = lib->read(T, set_obj_n);
        if(0 != read_res)
        {
            //  mtx.lock();
            msg<<"@@in while read fail :"<<set_obj_n->ID<<" "<<read_res<<endl;
            //    print_oblist();
            msg<<"@@killing : "<<d->thread_id<<endl;
            cout<<msg.str();
            msg.clear();
            //mtx.unlock();
            mtxc.lock();
            num_ins_aborts++;
            mtxc.unlock();
            //read failed no point in continuing transac , do releaselock
            pthread_exit(NULL);//or simply break ;
        }
	}

	if(0 == read_res)
	{
        result = ((set_obj_n->ID) != k2);

        if (result)//id node not already present insert node
        {

           (*(val_t*)set_obj_p->value).nextID = set_obj->ID;

           (*(val_t*)set_obj->value).nextID = set_obj_n->ID;

            write_res=lib->write(T, set_obj);
            if(0 != write_res)
            {
                msg<<"@@write fail\n";
                cout<<msg.str();
                msg.clear();
            }
            write_res=lib->write(T, set_obj_p);
            if(0 != write_res)
            {
                msg<<"@@write fail\n";
                cout<<msg.str();
                msg.clear();

            }
            write_res=lib->write(T, set_obj_n);
            if(0 != write_res)
            {
                msg<<"@@write fail\n";
                cout<<msg.str();
                msg.clear();
            }

        }
	}
}
else{
    //retry
    cout<<"retry"<<endl;
    retry =1;
}



if(read_res == 0 && write_res == 0 && !retry)
{
    trans_succ_flag = lib->try_commit(T, error_id);
    if(0 != trans_succ_flag)
    {
        msg<<"@commit failed to move"<<d->thread_id<<endl;
        cout<<msg.str();
        msg.clear();

        mtxc.lock();
        num_del_aborts++;
        mtxc.unlock();
    }
    else
    {
        msg<<"MOVED [k1->k2]"<<k1<<":"<<k2<<endl;
        cout<<msg.str();
        msg.clear();
    }
}

}






/*
*DESCRIPTION :  test() executes insert, delete or lookup operation by reading the thread_op of each thread
                if thread_op = 0 --> insert()
                or thread_op = 1 --> delete()
                or thread_op = 2 --> lookup()
*AUTHOR      :  AJAY SINGH
*COMPANY     :  IIT Hyderabad
*REMARK      :  didnot made separate set_add() as threadData was not getting carried and causing crash   TODO
*/
void* test(void *threadData)
{
thread_info *d = (thread_info *)threadData;

/*barrier for all threads otherwise sum inserting threads may arrive later than the main threads finish joining.*/
barrier_cross(d->barrier);

//find bucket
int bucket_id = d->thread_val%TAB_SIZE;

int val_min = VAL_MIN + bucket_id;
int val_max = VAL_MAX - bucket_id;

/*......................................................INSERT............................................................................*/
if(d->thread_op == 0)//insert()
{
    add(d);
//    cout<<"multi add..\n";
//    add(d);
//    cout<<"multi added..\n";

}//if(d->thread_id < insertNum)//insert()

/*......................................................INSERT END............................................................................*/

/*......................................................DELETE................................................................................*/
else if(d->thread_op == 1)//(d->thread_id < insertNum + delNum)//delete()
{
    del(d);
//    del(d);
//    del(d);
//    del(d);
}//remove
/*......................................................DELETE END............................................................................*/

/*......................................................LOOKUP............................................................................*/
else if(d->thread_op == 2)//contain
{
    lookup(d);
//    cout<<"multi lookup..\n";
//    lookup(d);
//    cout<<"multi lookuped..\n";
}

/*......................................................LOOKUP END............................................................................*/
else if(d->thread_op == 3)//contain
{
    move(d);
//    cout<<"multi lookup..\n";
//    lookup(d);
//    cout<<"multi lookuped..\n";
}
//    mtxc.lock();
//    cout<< d->thread_id<<"exiting"<<endl;
//    mtxc.unlock();

   pthread_exit(NULL);
}
