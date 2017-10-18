/*
 * C++ Program to Implement Hash Tables Chaining with List Heads
NOTE: EVALUATION key points
When each transaction has one operation   specially with only insert and delete operations There is no abort (negligible). This is an important advantage over r/w STM.

 */
#include <iostream>
#include <fstream>

#include "tablsl.h"
#include "ostm.h"
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>

#include "common.h"

using namespace std;
OSTM* lib = new OSTM();
HashMap* hasht  = lib->hash_table;


//DEFAULT VALUES
const uint_t num_threads = 200; /*multiple of 10 the better, for exact thread distribution */

/*should sum upto 100*/
uint_t num_insert_percent = 45;
uint_t num_delete_percent = 15;
uint_t num_lookup_percent = 30;
uint_t num_move_percent = 10;
uint_t num_insert, num_delete, num_lookup, num_move;
uint_t num_op_per_tx = 4;

std::thread t[num_threads];

/*************************Barrier code begins*****************************/
std::mutex mtx;
std::mutex pmtx; // to print in concurrent scene
std::condition_variable cv;
bool launch = false;

/*
* DESCP:	barrier to sychronize all threads after creation.
* AUTHOR:	Ajay Singh
*/
void wait_for_launch()
{
	std::unique_lock<std::mutex> lck(mtx);
	//printf("locked-waiting\n");
	while (!launch) cv.wait(lck);
}

/*
* DESCP:	let threads execute their task after final thread has arrived.
* AUTHOR:	Ajay Singh
*/
void shoot()
{
	std::unique_lock<std::mutex> lck(mtx);
	launch = true;
	//printf("main locked-notify\n");
	cv.notify_all();
}
/*************************Barrier code ends*****************************/


/*
 * declartion
 */

STATUS add_init(uint_t key, uint_t thid)
{
    trans_log* txlog;
    STATUS ops, txs;
    txlog = lib->begin();

//    uint_t key = rand()%(MAX_KEY - 1) + 1;
    uint_t val = rand()%(MAX_KEY - 1) + 1;
    ops = lib->t_insert(txlog, 0, key, val);//inserting value = 100

//    pmtx.lock();
    stringstream msg;
    msg<<" rv-phase insert [key:thid]       \t"<<key <<":"<<thid<<endl;
    msg<<" rv-phase insert [op status: thid]\t "<<status(ops) <<":"<<thid <<endl;
    cout<<msg.str();
//    pmtx.unlock();

    if(ABORT != ops)
        txs = lib->tryCommit(txlog);

    msg.clear();
//    pmtx.lock();
    msg<<" commit [op status: thid]\t "<< status(txs) <<":"<<thid <<endl<<endl;
    cout<<msg.str();
//    pmtx.unlock();

    return txs;
}


STATUS add(uint_t key, uint_t val, uint_t thid)
{
    trans_log* txlog;
    STATUS ops, txs;

    txlog = lib->begin();

//    uint_t key = rand()%(MAX_KEY - 1) + 1;
    ops = lib->t_insert(txlog, 0, key, 100);//inserting value = 100

//    pmtx.lock();
    stringstream msg;
    msg<<" rv-phase insert [key:thid]       \t"<<key <<":"<<thid<<endl;
    msg<<" rv-phase insert [op status: thid]\t "<<status(ops) <<":"<<thid <<endl;
    cout<<msg.str();
//    pmtx.unlock();

    if(ABORT != ops)
        txs = lib->tryCommit(txlog);

        msg.clear();
//    pmtx.lock();
//    stringstream msg;
    msg<<" commit [op status: thid]\t "<< status(txs) <<":"<<thid <<endl<<endl;
    cout<<msg.str();
//    pmtx.unlock();

    return txs;
}

STATUS look(uint_t key, uint_t thid)
{

    trans_log* txlog;
    STATUS ops, txs = ABORT;
    int* val = new int;

    txlog = lib->begin();

//    uint_t key = rand()%(MAX_KEY - 1) + 1;
    ops = lib->t_lookup(txlog, 0, key, val);

//    pmtx.lock();
    stringstream msg;
    msg<<" rv-phase lookup [key:thid]       \t"<<key <<":"<<thid<<endl;
    msg<<" rv-phase lookup [op status: thid]\t "<<status(ops) <<":"<<thid <<endl;
    cout<<msg.str();
//    pmtx.unlock();

    if(ABORT != ops) //execute only if all mths succeed in rv-method execution phase for this Tx
    {
        txs = lib->tryCommit(txlog);
    }

    msg.clear();
 //   stringstream msg;
    msg<<" commit [op status: thid]\t "<< status(txs) <<":"<<thid <<endl<<endl;
    cout<<msg.str();

    return txs;
}


STATUS del(uint_t key, uint_t thid)
{
//    pthread_mutex_lock(&print_mtx);
//    cout<<"\t\t key "<<key<<" thread id "<<thid<<"::"<<endl;
//    pthread_mutex_unlock(&print_mtx);

    trans_log* txlog;
    STATUS ops, txs = ABORT;
    int* val = new int;

    txlog = lib->begin();

//    uint_t key = rand()%(MAX_KEY - 1) + 1;
    ops = lib->t_delete(txlog, 0, key, val);//inserting value = 100

//    pmtx.lock();
    stringstream msg;
    msg<<" rv-phase delete [key:thid]       \t"<<key <<":"<<thid<<endl;
    msg<<" rv-phase delete [op status: thid]\t "<<status(ops) <<":"<<thid <<endl;
    cout<<msg.str();
//    pmtx.unlock();

    if(ABORT != ops)
    {
        txs = lib->tryCommit(txlog);
    }

    msg.clear();
//    pmtx.lock();
//    stringstream msg;
    msg<<" commit [op status: thid]\t "<< status(txs) <<":"<<thid <<endl<<endl;
    cout<<msg.str();
//    pmtx.unlock();

    return txs;

}

STATUS multilook(int thid)
{
//    pthread_mutex_lock(&print_mtx);
//    cout<<"\t\t key "<<key<<" thread id "<<thid<<"::"<<endl;
//    pthread_mutex_unlock(&print_mtx);

    trans_log* txlog;
    STATUS ops, txs = ABORT;
    int* val = new int;

    txlog = lib->begin();

    for(int i = 0; i < num_op_per_tx; i++)
    {

        uint_t key = rand()%(MAX_KEY - 1) + 1;
        ops = lib->t_lookup(txlog, 0, key, val);

  #if DEBUG_LOGS
  //      pmtx.lock();
        stringstream msg;
        msg<<" rv-phase lookup [key:thid]       \t"<<key <<":"<<thid<<endl;
        msg<<" rv-phase lookup [op status: thid]\t "<<status(ops) <<":"<<thid <<endl;
        cout<<msg.str();
 //       pmtx.unlock();
 #endif // DEBUG_LOGS
        if(ABORT == ops)
        {
            break;
        }

    }

    if(ABORT != ops) //execute only if all mths succeed in rv-method execution phase for this Tx
    {
        txs = lib->tryCommit(txlog);
    }

    #if DEBUG_LOGS
    stringstream msg;
    msg<<" commit [op status: thid]\t "<< status(txs) <<":"<<thid <<endl<<endl;
    cout<<msg.str();
    #endif // DEBUG_LOGS

    return txs;
}

STATUS multiadd(int thid)
{
//    pthread_mutex_lock(&print_mtx);
//    cout<<"\t\t key "<<key<<" thread id "<<thid<<"::"<<endl;
//    pthread_mutex_unlock(&print_mtx);

    trans_log* txlog;
    STATUS ops, txs;
    int* val = new int;

    txlog = lib->begin();

    for(int i = 0; i < num_op_per_tx; i++)
    {

        uint_t key = rand()%(MAX_KEY - 1) + 1;
        ops = lib->t_insert(txlog, 0, key, 100);//inserting value = 100

 #if DEBUG_LOGS
 //       pmtx.lock();
        stringstream msg;
        msg<<" rv-phase insert [key:thid]       \t"<<key <<":"<<thid<<endl;
        msg<<" rv-phase insert [op status: thid]\t "<<status(ops) <<":"<<thid <<endl;
        cout<<msg.str();
 //       pmtx.unlock();
 #endif // DEBUG_LOGS
        if(ABORT == ops)
        {
            break;
        }

    }


    //  cout<<"op status "<<ops<<endl;

    if(ABORT != ops)
        txs = lib->tryCommit(txlog);

    #if DEBUG_LOGS
    //pmtx.lock();
    stringstream msg;
    msg<<" commit [op status: thid]\t "<< status(txs) <<":"<<thid <<endl<<endl;
    cout<<msg.str();
   // pmtx.unlock();
   #endif // DEBUG_LOGS

    return txs;
}

STATUS muldel(int thid)
{
//    pthread_mutex_lock(&print_mtx);
//    cout<<"\t\t key "<<key<<" thread id "<<thid<<"::"<<endl;
//    pthread_mutex_unlock(&print_mtx);

    trans_log* txlog;
    STATUS ops, txs = ABORT;
    int* val = new int;

    txlog = lib->begin();

    for(int i = 0; i < num_op_per_tx; i++)
    {

        uint_t key = rand()%(MAX_KEY - 1) + 1;
        ops = lib->t_delete(txlog, 0, key, val);//inserting value = 100

      #if DEBUG_LOGS
      //  pmtx.lock();
        stringstream msg;
        msg<<" rv-phase delete [key:thid]       \t"<<key <<":"<<thid<<endl;
        msg<<" rv-phase delete [op status: thid]\t "<<status(ops) <<":"<<thid <<endl;
        cout<<msg.str();
     //   pmtx.unlock();
      #endif
        if(ABORT == ops)
        {
            break;
        }

    }

    if(ABORT != ops)
    {
        txs = lib->tryCommit(txlog);
    }

   #if DEBUG_LOGS
   // pmtx.lock();
    stringstream msg;
    msg<<" commit [op status: thid]\t "<< status(txs) <<":"<<thid <<endl<<endl;
    cout<<msg.str();
   // pmtx.unlock();
   #endif

return txs;

}

STATUS mov(uint_t key1, uint_t key2, uint_t val1, uint_t val2, uint_t thid)
{
//    pthread_mutex_lock(&print_mtx);
//    cout<<"\t\t key "<<key<<" thread id "<<thid<<"::"<<endl;
//    pthread_mutex_unlock(&print_mtx);

    trans_log* txlog;
    STATUS ops, txs = ABORT;
    int* val = new int;

    txlog = lib->begin();


    ops = lib->t_delete(txlog, 0, key1, val);//inserting value = 100

 #if DEBUG_LOGS
  //  pmtx.lock();
    stringstream msg;
    msg<<" rv-phase move-delete [key:thid]       \t"<<key1 <<":"<<thid<<endl;
    msg<<" rv-phase move-delete [op status: thid]\t "<<status(ops) <<":"<<thid <<endl;
    cout<<msg.str();
  //  pmtx.unlock();

    msg.clear();
      #endif // DEBUG_LOGS

    if(ABORT != ops) //check for ops Fail
    {
        ops = lib->t_insert(txlog, 0, key2, 100);//inserting value = 100

        #if DEBUG_LOGS
//        pmtx.lock();
//        stringstream msg;
        msg<<" rv-phase move-insert [key:thid]       \t"<<key2 <<":"<<thid<<endl;
        msg<<" rv-phase move-insert [op status: thid]\t "<<status(ops) <<":"<<thid <<endl;
        cout<<msg.str();
//        pmtx.unlock();
#endif // DEBUG_LOGS
    }


    if(ABORT != ops)
    {
        txs = lib->tryCommit(txlog);
    }

    #if DEBUG_LOGS
    msg.clear();
    //pmtx.lock();
//    stringstream msg;
    msg<<" commit move [op status: thid]\t "<< status(txs) <<":"<<thid <<endl<<endl;
    cout<<msg.str();
    //pmtx.unlock();
    #endif // DEBUG_LOGS

return txs;

}



/*
* DESCP:	worker for threads that call ht's function as per their distribution.
* AUTHOR:	Ajay Singh
*/
void worker(uint_t tid)
{
	//barrier to synchronise all threads for a coherent launch :)
	wait_for_launch();

	if(tid < num_insert)
	{
        #if !(MULTI_OP)
        uint_t key = rand()%(MAX_KEY - 1) + 1;
        uint_t val = rand()%(MAX_KEY - 1) + 1;
        add(key, val, tid);
        std::cout<<"not multi--------------------------------------\n";
        #endif

		multiadd(tid);
	}
	else if(tid < (num_insert + num_delete ))
    {
        #if !(MULTI_OP)
        uint_t key = rand()%(MAX_KEY - 1) + 1;
        uint_t val;
        del(key, tid);
        #endif


        muldel(tid);
    }
	else if(tid < (num_insert + num_delete + num_lookup))//init for lookup threads
	{
        #if !(MULTI_OP)
        uint_t key = rand()%(MAX_KEY - 1) + 1;
        look(key, tid);
        #endif

		multilook(tid);
	}
	else if(tid < (num_insert + num_delete + num_lookup + num_move))//init for lookup threads
	{
        uint_t key1 = rand()%(MAX_KEY - 1) + 1;
        uint_t val1 = rand()%(MAX_KEY - 1) + 1;

        uint_t key2 = rand()%(MAX_KEY - 1) + 1;
        uint_t val2 = rand()%(MAX_KEY - 1) + 1;

		mov(key1, key2, val1, val2, tid);
	}
	else
	{
		std::cout<<"something wrong in thread distribution to operations"<<std::endl;
	}
}

int main(int argc, char **argv)
{

    double duration;
	struct timeval start_time, end_time;

	if((num_insert_percent + num_delete_percent + num_lookup_percent + num_move_percent) != 100)
	{
		std::cout<<"Oo LaLa! Seems you got arithmatic wrong :) #operations should sumup to 100" <<std::endl;
		return 0;
	}

	time_t tt;
	srand(time(&tt));

	num_insert = (uint_t)ceil((num_insert_percent*num_threads)/100);
	num_delete = (uint_t)ceil((num_delete_percent*num_threads)/100);
	num_lookup = (uint_t)ceil((num_lookup_percent*num_threads)/100);
	num_move = (uint_t)ceil((num_move_percent*num_threads)/100);

	std::cout<<" num_insert:"<<num_insert<<"\n num_delete: "<<num_delete<<"\n num_lookup: "<<num_lookup<<"\n num_move: "<<num_move<<"\n";

	if((num_insert + num_delete + num_lookup + num_move) > num_threads)
	{
		std::cout<<"((insertNum + delNum + lookupNum) > number_of_threads)"<<std::endl;
		return 0;
	}

	for (uint_t i = 0; i < num_threads; ++i)
	{
		t[i] = std::thread(worker, i);
	}

    std::cout<<" intial table: \n";

    for (uint_t i = 0; i < 25; ++i)
    {

        trans_log* txlog;
        STATUS ops, txs;
        uint_t key = rand()%(MAX_KEY - 1) + 1;
        uint_t val = rand()%(MAX_KEY - 1) + 1;

        txlog = lib->begin();
        ops = lib->t_insert(txlog, 0, key, val);//inserting value = 100

        if(ABORT != ops)
            txs = lib->tryCommit(txlog);

        if(ABORT == txs)
        {
            cout<<" main thread populationg table aborted. \n";
        }
    }

    hasht->printTable();
    hasht->printBlueTable();

	std::cout <<"\n********STARTING...\n";
	gettimeofday(&start_time, NULL);
	shoot(); //notify all threads to begin the worker();

	for (uint_t i = 0; i < num_threads; ++i)
	{
		t[i].join();
	}
	gettimeofday(&end_time, NULL);
	std::cout <<"\n********STOPPING...\n";

	hasht->printTable();
	hasht->printBlueTable();


	duration = (end_time.tv_sec - start_time.tv_sec);
	duration += (end_time.tv_usec - start_time.tv_usec)/ 1000000.0;
	std::cout<<"time: "<<duration<<"seconds"<<std::endl;

	return 0;
}

