/*
 * C++ Program to Implement Hash Tables Chaining with List Heads
NOTE: EVALUATION key points
When each transaction has one operation   specially with only insert and delete operations There is no abort (negligible). This is an important advantage over r/w STM.

 */
#include <iostream>
#include <fstream>

#include "tablsl.h"
#include "ostm.h"
using namespace std;
OSTM* lib = new OSTM();
HashMap* hasht  = lib->hash_table;

/*
 * Main Contains Menu
 */
 #if 0
int main()
{

#if 1


    cout<<"0. OSTM opn"<<endl;

    OSTM* lib = new OSTM();
    trans_log* txlog;
    STATUS ops;
    int* val = new int;
//    txlog = lib->begin();
//
//    ops = lib->t_lookup(txlog, 0, 5, val);
//    cout<<"op status "<<ops<<endl;
//
//    ops = lib->t_lookup(txlog, 0, 10, val);
//    cout<<"op status "<<ops<<endl;
//
//    ops = lib->t_lookup(txlog, 0, 15, val);
//    cout<<"op status "<<ops<<endl;
//    lib->tryCommit(txlog);
//
//
    txlog = lib->begin();
    ops = lib->t_insert(txlog, 0, 5, 1000);//lib->t_lookup(txlog, 0, 6, val);
    cout<<"op status "<<ops<<endl;

    ops = lib->t_insert(txlog, 0, 10, 10);//lib->t_lookup(txlog, 0, 6, val);
    cout<<"op status "<<ops<<endl;

    lib->tryCommit(txlog);

    txlog = lib->begin();

    ops = lib->t_lookup(txlog, 0, 5, val);
    cout<<"op status "<<ops<< *val<<endl;


    ops = lib->t_insert(txlog, 0, 5, 100);//lib->t_lookup(txlog, 0, 6, val);
    cout<<"op status "<<ops<<endl;
    lib->tryCommit(txlog);


    HashMap* hash  = lib->hash_table;
    int key, value;
    int choice;
    while(1)
    {
        cout<<"\n----------------------"<<endl;
        cout<<"Operations on Hash Table"<<endl;
        cout<<"\n----------------------"<<endl;




        cout<<"1.lslInsert element into the table"<<endl;
        cout<<"2.Search element from the key"<<endl;
        cout<<"3.lslDelete element at a key"<<endl;
        cout<<"4.Exit"<<endl;
        cout<<"Enter your choice: ";
        cin>>choice;
        switch(choice)
        {

        case 1:
            cout<<"Enter element to be inserted: ";
            cin>>value;
            cout<<"Enter key at which element to be inserted: ";
            cin>>key;
            hash->lslInsert(key, value);
            break;

        case 2:
            cout<<"Enter key of the element to be searched: ";
            cin>>key;
            if (hash->lslSearch(key) == -1)
            cout<<"No element found at key "<<key<<endl;
            else
            {
                cout<<"Elements at key "<<key<<" : ";
                cout<<hash->lslSearch(key)<<endl;
            }
            break;
        case 3:
            cout<<"Enter key of the element to be deleted: ";
            cin>>key;
            if (hash->lslSearch(key) == -1)
                cout<<"Key "<<key<<" is empty, nothing to t_delete"<<endl;
            else
            {
                hash->lslDelete(key);
                cout<<"Entry Deleted"<<endl;
            }
            break;
        case 5:
            hash->printTable();
            break;
        case 4:
            exit(1);
        default:
           cout<<"\nEnter correct option\n";
       }
    }
    #else

    OSTM* lib = new OSTM();
    trans_log* txlog = lib->begin();
    int* value = new int;
    lib->t_lookup(txlog, 0, 5, value);



    #endif



    return 0;
}
#endif

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>

#include "common.h"

//DEFAULT VALUES
const uint_t num_threads = 100; /*multiple of 10 the better, for exact thread distribution */

/*should sum upto 100*/
uint_t num_insert_percent = 50;
uint_t num_delete_percent = 40;
uint_t num_lookup_percent = 10;
uint_t num_insert, num_delete, num_lookup;

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

STATUS add(int key, int thid)
{
//    pthread_mutex_lock(&print_mtx);
//    cout<<"\t\t key "<<key<<" thread id "<<thid<<"::"<<endl;
//    pthread_mutex_unlock(&print_mtx);

    trans_log* txlog;
    STATUS ops, txs;
    int* val = new int;

    txlog = lib->begin();

//    if(thid%2)
//    {
//    key  = 3;
//    }
//    else
//    {
//        key = 8;
//    }



    ops = lib->t_insert(txlog, 0, key, 100);//inserting value = 100
    //  cout<<"op status "<<ops<<endl;

    if(ABORT != ops)
        txs = lib->tryCommit(txlog);

    pmtx.lock();
    stringstream msg;
    msg<<" rv-phase insert  [key:thid]      \t"<<key <<":"<<thid<<endl;
    msg<<" rv-phase insert [op status: thid]\t "<<status(ops) <<":"<<thid <<endl;
    msg<<" commit [op status: thid]\t "<< status(txs) <<":"<<thid <<endl<<endl;
    cout<<msg.str();
    pmtx.unlock();

    return txs;
}

STATUS look(int key, int thid)
{
//    pthread_mutex_lock(&print_mtx);
//    cout<<"\t\t key "<<key<<" thread id "<<thid<<"::"<<endl;
//    pthread_mutex_unlock(&print_mtx);

    trans_log* txlog;
    STATUS ops, txs = ABORT;
    int* val = new int;

    txlog = lib->begin();

    ops = lib->t_lookup(txlog, 0, key, val);

    //        ops = lib->t_insert(txlog, 0, key, 100);//lib->t_lookup(txlog, 0, 6, val);
    //        cout<<"op status "<<ops<<endl;
    if(ABORT != ops)
    {
        txs = lib->tryCommit(txlog);
    }

    pmtx.lock();
    stringstream msg;
    msg<<" rv-phase lookup  [key:thid]      \t"<<key <<":"<<thid<<endl;
    msg<<" rv-phase lookup [op status: thid]\t "<<status(ops) <<":"<<thid <<endl;
    msg<<" commit [op status: thid]\t "<< status(txs) <<":"<<thid <<endl<<endl;
    cout<<msg.str();
    pmtx.unlock();

    return txs;
}


STATUS del(int key, int thid)
{
//    pthread_mutex_lock(&print_mtx);
//    cout<<"\t\t key "<<key<<" thread id "<<thid<<"::"<<endl;
//    pthread_mutex_unlock(&print_mtx);

    trans_log* txlog;
    STATUS ops, txs = ABORT;
    int* val = new int;

    txlog = lib->begin();
    ops = lib->t_delete(txlog, 0, key, val);

    if(ABORT != ops)
    {
        txs = lib->tryCommit(txlog);
    }

    pmtx.lock();
    stringstream msg;
    msg<<" rv-phase delete  [key:thid]      \t"<<key <<":"<<thid<<endl;
    msg<<" rv-phase delete [op status: thid]\t "<<status(ops) <<":"<<thid <<endl;
    msg<<" commit [op status: thid]\t "<< status(txs) <<":"<<thid <<endl<<endl;
    cout<<msg.str();
    pmtx.unlock();

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

    for(int i = 0; i < 10; i++)
    {

        uint_t key = rand()%(MAX_KEY - 1) + 1;
        ops = lib->t_lookup(txlog, 0, key, val);

        pmtx.lock();
        stringstream msg;
        msg<<" rv-phase lookup [key:thid]       \t"<<key <<":"<<thid<<endl;
        msg<<" rv-phase lookup [op status: thid]\t "<<status(ops) <<":"<<thid <<endl;
        cout<<msg.str();
        pmtx.unlock();
        if(ABORT == ops)
        {
            break;
        }

    }

    if(ABORT != ops) //execute only if all mths succeed in rv-method execution phase for this Tx
    {
        txs = lib->tryCommit(txlog);
    }

    stringstream msg;
    msg<<" commit [op status: thid]\t "<< status(txs) <<":"<<thid <<endl<<endl;
    cout<<msg.str();

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

    for(int i = 0; i < 10; i++)
    {

        uint_t key = rand()%(MAX_KEY - 1) + 1;
        ops = lib->t_insert(txlog, 0, key, 100);//inserting value = 100

        pmtx.lock();
        stringstream msg;
        msg<<" rv-phase insert [key:thid]       \t"<<key <<":"<<thid<<endl;
        msg<<" rv-phase insert [op status: thid]\t "<<status(ops) <<":"<<thid <<endl;
        cout<<msg.str();
        pmtx.unlock();
        if(ABORT == ops)
        {
            break;
        }

    }


    //  cout<<"op status "<<ops<<endl;

    if(ABORT != ops)
        txs = lib->tryCommit(txlog);

    pmtx.lock();
    stringstream msg;
    msg<<" commit [op status: thid]\t "<< status(txs) <<":"<<thid <<endl<<endl;
    cout<<msg.str();
    pmtx.unlock();

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

    for(int i = 0; i < 10; i++)
    {

        uint_t key = rand()%(MAX_KEY - 1) + 1;
        ops = lib->t_delete(txlog, 0, key, val);//inserting value = 100

        pmtx.lock();
        stringstream msg;
        msg<<" rv-phase delete [key:thid]       \t"<<key <<":"<<thid<<endl;
        msg<<" rv-phase delete [op status: thid]\t "<<status(ops) <<":"<<thid <<endl;
        cout<<msg.str();
        pmtx.unlock();
        if(ABORT == ops)
        {
            break;
        }

    }

    if(ABORT != ops)
    {
        txs = lib->tryCommit(txlog);
    }

    pmtx.lock();
    stringstream msg;
    msg<<" commit [op status: thid]\t "<< status(txs) <<":"<<thid <<endl<<endl;
    cout<<msg.str();
    pmtx.unlock();

return txs;

}

STATUS mov(int thid)
{
//    pthread_mutex_lock(&print_mtx);
//    cout<<"\t\t key "<<key<<" thread id "<<thid<<"::"<<endl;
//    pthread_mutex_unlock(&print_mtx);

    trans_log* txlog;
    STATUS ops, txs = ABORT;
    int* val = new int;

    txlog = lib->begin();

    //for(int i = 0; i < 10; i++)
    {

        uint_t key = rand()%(MAX_KEY - 1) + 1;
        ops = lib->t_insert(txlog, 0, key, 100);//inserting value = 100

        pmtx.lock();
        stringstream msg;
        msg<<" rv-phase insert [key:thid]       \t"<<key <<":"<<thid<<endl;
        msg<<" rv-phase insert [op status: thid]\t "<<status(ops) <<":"<<thid <<endl;
        cout<<msg.str();
        pmtx.unlock();

        if(ABORT != ops)
        {

            uint_t key = rand()%(MAX_KEY - 1) + 1;
            ops = lib->t_delete(txlog, 0, key, val);//inserting value = 100

            pmtx.lock();
            stringstream msg;
            msg<<" rv-phase delete [key:thid]       \t"<<key <<":"<<thid<<endl;
            msg<<" rv-phase delete [op status: thid]\t "<<status(ops) <<":"<<thid <<endl;
            cout<<msg.str();
            pmtx.unlock();
        }

        if(ABORT != ops)
        {
            uint_t key = rand()%(MAX_KEY - 1) + 1;
            ops = lib->t_lookup(txlog, 0, key, val);

            pmtx.lock();
            stringstream msg;
            msg<<" rv-phase lookup [key:thid]       \t"<<key <<":"<<thid<<endl;
            msg<<" rv-phase lookup [op status: thid]\t "<<status(ops) <<":"<<thid <<endl;
            cout<<msg.str();
            pmtx.unlock();
         }

        if(ABORT != ops)
        {
            uint_t key = rand()%(MAX_KEY - 1) + 1;
            ops = lib->t_insert(txlog, 0, key, 100);//inserting value = 100

            pmtx.lock();
            stringstream msg;
            msg<<" rv-phase insert [key:thid]       \t"<<key <<":"<<thid<<endl;
            msg<<" rv-phase insert [op status: thid]\t "<<status(ops) <<":"<<thid <<endl;
            cout<<msg.str();
            pmtx.unlock();
        }


        if(ABORT != ops)
        {
            uint_t key = rand()%(MAX_KEY - 1) + 1;
            ops = lib->t_lookup(txlog, 0, key, val);

            pmtx.lock();
            stringstream msg;
            msg<<" rv-phase lookup [key:thid]       \t"<<key <<":"<<thid<<endl;
            msg<<" rv-phase lookup [op status: thid]\t "<<status(ops) <<":"<<thid <<endl;
            cout<<msg.str();
            pmtx.unlock();
         }



    }

    if(ABORT != ops)
    {
        txs = lib->tryCommit(txlog);
    }

    pmtx.lock();
    stringstream msg;
    msg<<" commit [op status: thid]\t "<< status(txs) <<":"<<thid <<endl<<endl;
    cout<<msg.str();
    pmtx.unlock();

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
		uint_t key = rand()%(MAX_KEY - 1) + 1;
		//uint_t val = rand()%(MAX_KEY - 1) + 1;
		//add(key, tid);
		multiadd(tid);
	}
	else if(tid < (num_insert + num_delete ))
    {
    	uint_t key = rand()%(MAX_KEY - 1) + 1;
		//uint_t val;
//        del(key, tid);
        //muldel(tid);
        mov(tid);
    }
	else if(tid < (num_insert + num_delete + num_lookup))//init for lookup threads
	{
		uint_t key = rand()%(MAX_KEY - 1) + 1;

		//look(key, tid);
		multilook(tid);
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

	if((num_insert_percent + num_delete_percent + num_lookup_percent) != 100)
	{
		std::cout<<"Oo LaLa! Seems you got arithmatic wrong :) #operations should sumup to 100" <<std::endl;
		return 0;
	}

	time_t tt;
	srand(time(&tt));

	num_insert = (uint_t)ceil((num_insert_percent*num_threads)/100);
	num_delete = (uint_t)ceil((num_delete_percent*num_threads)/100);
	num_lookup = (uint_t)ceil((num_lookup_percent*num_threads)/100);

	std::cout<<" num_insert:"<<num_insert<<"\n num_delete: "<<num_delete<<"\n num_lookup: "<<num_lookup<<std::endl;

	if((num_insert + num_delete + num_lookup) > num_threads)
	{
		std::cout<<"((insertNum + delNum + lookupNum) > number_of_threads)"<<std::endl;
		return 0;
	}

	for (uint_t i = 0; i < num_threads; ++i)
	{
		t[i] = std::thread(worker, i);
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

