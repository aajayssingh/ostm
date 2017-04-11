/*
 * C++ Program to Implement Hash Tables Chaining with List Heads
 */
#include <iostream>
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


/* ---------------------------------------------------------------------------
 *
 * author: suzuki hironobu (hironobu@interdb.jp) 2009.Dec.01
 * Copyright (C) 2009-2014  suzuki hironobu
 *
 * ---------------------------------------------------------------------------
 */
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <sys/un.h>
#include <unistd.h>
#include <limits.h>
#include <assert.h>
#include <signal.h>

#include "common.h"




#define PIPE_MAXLINE 32
#define MAX_THREADS 200
#define MAX_ITEMS 30000
#define MAX_LEVEL 16

#define DEFAULT_THREADS 100
#define DEFAULT_ITEMS 1


static void core_dump(int sigid)
{
    kill(getpid(), SIGSEGV);
}


static pthread_mutex_t begin_mtx;
static pthread_cond_t begin_cond;
static unsigned int begin_thread_num;
static pthread_mutex_t end_mtx;
static pthread_cond_t end_cond;
static unsigned int end_thread_num;

static pthread_mutex_t add_print_mtx;
static pthread_mutex_t look_print_mtx;
static pthread_mutex_t del_print_mtx;



typedef struct {
    int thread_num;
    int item_num;
    int verbose;
    int max_level;
} system_variables_t;

struct stat_time {
    struct timeval begin;
    struct timeval end;
};
typedef struct stat_time stat_data_t;

/*
 * declartion
 */
static double get_interval(struct timeval, struct timeval);
static void master_thread(void);
//static void worker_thread(void *);

static void worker_thread_add(void *);
static void worker_thread_look(void *);
static void worker_thread_del(void *);

static int workbench(void);
static void usage(char **);
static void init_system_variables(void);
void add(int key, int thid)
{
//    pthread_mutex_lock(&print_mtx);
//    cout<<"\t\t key "<<key<<" thread id "<<thid<<"::"<<endl;
//    pthread_mutex_unlock(&print_mtx);

    trans_log* txlog;
    STATUS ops;
    int* val = new int;

    txlog = lib->begin();

        ops = lib->t_insert(txlog, 0, key, 100);//lib->t_lookup(txlog, 0, 6, val);
      //  cout<<"op status "<<ops<<endl;

        if(ABORT != ops)
            lib->tryCommit(txlog);
}

void look(int key, int thid)
{
//    pthread_mutex_lock(&print_mtx);
//    cout<<"\t\t key "<<key<<" thread id "<<thid<<"::"<<endl;
//    pthread_mutex_unlock(&print_mtx);

  trans_log* txlog;
    STATUS ops;
    int* val = new int;

    txlog = lib->begin();

        ops = lib->t_lookup(txlog, 0, key, val);
        //cout<<"op status "<<ops<<endl;

//        ops = lib->t_insert(txlog, 0, key, 100);//lib->t_lookup(txlog, 0, 6, val);
//        cout<<"op status "<<ops<<endl;
if(ABORT != ops)
    lib->tryCommit(txlog);
}


void del(int key, int thid)
{
//    pthread_mutex_lock(&print_mtx);
//    cout<<"\t\t key "<<key<<" thread id "<<thid<<"::"<<endl;
//    pthread_mutex_unlock(&print_mtx);

    trans_log* txlog;
    STATUS ops;
    int* val = new int;

    txlog = lib->begin();

        ops = lib->t_delete(txlog, 0, key, val);

       // cout<<"op status "<<ops<<endl;

//
//        ops = lib->t_insert(txlog, 0, key, 100);//lib->t_lookup(txlog, 0, 6, val);
//        cout<<"op status "<<ops<<endl;

if(ABORT != ops)
{
    ops = lib->tryCommit(txlog);

    #ifdef DEBUG_LOGS
    if(ops == ABORT)
    {
        cout<<"hello key "<<key<<endl;
    }
    #endif
//    elog("hi");
}

}






/*
 * global variables
 */
static system_variables_t system_variables;
static pthread_t *work_thread_tptr;
static pthread_t tid;
static stat_data_t *stat_data;

static struct timeval stat_data_begin;
static struct timeval stat_data_end;

/*
 * local functions
 */

static double get_interval(struct timeval bt, struct timeval et)
{
    double b, e;

    b = bt.tv_sec + (double) bt.tv_usec * 1e-6;
    e = et.tv_sec + (double) et.tv_usec * 1e-6;
    return e - b;
}


/*
 * master_thread
 */
static void master_thread(void)
{
    unsigned int i;

    /* wait for all threads end */
    pthread_mutex_lock(&end_mtx);
    while (system_variables.thread_num != end_thread_num)
	pthread_cond_wait(&end_cond, &end_mtx);
    pthread_mutex_unlock(&end_mtx);

    //    show_list(list);
//    free_list(list);

    /* display result */
    double tmp_itvl;
    double min_itvl = 0x7fffffff;
    double ave_itvl = 0.0;
    double max_itvl = 0.0;
    long double itvl = 0.0;

    unsigned long long int total = 0;
    long long int count1, count2;

    gettimeofday(&stat_data_end, NULL);

    total = 0;
    for (i = 0; i < system_variables.thread_num; i++) {
 //     total += sum[i];

      tmp_itvl = get_interval(stat_data[i].begin, stat_data[i].end);

      itvl += tmp_itvl;
      if (max_itvl < tmp_itvl)
	max_itvl = tmp_itvl;
      if (tmp_itvl < min_itvl)
	min_itvl = tmp_itvl;
      if (0 < system_variables.verbose)
	fprintf(stderr, "thread(%d) end %f[sec]\n", i, tmp_itvl);
    }


    fprintf (stderr, "condition =>\n");
    printf ("\t%d threads run\n", system_variables.thread_num);
    printf ("\t%d items inserted and deleted / thread, total %d items\n",
	    system_variables.item_num,
	    system_variables.item_num * system_variables.thread_num);

    assert(0 < system_variables.thread_num);
    ave_itvl = (double) (itvl / system_variables.thread_num);

    tmp_itvl = get_interval(stat_data_begin, stat_data_end);

    fprintf(stderr, "performance =>\n\tinterval =  %f [sec]\n", tmp_itvl);

    fprintf
      (stderr, "\tthread info:\n\t  ave. = %f[sec], min = %f[sec], max = %f[sec]\n",
       ave_itvl, min_itvl, max_itvl);

        cout<<endl<<"full table ::"<<endl;
        hasht->printTable();

        cout<<endl<<"blue table ::"<<endl;
        hasht->printBlueTable();
}

static void worker_thread_add(void *arg)
{
    uintptr_t no = (uintptr_t) arg;
    unsigned int i;
    int key;
    int getval;


    /*
     * increment begin_thread_num, and wait for broadcast signal from last created thread
     */
    if (system_variables.thread_num != 1) {
      pthread_mutex_lock(&begin_mtx);
      begin_thread_num++;
      if (begin_thread_num == system_variables.thread_num)
	pthread_cond_broadcast(&begin_cond);
      else {
	while (begin_thread_num < system_variables.thread_num)
	  pthread_cond_wait(&begin_cond, &begin_mtx);
      }
      pthread_mutex_unlock(&begin_mtx);
    }

    gettimeofday(&stat_data[no].begin, NULL);

    /*  main loop */
    key = no * system_variables.item_num;
    for (i = 0; i < system_variables.item_num; i++) {
        ++key;
        if (0 < system_variables.verbose)
            fprintf(stderr, "thread[%lu] add: %lu\n", (uintptr_t)no, (uintptr_t) key);

        pthread_mutex_lock(&add_print_mtx);
        cout<<"add \t\t key "<<key<<" thread id "<<no<<"::"<<endl;
        pthread_mutex_unlock(&add_print_mtx);


       add(key, no);

      //      usleep(no);
      //      pthread_yield(NULL);
    }


    /* send signal */
    gettimeofday(&stat_data[no].end, NULL);
    pthread_mutex_lock(&end_mtx);
    end_thread_num++;
    pthread_cond_signal(&end_cond);
    pthread_mutex_unlock(&end_mtx);
}


static void worker_thread_look(void *arg)
{
    uintptr_t no = (uintptr_t) arg;
    unsigned int i;
    int key;
    int getval;


    /*
     * increment begin_thread_num, and wait for broadcast signal from last created thread
     */
    if (system_variables.thread_num != 1) {
      pthread_mutex_lock(&begin_mtx);
      begin_thread_num++;
      if (begin_thread_num == system_variables.thread_num)
	pthread_cond_broadcast(&begin_cond);
      else {
	while (begin_thread_num < system_variables.thread_num)
	  pthread_cond_wait(&begin_cond, &begin_mtx);
      }
      pthread_mutex_unlock(&begin_mtx);
    }

    gettimeofday(&stat_data[no].begin, NULL);

    /*  main loop */
//    usleep(no * 10);

    key = no * system_variables.item_num;
    for (i = 0; i < system_variables.item_num; i++) {
        ++key;


        pthread_mutex_lock(&look_print_mtx);
        cout<<"lookup\t\t key "<<key<<" thread id "<<no<<"::"<<endl;
        pthread_mutex_unlock(&look_print_mtx);

        look(key, no);

          if (0 < system_variables.verbose)
            fprintf(stderr, "t_delete: val = %ld\n", getval);

    }


    /* send signal */
    gettimeofday(&stat_data[no].end, NULL);
    pthread_mutex_lock(&end_mtx);
    end_thread_num++;
    pthread_cond_signal(&end_cond);
    pthread_mutex_unlock(&end_mtx);
}


static void worker_thread_del(void *arg)
{
    uintptr_t no = (uintptr_t) arg;
    unsigned int i;
    int key;
    int getval;


    /*
     * increment begin_thread_num, and wait for broadcast signal from last created thread
     */
    if (system_variables.thread_num != 1) {
      pthread_mutex_lock(&begin_mtx);
      begin_thread_num++;
      if (begin_thread_num == system_variables.thread_num)
	pthread_cond_broadcast(&begin_cond);
      else {
	while (begin_thread_num < system_variables.thread_num)
	  pthread_cond_wait(&begin_cond, &begin_mtx);
      }
      pthread_mutex_unlock(&begin_mtx);
    }

    gettimeofday(&stat_data[no].begin, NULL);

    /*  main loop */
    key = no * system_variables.item_num;
    for (i = 0; i < system_variables.item_num; i++) {
        ++key;
        key = rand()%(20);

        pthread_mutex_lock(&del_print_mtx);
        cout<<"delete\t\t key "<<key<<" thread id "<<no<<"::"<<endl;
        pthread_mutex_unlock(&del_print_mtx);
        del(key, no);


          if (0 < system_variables.verbose)
            fprintf(stderr, "t_delete: val = %ld\n", getval);

    }


    /* send signal */
    gettimeofday(&stat_data[no].end, NULL);
    pthread_mutex_lock(&end_mtx);
    end_thread_num++;
    pthread_cond_signal(&end_cond);
    pthread_mutex_unlock(&end_mtx);
}


//static void worker_thread(void *arg)
//{
//    uintptr_t no = (uintptr_t) arg;
//    unsigned int i;
//    int key;
//    int getval;
//
//
//    /*
//     * increment begin_thread_num, and wait for broadcast signal from last created thread
//     */
//    if (system_variables.thread_num != 1) {
//      pthread_mutex_lock(&begin_mtx);
//      begin_thread_num++;
//      if (begin_thread_num == system_variables.thread_num)
//	pthread_cond_broadcast(&begin_cond);
//      else {
//	while (begin_thread_num < system_variables.thread_num)
//	  pthread_cond_wait(&begin_cond, &begin_mtx);
//      }
//      pthread_mutex_unlock(&begin_mtx);
//    }
//
//    gettimeofday(&stat_data[no].begin, NULL);
////    sum[no] = 0;
//
//    /*  main loop */
//    key = no * system_variables.item_num;
//    for (i = 0; i < system_variables.item_num; i++) {
//        ++key;
//        if (0 < system_variables.verbose)
//            fprintf(stderr, "thread[%lu] add: %lu\n", (uintptr_t)no, (uintptr_t) key);
//
//        pthread_mutex_lock(&print_mtx);
//        cout<<"\t\t key "<<key<<" thread id "<<no<<"::"<<endl;
//        pthread_mutex_unlock(&print_mtx);
//
//
//       add(key, no);
//
//      //      usleep(no);
//      //      pthread_yield(NULL);
//    }
//
//
//    usleep(no * 10);
//
//    key = no * system_variables.item_num;
//    for (i = 0; i < system_variables.item_num; i++) {
//        ++key;
//
//        look(key, no);
////        del(key, no);
//
//
//          if (0 < system_variables.verbose)
//            fprintf(stderr, "t_delete: val = %ld\n", getval);
//
//    }
//
//
//
//    key = no * system_variables.item_num;
//    for (i = 0; i < system_variables.item_num; i++) {
//        ++key;
//        key = rand()%(20);
//
//        pthread_mutex_lock(&print_mtx);
//        cout<<"\t\tDEL key "<<key<<" thread id "<<no<<"::"<<endl;
//        pthread_mutex_unlock(&print_mtx);
//        del(key, no);
//
//
//          if (0 < system_variables.verbose)
//            fprintf(stderr, "t_delete: val = %ld\n", getval);
//
//    }
//
//
//    /* send signal */
//    gettimeofday(&stat_data[no].end, NULL);
//    pthread_mutex_lock(&end_mtx);
//    end_thread_num++;
//    pthread_cond_signal(&end_cond);
//    pthread_mutex_unlock(&end_mtx);
//}

static int workbench(void)
{
    void *ret = NULL;
   //unsigned int i;

    fprintf(stderr, "<<simple algorithm test bench>>\n");


    if ((stat_data =
	 calloc(system_variables.thread_num, sizeof(stat_data_t))) == NULL)
    {
      elog("calloc error");
      goto end;
    }
    if ((work_thread_tptr =
	 calloc(system_variables.thread_num, sizeof(pthread_t))) == NULL) {
      elog("calloc error");
      goto end;
    }
    if (pthread_create(&tid, (void *) NULL, (void *) master_thread,
		       (void *) NULL) != 0) {
      elog("pthread_create() error");
      goto end;
    }
    gettimeofday(&stat_data_begin, NULL);

//    for (i = 0; i < system_variables.thread_num; i++)
//      if (pthread_create(&work_thread_tptr[i], NULL, (void *) worker_thread,
//			 (void *)(intptr_t) i) != 0) {
//	elog("pthread_create() error");
//	goto end;
//      }

int nlook = 0.7*system_variables.thread_num;
int nadd = 0.2*system_variables.thread_num;
int ndel = 0.1*system_variables.thread_num;

      for (int i = 0; i < nlook; i++)
      if (pthread_create(&work_thread_tptr[i], NULL, (void *) worker_thread_look,
			 (void *)(intptr_t) i) != 0) {
	elog("pthread_create() error");
	goto end;
      }

      for (int i = nlook; i < (nlook + nadd); i++)
      if (pthread_create(&work_thread_tptr[i], NULL, (void *) worker_thread_add,
			 (void *)(intptr_t) i) != 0) {
	elog("pthread_create() error");
	goto end;
      }

      for (int i = (nlook + nadd); i < (nlook + nadd + ndel); i++)
      if (pthread_create(&work_thread_tptr[i], NULL, (void *) worker_thread_del,
			 (void *)(intptr_t) i) != 0) {
	elog("pthread_create() error");
	goto end;
      }





      fprintf(stderr, "<<Gonna join>>\n");
     /*
     AJ fault : #4  0x0000000000402197 in workbench () at main.cpp:488
488		if (pthread_join(work_thread_tptr[i], &ret)) {

seg fault due to kill()

     */


    for (int j = 0; j < system_variables.thread_num; j++)
	if (pthread_join(work_thread_tptr[j], &ret)) {
	  elog("pthread_join() error");
	  goto end;
	}

    if (pthread_join(tid, &ret)) {
      elog("pthread_join() error");
      goto end;
    }

    return 0;

 end:
    free(stat_data);
    free(work_thread_tptr);
    return -1;
}


static void usage(char **argv)
{
    fprintf(stderr, "simple algorithm test bench\n");
    fprintf(stderr, "usage: %s [Options<default>]\n", argv[0]);
    fprintf(stderr, "\t\t-t number_of_threads<%d>\n", DEFAULT_THREADS);
    fprintf(stderr, "\t\t-n number_of_items<%d>\n", DEFAULT_ITEMS);
    fprintf(stderr, "\t\t-v               :verbose\n");
    fprintf(stderr, "\t\t-V               :debug mode\n");
    fprintf(stderr, "\t\t-h               :help\n");
}


static void init_system_variables(void)
{
    system_variables.thread_num = DEFAULT_THREADS;
    system_variables.item_num = DEFAULT_ITEMS;
//    system_variables.max_level = DEFAULT_LEVEL;
    system_variables.verbose = 0;
}


int main(int argc, char **argv)
{

    signal(SIGINT, core_dump);

    char c;

    /*
     * init
     */
    begin_mtx = (pthread_mutex_t) PTHREAD_MUTEX_INITIALIZER;
    begin_cond = (pthread_cond_t) PTHREAD_COND_INITIALIZER;
    end_mtx = (pthread_mutex_t) PTHREAD_MUTEX_INITIALIZER;
    end_cond = (pthread_cond_t) PTHREAD_COND_INITIALIZER;

    add_print_mtx = (pthread_mutex_t) PTHREAD_MUTEX_INITIALIZER;
    look_print_mtx = (pthread_mutex_t) PTHREAD_MUTEX_INITIALIZER;
    del_print_mtx = (pthread_mutex_t) PTHREAD_MUTEX_INITIALIZER;

    begin_thread_num = 0;
    end_thread_num = 0;
    init_system_variables();

    /* options  */
#if defined(_Skiplist_) || (_LazySkiplist_) || (_LockFreeSkiplist_)
    while ((c = getopt(argc, argv, "t:n:l:vVh")) != -1) {
#else
    while ((c = getopt(argc, argv, "t:n:vVh")) != -1) {
#endif
	switch (c) {
	case 't':		/* number of thread */
	    system_variables.thread_num = strtol(optarg, NULL, 10);
	    if (system_variables.thread_num <= 0) {
		fprintf(stderr, "Error: thread number %d is not valid\n",
			system_variables.thread_num);
		exit(-1);
	    } else if (MAX_THREADS <= system_variables.thread_num)
		system_variables.thread_num = MAX_THREADS;

	    break;
	case 'n':		/* number of item */
	    system_variables.item_num = strtol(optarg, NULL, 10);
	    if (system_variables.item_num <= 0) {
		fprintf(stderr, "Error: item number %d is not valid\n",
			system_variables.item_num);
		exit(-1);
	    } else if (MAX_ITEMS <= system_variables.item_num)
		system_variables.item_num = MAX_ITEMS;
	    break;
#if defined(_Skiplist_) || (_LazySkiplist_) || (_LockFreeSkiplist_)
	case 'l':		/* max level of skiplist */
	    system_variables.max_level = strtol(optarg, NULL, 10);
	    if (system_variables.max_level <= 0) {
		fprintf(stderr, "Error: max level %d is not valid\n",
			system_variables.max_level);
		exit(-1);
	    } else if (MAX_LEVEL <= system_variables.max_level)
		system_variables.max_level = MAX_LEVEL;
	    break;
#endif
	case 'v':               /* verbose 1 */
	    system_variables.verbose = 1;
	    break;
	case 'V':               /* verbose 2 */
	    system_variables.verbose = 2;
	    break;
	case 'h':	        /* help */
	    usage(argv);
	    exit(0);
	default:
	    fprintf(stderr, "ERROR: option error: -%c is not valid\n",
		    optopt);
	    exit(-1);
	}
    }

    /*
     * main work
     */
     srand(time(NULL));

    if (workbench() != 0)
      abort();

    free (stat_data);
    free (work_thread_tptr);

    return 0;
}

