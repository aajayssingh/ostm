checking for composability no overhead.
only app1 is supposed to run.


IITHSTM--> g++ -std=c++11 main.cpp -o rwstm -lpthread -ltbb


home/sathya/aj-ws/ostm-synchrobench/IITH_STM  main.cpp

line 65  key range
line 151 threads
line 164 workload





g++ -std=c++11 main.cpp ostm-lib/ostm.cpp ostm-lib/tablsl.cpp -o OSTM -lpthread



.............
OSTM:
.............

common.h
MAX_KEY-----

main.cpp
line 32  num_threads
line 44  workload



......................
ESTM:
......................

>>>linkedlist.h  in aj-ws/ostm-synchrobench/ESTM/c-cpp/src/linkedlists/lockfree-list

#define DEFAULT_DURATION                100
#define DEFAULT_INITIAL                 5//256 //tab size
#define DEFAULT_NB_THREADS             	32
#define DEFAULT_RANGE                   1000//0x7FFFFFFF


>>>test.c  in aj-ws/ostm-synchrobench/ESTM/c-cpp/src/hashtables/lockfree-ht

int ajmovp = 0;
	int ajaddp = 30;
	int ajremp = 40;
	int ajcontp = 30;

compile>
/aj-ws/ostm-synchrobench/ESTM/c-cpp$ make -f Makefile.x86_64.linux

run>
/aj-ws/ostm-synchrobench/ESTM/c-cpp$ bin/ESTM-hashtable
