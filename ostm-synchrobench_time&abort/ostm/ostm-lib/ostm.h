/*
*DESCRIPTION    :   OSTM hash table
*AUTHOR         :   AJAY SINGH
*COMPANY        :   IIT Hyderabad
*/
#ifndef OSTM_H
#define OSTM_H

#include "common.h"
#include "tablsl.h"
#include <vector>
#include <atomic>
#include <algorithm>


struct ll_entry
{
    int obj_id, bucketId;
    int key;
    int value;
    LinkedHashNode** preds; //alloc dynamically these arrays preds[]
    LinkedHashNode** currs;
    LinkedHashNode* node; //for the inserted single node in BL
    OPN_NAME opn;
    STATUS op_status;

    ~ll_entry()
    {
        delete[] preds;
        delete[] currs;
        preds = NULL;
        currs = NULL;
    }

};

//std::vector <int>al;
typedef std::vector < std::pair< int, ll_entry*> > ll_list;  /*local buffer - release the memory in commit* --> its key, ll_entry pair/

/*Transaction class*/
class trans_log
{
public:
  int tid;                          /*unique ID*/
  STATUS tx_status;
  ll_list ll;  /*local buffer - release the memory in commit*/

  trans_log(int );
  ~trans_log();              /*trans_state destructor frees memory allocated to write buffer*/


  int findinLL(int key);
  int createLLentry(int key);
  void setPredsnCurrs(int pos, LinkedHashNode** preds, LinkedHashNode** currs);
  void setOpnName(int pos, OPN_NAME opn);
  void setOpStatus(int pos, STATUS op_status);
  void setValue(int pos, int value);
  void setKey(int pos, int key);
  void setbucketId(int pos, int bid);

  OPN_NAME getOpn(int ll_pos);
  STATUS getOpStatus(int ll_pos);
  int getValue(int ll_pos);
  int getKey(int ll_pos);
  int getbucketId(int ll_pos);


  void printTxlog();
  void printPredsnCurrsTxlog();
};

class OSTM {


public:
    std::atomic<int> tid_counter;           /*keeps count of transactions for allocating new transaction IDs*/
    //std::atomic<int> abort_count;

    HashMap* hash_table;                /*shared memory*/ //will init hashtablle as in main.cpp
    // txlog;

    OSTM();

    ~OSTM();

    trans_log* begin();   /*starts a transaction by initing a log of it as well to record*/

    STATUS t_lookup(trans_log* txlog, int obj_id, int key, int* value);

    STATUS t_delete(trans_log* txlog, int obj_id, int key, int* value);
    STATUS t_insert(trans_log* txlog, int obj_id, int key, int value);
    STATUS tryCommit(trans_log* txlog);
    STATUS tryAbort(trans_log* txlog);
};

#endif //OSTM_H
