/*
*DESCRIPTION        :   file containing OSTM exported methods
*AUTHOR             :   AJAY SINGH (M.TECH. Student)
*INSTITUTE          :   IIT Hyderabad
*DATE               :   Jan 2018
*/
#include "ostm.h"

trans_log:: trans_log(int tid)
{

    //init tid
    this->tid = tid;

  //debug default init of local log
  #ifdef DEBUG_INIT_LOG
    for (int i=0; i<5; i++)
    {

        //init local log for this transaction
        ll_entry* log_entry = new ll_entry;
        log_entry->key = i;
        log_entry->value = i;
        log_entry->opn = LOOKUP;
        log_entry->preds = new LinkedHashNode*[2];
        log_entry->currs = new LinkedHashNode*[2];
        log_entry->op_status = DEFAULT_OP_STATUS;

//        this->ll.push_back(std::pair<int, ll_entry*>(i, log_entry));
        this->ll.push_back(std::make_pair(i, log_entry));
    }
    #endif // DEBUG

}


trans_log::~trans_log()
{
//    map<int,common_tOB*>::iterator tb;
//    for (tb = (local_buffer).begin(); tb != (local_buffer).end(); tb++)
//    {
//      delete(tb->second);            /*invokes tOB destructor*/
//    }
//    map<int,common_tOB*>::iterator begin_itr=local_buffer.begin();
//    map<int,common_tOB*>::iterator end_itr=local_buffer.end();
//    local_buffer.erase(begin_itr,end_itr);

//delete trans_log call des from ~OSTM
for(int i = 0; i < this->ll.size(); i++)
{
   /*temp COMMENT possible seg fault at POval*/

    //this->ll[i].second->~ll_entry();  //deletes preds n currs -->> verify that original shared memory pointers are not freed.
}
//delete[] this->ll;
/*TEMP comment*/
//this->ll.clear();

}

/*
* DESCP:    creates local entry.
* AUTHOR:   Ajay Singh
*/
int trans_log:: createLLentry(int key)
{
        ll_entry* log_entry = new ll_entry;
        log_entry->key = DEFAULT_KEY;
        log_entry->value = DEFAULT_VALUE;
        log_entry->opn = DEFAULT_OPN_NAME;
        log_entry->obj_id = DEFAULT_VALUE;
        log_entry->bucketId = DEFAULT_VALUE;
        log_entry->preds = new LinkedHashNode*[2];
        log_entry->currs = new LinkedHashNode*[2];
        log_entry->node = NULL;
        log_entry->op_status = DEFAULT_OP_STATUS;

        this->ll.push_back(std::make_pair(key, log_entry));

        return (this->ll.size() - 1); //the returned index is used to index into the log vector to access the log entry, thus we return size-1.
}

/*
* DESCP:    transaction log utility.
* AUTHOR:   Ajay Singh
*/
void trans_log::setPredsnCurrs(int pos, LinkedHashNode** preds, LinkedHashNode** currs)
{
    this->ll[pos].second->preds[0] = preds[0];
    this->ll[pos].second->preds[1] = preds[1];
    this->ll[pos].second->currs[0] = currs[0];
    this->ll[pos].second->currs[1] = currs[1];
}

/*
* DESCP:    transaction log utility.
* AUTHOR:   Ajay Singh
*/
void trans_log::setOpnName(int pos, OPN_NAME opn)
{
    this->ll[pos].second->opn = opn;
}

/*
* DESCP:    transaction log utility.
* AUTHOR:   Ajay Singh
*/
void trans_log::setOpStatus(int pos, STATUS op_status)
{
    this->ll[pos].second->op_status = op_status;
}

/*
* DESCP:    transaction log utility.
* AUTHOR:   Ajay Singh
*/
void trans_log::setValue(int pos, int value)
{
    this->ll[pos].second->value = value;
}

/*
* DESCP:    transaction log utility.
* AUTHOR:   Ajay Singh
*/
void trans_log::setKey(int pos, int key)
{
    this->ll[pos].second->key = key;
}

/*
* DESCP:    transaction log utility.
* AUTHOR:   Ajay Singh
*/
void trans_log::setbucketId(int pos, int bid)
{
    this->ll[pos].second->bucketId = bid;
}

/*
* DESCP:    transaction log utility.
* AUTHOR:   Ajay Singh
*/
int trans_log::findinLL(int key)
{
//    this->ll[0].first;
//
//    std::cout<< "inside findll"<<std::endl;
//    for(int i = 0; i < this->ll.size(); i++)
//    {
//    std::cout<< this->ll[i].first<<" "<< this->ll[i].second->opn;
//    }

    for(int i = 0; i < this->ll.size(); i++)
    {
        if(key == this->ll[i].first)
        {
            return i;
        }
    }

// NOTE (ajay#1#): change the bad index to a negative value. ...
//And corespondingly change the sanity check for bad index everywhere in lookup insert delete.
    return BAD_INDEX;
}

/*
* DESCP:    transaction log utility.
* AUTHOR:   Ajay Singh
*/
OPN_NAME trans_log::getOpn(int ll_pos)
{
    if(this->ll[ll_pos].second)
        return this->ll[ll_pos].second->opn;
    else{
            #if DEBUG_LOGS
        std::cout <<"ll[ll_pos].second is  NULL"<<std::endl;
    #endif // DEBUG_LOGS
        return WRONG_OPN;
    }
}

/*
* DESCP:    transaction log utility.
* AUTHOR:   Ajay Singh
*/
int trans_log::getValue(int ll_pos)
{
    if(this->ll[ll_pos].second)
        return this->ll[ll_pos].second->value;
    else{
        std::cout <<"ll[ll_pos].second is  NULL"<<std::endl;
        return BAD_VALUE;
    }
}

/*
* DESCP:    transaction log utility.
* AUTHOR:   Ajay Singh
*/
int trans_log::getKey(int ll_pos)
{
    if(this->ll[ll_pos].second)
        return this->ll[ll_pos].second->key;
    else{
        std::cout <<"ll[ll_pos].second is  NULL"<<std::endl;
        return BAD_VALUE;
    }
}

/*
* DESCP:    transaction log utility.
* AUTHOR:   Ajay Singh
*/
STATUS trans_log::getOpStatus(int ll_pos)
{

    if(this->ll[ll_pos].second)
        return this->ll[ll_pos].second->op_status;
    else{
        std::cout <<"ll[ll_pos].second is  NULL"<<std::endl;
        return WRONG_STATUS;
    }
}

/*
* DESCP:    transaction log utility.
* AUTHOR:   Ajay Singh
*/
void trans_log::printTxlog()
{
    std::stringstream msg;
    msg<<"PRINTING TXLOG "<<std::endl;
    for(int i = 0; i < this->ll.size(); i++)
    {
        msg <<this->ll[i].second->bucketId <<":"<<this->ll[i].first<<" ";
    }
    std::cout<<msg.str()<<std::endl;
}

/*
* DESCP:    transaction log utility.
* AUTHOR:   Ajay Singh
*/
void trans_log::printPredsnCurrsTxlog()
{
    std::stringstream msg;
    msg<<"printPredsnCurrsTxlog "<<std::endl;
    for(int i = 0; i < this->ll.size(); i++)
    {
        msg<<"printPredsnCurrsTxlog:: [key] - preds "<<this->ll[i].first<<" "<<this->ll[i].second->preds[0]->key<<"-"<<this->ll[i].second->preds[0]<<" "<<this->ll[i].second->preds[1]->key<<"-"<<this->ll[i].second->preds[1]<<std::endl;
        msg<<"printPredsnCurrsTxlog:: [key] - currs "<<this->ll[i].first<<" "<<this->ll[i].second->currs[0]->key<<"-"<<this->ll[i].second->currs[0]<<" "<<this->ll[i].second->currs[1]->key<<"-"<<this->ll[i].second->currs[1]<<std::endl;
        msg<<std::endl;
    }
    std::cout<<msg.str()<<std::endl;
}

/*
* DESCP:    lib constructor.
* AUTHOR:   Ajay Singh
*/
OSTM::OSTM()
{
    hash_table = new HashMap();                /*shared memory*/ //will init hashtablle as in main.cpp
}

/*
* DESCP:    library destructor sorry ;) I ve abused standards and havn't used it to free resources.
            Don't worry nothing will go wrong... All works so well ;P
* AUTHOR:   Ajay Singh
*/
OSTM::~OSTM()
{
//int hello;
}

/*
* DESCP:    Exported method to begin transaction.
* AUTHOR:   Ajay Singh
*/
trans_log* OSTM::begin()
{
    //init the transaction --> tid and local log
    int tid = tid_counter++;  //it is atomic var
    trans_log* txlog = new trans_log(tid);
    return txlog;
}

/*
* DESCP:    Exported method to lookup.
* AUTHOR:   Ajay Singh
*/
STATUS OSTM::t_lookup(trans_log* txlog, int obj_id, int key, int* value)
{

STATUS op_status = RETRY;
int tid = txlog->tid;

//local log:
int ll_pos = txlog->findinLL(key);
//assert(ll_pos != BAD_INDEX);

if(BAD_INDEX != ll_pos)
{
    OPN_NAME opn = txlog->getOpn(ll_pos);
    if( (INSERT == opn) || (LOOKUP == opn) )
    {
        *value = txlog->getValue(ll_pos);
        //assert(*value != BAD_VALUE);

        op_status = txlog->getOpStatus(ll_pos);
        //assert(op_status != WRONG_STATUS);
    }
    else if(DELETE == opn)
    {
        *value = DEFAULT_VALUE; //node doesnt exist so value is bad
        op_status = FAIL;
    }

    //update local log
    txlog->setValue(ll_pos, *value);
    txlog->setKey(ll_pos, key);
    txlog->setOpStatus(ll_pos, op_status);
    txlog->setOpnName(ll_pos, LOOKUP);

}
else
{

    LinkedHashNode* preds[2];
    LinkedHashNode* currs[2];
    *value = BAD_VALUE;

    op_status = hash_table->lslSch(obj_id, key, value, preds, currs, RV, tid);

    if(ABORT == op_status)
    {
        tryAbort(txlog);

        #if DEBUG_LOGS
        std::stringstream msg;
        msg<<"t_lookup::ABORT {key-op}-->"<< key <<"--"<<opname(LOOKUP)<<std::endl;
        std::cout<<msg.str();
#endif // DEBUG_LOGS

// NOTE (ajay#1#): May Shift the unlocks to LslSch().
        preds[0]->lmutex.unlock();
        preds[1]->lmutex.unlock();

        currs[0]->lmutex.unlock();
        currs[1]->lmutex.unlock();

        return ABORT;
    }
    else
    {
        if(key == currs[1]->key)
        {
            op_status = OK;
            currs[1]->max_ts.look_ts = txlog->tid;

//            txlog->ll[0].second->preds[0]->key;
        }
        else if(key == currs[0]->key)
        {
            op_status = FAIL;
            currs[0]->max_ts.look_ts = txlog->tid;
        }
        else
        {

            hash_table->lslIns(key, 0, preds, currs, RL);
            op_status = FAIL;
            preds[1]->red_next->max_ts.look_ts = txlog->tid; //setting ts of new created node
        }
    }
#if DEBUG_LOGS
    std::cout<<"t_lookup:: lslsearch returned preds "<<preds[0]->key<<" "<<preds[1]->key<<std::endl;
    std::cout<<"t_lookup:: lslsearch returned currs "<<currs[0]->key<<" "<<currs[1]->key<<std::endl;
    std::cout<<"t_lookup:: lslsearch returned tid "<<txlog->tid<<std::endl;
#endif // DEBUG_LOGS
    //create ll_entry and log all new values for subsequent ops
    int ll_pos = txlog->createLLentry(key);
    txlog->setPredsnCurrs(ll_pos, preds, currs);
    txlog->setOpnName(ll_pos, LOOKUP);
    txlog->setValue(ll_pos, *value);
    txlog->setKey(ll_pos, key);
    txlog->setOpStatus(ll_pos, op_status);
    txlog->setbucketId(ll_pos, hash_table->HashFunc(key));

    preds[0]->lmutex.unlock();
    preds[1]->lmutex.unlock();

    currs[0]->lmutex.unlock();
    currs[1]->lmutex.unlock();
}

//check if lslSearch works fine
//hash_table->lslSearch(0);

return op_status;
}


/*
* DESCP:    Exported method to delete.
* AUTHOR:   Ajay Singh
*/
STATUS OSTM::t_delete(trans_log* txlog, int obj_id, int key, int* value)
{

STATUS op_status = RETRY;
int tid = txlog->tid;

//local log:
int ll_pos = txlog->findinLL(key);
//assert(ll_pos != BAD_INDEX);

if(BAD_INDEX != ll_pos)
{
    OPN_NAME opn = txlog->getOpn(ll_pos);
    if( INSERT == opn)
    {
        *value = txlog->getValue(ll_pos);
        //assert(*value != BAD_VALUE);

        op_status = txlog->getOpStatus(ll_pos); //op_status should be OK as prev opn is t_insert
        //assert(op_status != WRONG_STATUS);
    }
    else if(DELETE == opn)
    {
        *value = DEFAULT_VALUE; //node doesnt exist so value is bad
        op_status = FAIL;
    }
    else
    {
        *value = txlog->getValue(ll_pos);
        op_status = txlog->getOpStatus(ll_pos);

    }

    //update local log
    txlog->setValue(ll_pos, *value);
    txlog->setKey(ll_pos, key);
    txlog->setOpStatus(ll_pos, op_status);
    txlog->setOpnName(ll_pos, DELETE);

}
else
{

    LinkedHashNode* preds[2];
    LinkedHashNode* currs[2];
    *value = BAD_VALUE;

    op_status = hash_table->lslSch(obj_id, key, value, preds, currs, RV, tid);

    if(ABORT == op_status)
    {
        tryAbort(txlog);
        #if DEBUG_LOGS
        std::stringstream msg;
        msg<<"t_delete::ABORT {key-op}-->"<< key <<"--"<<opname(DELETE)<<std::endl;
        std::cout<<msg.str();
#endif // DEBUG_LOGS
        preds[0]->lmutex.unlock();
        preds[1]->lmutex.unlock();

        currs[0]->lmutex.unlock();
        currs[1]->lmutex.unlock();

        return ABORT;
    }
    else
    {
        if(key == currs[1]->key)
        {
            op_status = OK;
            currs[1]->max_ts.look_ts = txlog->tid;

//            txlog->ll[0].second->preds[0]->key;
        }
        else if(key == currs[0]->key)
        {
            op_status = FAIL;
            currs[0]->max_ts.look_ts = txlog->tid;
            *value = BAD_VALUE;
        }
        else
        {

            hash_table->lslIns(key, 0, preds, currs, RL);
            op_status = FAIL;
// NOTE (ajay#1#): check either lookup or delete TS to be updated
//setting ts of new created node, setting a lookts helps the concurrent lookups to succeed thus lesser aborts.
//the method is identified by its name in commit and there we update the delete TS.
//So only the current TX executing this method is saved from aborts.
            preds[1]->red_next->max_ts.look_ts = txlog->tid;
            *value = BAD_VALUE;
        }
    }

    #if DEBUG_LOGS
    std::cout<<"t_delete:: lslsearch returned preds "<<preds[0]->key<<" "<<preds[1]->key<<std::endl;
    std::cout<<"t_delete:: lslsearch returned currs "<<currs[0]->key<<" "<<currs[1]->key<<std::endl;
    std::cout<<"t_delete:: lslsearch returned tid "<<txlog->tid<<std::endl;
    #endif // DEBUG_LOGS

    //create ll_entry and log all new values for subsequent ops
    int ll_pos = txlog->createLLentry(key);
    txlog->setPredsnCurrs(ll_pos, preds, currs);
    txlog->setOpnName(ll_pos, DELETE);
    txlog->setValue(ll_pos, *value);
    txlog->setKey(ll_pos, key);
    txlog->setOpStatus(ll_pos, op_status);
    txlog->setbucketId(ll_pos, hash_table->HashFunc(key));

    preds[0]->lmutex.unlock();
    preds[1]->lmutex.unlock();

    currs[0]->lmutex.unlock();
    currs[1]->lmutex.unlock();
}

//check if lslSearch works fine
//hash_table->lslSearch(0);

return op_status;
}

/*
* DESCP:    Exported method to insert.
* AUTHOR:   Ajay Singh
*/
STATUS OSTM::t_insert(trans_log* txlog, int obj_id, int key, int value)
{
    //local log:
    int ll_pos = txlog->findinLL(key);
    //assert(ll_pos != BAD_INDEX);

    if(BAD_INDEX != ll_pos)
    {
        //update local log
        //op_status = OK;
        txlog->setValue(ll_pos, value);
        txlog->setKey(ll_pos, key);
        txlog->setOpStatus(ll_pos, OK);
        txlog->setOpnName(ll_pos, INSERT);

    }
    else //the log entry for the key is not found. Create a new log entry.
    {
     //create ll_entry and log all new values for subsequent ops
        int ll_pos = txlog->createLLentry(key);
     //   txlog->setPredsnCurrs(ll_pos, preds, currs);
        txlog->setOpnName(ll_pos, INSERT);
        txlog->setValue(ll_pos, value);
        txlog->setKey(ll_pos, key);
        txlog->setOpStatus(ll_pos, OK);
        txlog->setbucketId(ll_pos, hash_table->HashFunc(key));
    }
    return OK;
}

/*
* DESCP:    Exported method to Abort.
* AUTHOR:   Ajay Singh
*/
STATUS OSTM::tryAbort(trans_log* txlog)
{
    //causes abort at po validation if release mem here
   // std::cout<<"abort called"<<std::endl;
   // aborts_count++;
    //txlog->~trans_log();

}

bool compare_entry( const std::pair< int, ll_entry*> &e1, const std::pair< int, ll_entry*> &e2)
{

    if(e1.second->bucketId != e2.second->bucketId)
        return (e1.second->bucketId < e2.second->bucketId);
    return (e1.first < e2.first);
}

// TODO (ajay#1#): Optimize trycommit for lookups
/*
* DESCP:    Exported method to commit.
* AUTHOR:   Ajay Singh
*/
STATUS OSTM::tryCommit(trans_log* txlog)
{
    STATUS tx_status = ABORT;
    STATUS op_status = DEFAULT_OP_STATUS;
    int tid = txlog->tid;
    //STATUS op_status = RETRY;
    sort(txlog->ll.begin(), txlog->ll.end(), compare_entry);  //use sorted list now on, as irrelevant the order of opn due to locks

    //txlog->printTxlog();
{
    LinkedHashNode* preds[2];
    LinkedHashNode* currs[2];
//  int  *value = BAD_VALUE;

    for(int i = 0; i < txlog->ll.size(); i++)
    {
        //get key and obj id
        int key = txlog->ll[i].first;
        int obj_id = txlog->ll[i].second->obj_id;

        if((txlog->ll[i].second->opn == LOOKUP)|| ((txlog->ll[i].second->opn == DELETE)&&(FAIL == txlog->getOpStatus(i))))
            continue;
/*For the updates methods again find the preds and currs which would be locked, remeber insert doesnt take lock so no worry of lock not released for the one taken earlier
Also Deletes take lock and release in rv-execution phase itself so no worry of lossing track of the locked nodes due to lslSch() here again.*/

        op_status = hash_table->lslSch(obj_id, key, NULL, preds, currs, TRYCOMMIT, tid); //??test value is null

        txlog->setPredsnCurrs(i, preds, currs);

        if(ABORT == op_status)
        {
            //release locks and memory
            #if DEBUG_LOGS
            std::stringstream msg;
            msg<<"trycommit::abort called {key-op}-->"<< key <<"--"<<opname(txlog->ll[i].second->opn)<<std::endl;
            std::cout<<msg.str();
            #endif // DEBUG_LOGS
            tryAbort(txlog);//delete all dynamic allocation of transaction

            //release lock for all the nodes in the local log
            for(int j = 0; j <= i; j++)
            {


            if((txlog->ll[j].second->opn == LOOKUP)|| ((txlog->ll[j].second->opn == DELETE)&&(FAIL == txlog->getOpStatus(j))))
                continue;

                txlog->ll[j].second->preds[0]->lock_count--;
                txlog->ll[j].second->preds[1]->lock_count--;
                txlog->ll[j].second->currs[0]->lock_count--;
                txlog->ll[j].second->currs[1]->lock_count--;

                txlog->ll[j].second->preds[0]->lmutex.unlock();
                txlog->ll[j].second->preds[1]->lmutex.unlock();
                txlog->ll[j].second->currs[0]->lmutex.unlock();
                txlog->ll[j].second->currs[1]->lmutex.unlock();

            }

//        std::cout<<"tryCommit:: lslsearch returned preds "<<preds[0]->key<<" "<<preds[1]->key<<std::endl;
//        std::cout<<"tryCommit:: lslsearch returned currs "<<currs[0]->key<<" "<<currs[1]->key<<std::endl;
//        std::cout<<"tryCommit:: lslsearch returned tid   "<<txlog->tid<<std::endl;

            return ABORT;
        }

      //  txlog->setPredsnCurrs(i, preds, currs);

#if DEBUG_LOGS
        std::cout<<"tryCommit:: lslsearch returned preds "<<preds[0]->key<<" "<<preds[1]->key<<std::endl;
        std::cout<<"tryCommit:: lslsearch returned currs "<<currs[0]->key<<" "<<currs[1]->key<<std::endl;
        std::cout<<"tryCommit:: lslsearch returned tid "<<txlog->tid<<std::endl;
#endif // DEBUG_LOGS
    }
}

   // txlog->printPredsnCurrsTxlog();
#if DEBUG_LOGS
   txlog->printTxlog();
#endif
/*change the underlying ds*/
    for(int i = 0; i < txlog->ll.size(); i++)
    {
        //get key and obj id
        int key = txlog->ll[i].first;
        int obj_id = txlog->ll[i].second->obj_id;
        OPN_NAME opn = txlog->ll[i].second->opn;

        if((txlog->ll[i].second->opn == LOOKUP)|| ((txlog->ll[i].second->opn == DELETE)&&(FAIL == txlog->getOpStatus(i))))//(txlog->ll[i].second->opn == LOOKUP) //TODO : include case where delete fails
            continue;

// TODO (ajay#1#): po validation
        //poval

       // if(i > 0) //for multiple update operations within a transaction where the preds and currs overlap the lost update may happen.
        {
            if((txlog->ll[i].second->preds[0]->marked)/*previous op was a delete*/ || (txlog->ll[i].second->preds[0]->blue_next != txlog->ll[i].second->currs[1])/*prev op was insert*/)
            {

                //since locks are recursive before changing the preds0 or currs unlock the pred0 as its sure that the old pred0 has been locked multiple times thus
                //while relasingall locks their would be a correct tracks of number of locks taken and would exactly released same number
                //of time as number of times it was acquired.

                assert( (txlog->ll[i].second->preds[0]->marked) || (txlog->ll[i].second->preds[0]->blue_next != txlog->ll[i].second->currs[1]));
                //assert(txlog->ll[i].second->preds[0]->marked);
                //search previous log entry which is update operation and belongs to same bucket as ith operation.
                int k = -1;
                int j = i-1;
                while(j >= 0)
                {
                    //calculate bucket of the current key and jth key
                    int bi = hash_table->HashFunc(key);
                    int bj = hash_table->HashFunc(txlog->ll[j].first);
                    if((bi == bj) && (LOOKUP != txlog->ll[j].second->opn)&& (!((txlog->ll[j].second->opn == DELETE)&&(FAIL == txlog->ll[j].second->op_status)))) //if both belong to same list and th ejth op is not lookup
                    {
                        k = j;
                        break;
                    }
                    j--;
                }
//                if(k == -1)
//                {
//                std::stringstream msg;
//                msg <<"LOL " <<key<<txlog->ll[i].second->preds[0]->key<<"-"<<txlog->ll[i].second->currs[1]->key<<std::endl;
//                std::cout<<msg.str();
//
//                }
                assert(k != -1);//k can never be -1 bcse if this is executed means their is a prev consecutive operation.

                if(INSERT == txlog->ll[k].second->opn) //if previous op was insert
                {
                    txlog->ll[i].second->preds[0]->lock_count--;
                    txlog->ll[i].second->preds[0]->lmutex.unlock(); //ulock the ols pred0 to balance the number of times acquired and release

                    //if(txlog->ll[i-1].second->preds[0]->blue_next->key == txlog->ll[i-1].second->key ) //check to verify correct pred is assigned
                    assert(txlog->ll[k].second->preds[0]->blue_next != NULL);
                    assert(txlog->ll[k].second->preds[0]->blue_next->key == txlog->ll[k].second->key);
                    //assert(txlog->ll[k].second->preds[0]->blue_next->key == txlog->ll[k].second->node->key);


                   //again take lock on node*** node field is just used to maintain lock balance not to set preds[0] as it might be null
                   //in case prev node was RL to BL insert.
                    txlog->ll[i].second->preds[0] = txlog->ll[k].second->preds[0]->blue_next;//txlog->ll[k].second->node;// //or the node of [i-1]
                    txlog->ll[i].second->preds[0]->lmutex.lock();
                    txlog->ll[i].second->preds[0]->lock_count++;
                    //txlog->ll[i].second->preds[0]->lmutex.lock(); //lock the previous inserted node
                }
                else{ //if previous op was a delete
                //release lock on current pred 0, since pred0 is gonna be changed this node will remain locked forever.
                    txlog->ll[i].second->preds[0]->lock_count--;
                    txlog->ll[i].second->preds[0]->lmutex.unlock(); //ulock the ols pred0 to balance the number of times acquired and release

                    txlog->ll[i].second->preds[0] = txlog->ll[k].second->preds[0];

                    txlog->ll[i].second->preds[0]->lmutex.lock();
                    txlog->ll[i].second->preds[0]->lock_count++;

                }

                //if red links have changed find that too
                if(txlog->ll[i].second->preds[1]->red_next != txlog->ll[i].second->currs[0])
                {
                    txlog->ll[i].second->preds[1]->lock_count--;
                    txlog->ll[i].second->preds[1]->lmutex.unlock(); //ulock the ols pred0 to balance the number of times acquired and release

                    assert(txlog->ll[k].second->preds[1]->red_next != NULL);
                    assert(txlog->ll[k].second->preds[1]->red_next->key == txlog->ll[k].second->key );


                    txlog->ll[i].second->preds[1] = txlog->ll[k].second->preds[1]->red_next;

                    txlog->ll[i].second->preds[1]->lmutex.lock();
                    txlog->ll[i].second->preds[1]->lock_count++;
                }


            }
        }

        if(INSERT == opn)
        {

            //fetch value from local log
// NOTE (ajay#1#): test if value is bad value or use get value---txlog->getValue(i);
            int value = txlog->getValue(i);//txlog->ll[i].second->value; //? test if value is bad value or use get value---txlog->getValue(i);

            if(key == txlog->ll[i].second->currs[1]->key)//key already present as blue node
            {
                //fetch value from local log
                //int value = txlog->getValue(i);//txlog->ll[i].second->value; //? test if value is bad value or use get value---txlog->getValue(i);

                //set value to underlying table node
                txlog->ll[i].second->currs[1]->value = value;
                txlog->ll[i].second->currs[1]->max_ts.ins_ts = txlog->tid;
                txlog->setOpStatus(i, OK);
            }
            else if(key == txlog->ll[i].second->currs[0]->key)//key is already present as red node
            {
                //int value = txlog->getValue(i);

                hash_table->lslIns(key, value, txlog->ll[i].second->preds, txlog->ll[i].second->currs, RL_BL);
                txlog->ll[i].second->currs[0]->max_ts.ins_ts = txlog->tid;
                txlog->setOpStatus(i, OK);
            }
            else //new node for sure added in this case so update ts of the node
            {
                //int value = txlog->getValue(i);

                hash_table->lslIns(key, value, txlog->ll[i].second->preds, txlog->ll[i].second->currs, BL);

//                std::stringstream msg;
//                msg<<"tryCommit:: inserting[key] preds "<<key<<" " <<txlog->ll[i].second->preds[0]->key<<" "<<txlog->ll[i].second->preds[1]->key<<std::endl;
//                msg<<"tryCommit:: inserting[key] currs "<<key<<" " <<txlog->ll[i].second->currs[0]->key<<" "<<txlog->ll[i].second->currs[1]->key<<std::endl;
//                std::cout<<msg.str();

                assert(key == txlog->ll[i].second->preds[0]->blue_next->key); //check the new node is logged indeed correctly and is in DS
                txlog->ll[i].second->preds[0]->blue_next->max_ts.ins_ts = txlog->tid;

                txlog->ll[i].second->node = txlog->ll[i].second->preds[0]->blue_next;//update the node field for inserted node in log entry
                #if DEBUG_LOGS
                std::stringstream msg;
                msg<<"   ------>   Key " << key<<txlog->ll[i].second->node<<":"<<txlog->ll[i].second->node->key<<" "<<tid<<std::endl;
                std::cout<<msg.str();
                #endif // DEBUG_LOGS

                //txlog->ll[i].second->node->lmutex.lock();
                txlog->ll[i].second->node->lock_count++;
                txlog->setOpStatus(i, OK);
            }

            #if DEBUG_LOGS
            std::cout<<"tryCommit:: INSERT "<<" "<<key<<std::endl;
            #endif // DEBUG_LOGS

        }
        else if(DELETE == opn)
        {
            if(key == txlog->ll[i].second->currs[1]->key)
            {
                hash_table->lslDel(txlog->ll[i].second->preds, txlog->ll[i].second->currs);
                txlog->ll[i].second->currs[1]->max_ts.del_ts = txlog->tid;

                txlog->setOpStatus(i, OK);
            }
            else//key guaranteed to be present in red list
            {
//                assert(txlog->ll[i].second->currs[0]->key == key);
//                txlog->ll[i].second->currs[0]->max_ts.del_ts = txlog->tid;
//
//                txlog->setOpStatus(i, FAIL);
            }

            #if DEBUG_LOGS
            std::cout<<"tryCommit:: DELETE "<<" "<<key<<std::endl;
            #endif // DEBUG_LOGS

        }

    }


    //prepare a vector of all preds[], currs[] and node as a pair <key, hashNode>
    //wherer key is key in hash node. Hashnode is pred, curr or node in txlog
    std::vector < std::pair< int, LinkedHashNode*> > listOfAllNodes;
    for(int i = 0; i < txlog->ll.size(); i++)
    {
        int k;
        LinkedHashNode* nd;

         if((txlog->ll[i].second->opn == LOOKUP)|| ((txlog->ll[i].second->opn == DELETE)&&(FAIL == txlog->getOpStatus(i))))//(txlog->ll[i].second->opn == LOOKUP)
            continue;

        k = txlog->ll[i].second->preds[0]->key;
        nd = txlog->ll[i].second->preds[0];
        listOfAllNodes.push_back(std::make_pair(k, nd));

        k = txlog->ll[i].second->preds[1]->key;
        nd = txlog->ll[i].second->preds[1];
        listOfAllNodes.push_back(std::make_pair(k, nd));

        k = txlog->ll[i].second->currs[0]->key;
        nd = txlog->ll[i].second->currs[0];
        listOfAllNodes.push_back(std::make_pair(k, nd));

        k = txlog->ll[i].second->currs[1]->key;
        nd = txlog->ll[i].second->currs[1];
        listOfAllNodes.push_back(std::make_pair(k, nd));

        if(txlog->ll[i].second->node)
        {
            k = txlog->ll[i].second->node->key;
            nd = txlog->ll[i].second->node;
            listOfAllNodes.push_back(std::make_pair(k, nd));
        }
    }
// NOTE (ajay#1#): sort the all node log by key object as well

    sort(listOfAllNodes.begin(), listOfAllNodes.end());

//    for(int i = 0; i < listOfAllNodes.size(); i++)
//    {
//        std::cout<<listOfAllNodes[i].first<<" ";
//    }
//    std::cout<<std::endl;

    //check lock balance for this tx
   // #if 0
//    for(int i = 0; i < listOfAllNodes.size(); i++)
//     {
//        if(listOfAllNodes[i].second->lock_count)
//        {
//            std::stringstream msg;
//            msg<<"      *key:lockcount:tid* " << listOfAllNodes[i].second->key<<":"<<listOfAllNodes[i].second->lock_count<<" "<<tid<<std::endl;
//            std::cout<<msg.str();
//        }
//     }

   // hash_table->printTable();
    //hash_table->printBlueTable();
   #if DEBUG_LOGS
    std::cout<<"to check inf loop AJAY"<<std::endl;
   #endif

    //release locks code
     for(int i = 0; i < listOfAllNodes.size(); i++)
     {

//        if(txlog->ll[i].second->opn == LOOKUP)
//            continue;

//        txlog->ll[i].second->preds[0]->lmutex.unlock();
//        txlog->ll[i].second->preds[1]->lmutex.unlock();
//        txlog->ll[i].second->currs[0]->lmutex.unlock();
//        txlog->ll[i].second->currs[1]->lmutex.unlock();

        listOfAllNodes[i].second->lock_count--;
        listOfAllNodes[i].second->lmutex.unlock();

     }
#if DEBUG_LOGS
    std::cout<<"\n";
    #endif // DEBUG_LOGS
    //check lock balance for this tx
 #if 0
    std::stringstream msg;
    msg<<"AFTER RELEASE: "<<std::endl;
    std::cout<<msg.str();

    for(int i = 0; i < listOfAllNodes.size(); i++)
     {
       // if(listOfAllNodes[i].second->lock_count)
        {
//            while(listOfAllNodes[i].second->lock_count>0)
//            {
//                listOfAllNodes[i].second->lmutex.unlock();
//                listOfAllNodes[i].second->lock_count--;
//            }

            std::stringstream msg;
            msg<<"*************Lock Count for key" << listOfAllNodes[i].second->key<<":"<<listOfAllNodes[i].second->lock_count<<std::endl;
            std::cout<<msg.str();
        }
     }

    std::stringstream msg;
    msg<<"CORRECTING"<<std::endl;
    std::cout<<msg.str();

    for(int i = 0; i < listOfAllNodes.size(); i++)
     {
        if(listOfAllNodes[i].second->lock_count)
        {
            while(listOfAllNodes[i].second->lock_count>0)
            {
                listOfAllNodes[i].second->lock_count--;
                listOfAllNodes[i].second->lmutex.unlock();

            }

            std::stringstream msg;
            msg<<"*************Lock Count for key" << listOfAllNodes[i].second->key<<":"<<listOfAllNodes[i].second->lock_count<<std::endl;
            std::cout<<msg.str();
        }
     }
#endif

    tx_status = COMMIT;
    txlog->tx_status = COMMIT;

    #if DEBUG_LOGS
    hash_table->printTable();
    #endif

    //release memory of local log
    //tryAbort(txlog);
   // txlog->ll[1].second->preds[0]->key;
    return tx_status;
}
