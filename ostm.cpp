#if 1
#include "ostm.h"

trans_log:: trans_log(int tid)
{

    //init tid
    this->tid = tid;

  //debug default init of local log
  #ifdef DEBUG
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

/*creates local entry */
int trans_log:: createLLentry(int key)
{
        ll_entry* log_entry = new ll_entry;
        log_entry->key = DEFAULT_KEY;
        log_entry->value = DEFAULT_VALUE;
        log_entry->opn = DEFAULT_OPN_NAME;
        log_entry->preds = new LinkedHashNode*[2];
        log_entry->currs = new LinkedHashNode*[2];
        log_entry->op_status = DEFAULT_OP_STATUS;

        this->ll.push_back(std::make_pair(key, log_entry));

        return (this->ll.size() - 1);
}

void trans_log::setPredsnCurrs(int pos, LinkedHashNode** preds, LinkedHashNode** currs)
{
    this->ll[pos].second->preds[0] = preds[0];
    this->ll[pos].second->preds[1] = preds[1];
    this->ll[pos].second->currs[0] = currs[0];
    this->ll[pos].second->currs[1] = currs[1];
}

void trans_log::setOpnName(int pos, OPN_NAME opn)
{
    this->ll[pos].second->opn = opn;
}

void trans_log::setOpStatus(int pos, STATUS op_status)
{
    this->ll[pos].second->op_status = op_status;
}

void trans_log::setValue(int pos, int value)
{
    this->ll[pos].second->value = value;
}

void trans_log::setKey(int pos, int key)
{
    this->ll[pos].second->key = key;
}

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
            return i;;
        }
    }

    return BAD_INDEX;
}

OPN_NAME trans_log::getOpn(int ll_pos)
{
    if(this->ll[ll_pos].second)
        return this->ll[ll_pos].second->opn;
    else{
        std::cout <<"ll[ll_pos].second is  NULL"<<std::endl;
        return WRONG_OPN;
    }
}

int trans_log::getValue(int ll_pos)
{
    if(this->ll[ll_pos].second)
        return this->ll[ll_pos].second->value;
    else{
        std::cout <<"ll[ll_pos].second is  NULL"<<std::endl;
        return BAD_VALUE;
    }
}

int trans_log::getKey(int ll_pos)
{
    if(this->ll[ll_pos].second)
        return this->ll[ll_pos].second->key;
    else{
        std::cout <<"ll[ll_pos].second is  NULL"<<std::endl;
        return BAD_VALUE;
    }
}

STATUS trans_log::getOpStatus(int ll_pos)
{

    if(this->ll[ll_pos].second)
        return this->ll[ll_pos].second->op_status;
    else{
        std::cout <<"ll[ll_pos].second is  NULL"<<std::endl;
        return WRONG_STATUS;
    }
}

OSTM::OSTM()
{
    hash_table = new HashMap();                /*shared memory*/ //will init hashtablle as in main.cpp
}

OSTM::~OSTM()
{
//int hello;
}

trans_log* OSTM::begin()
{
    //init the transaction --> tid and local log
    int tid = tid_counter++;  //it is atomic var
    trans_log* txlog = new trans_log(tid);
    return txlog;
}

/*Lookup*/
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

    //temp debug
//    if(key == 6)
//    {
//        op_status = ABORT;
//    }


    if(ABORT == op_status)
    {
        tryAbort(txlog);
        std::cout<<"t_lookup::ABORT returned "<<std::endl;

        unlock(preds[0]->mtx);
        if(preds[0] != preds[1])
            unlock(preds[1]->mtx);

        unlock(currs[0]->mtx);
        if(currs[0] != currs[1])
            unlock(currs[1]->mtx);

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

    std::cout<<"t_lookup:: lslsearch returned preds "<<preds[0]->key<<" "<<preds[1]->key<<std::endl;
    std::cout<<"t_lookup:: lslsearch returned currs "<<currs[0]->key<<" "<<currs[1]->key<<std::endl;
    std::cout<<"t_lookup:: lslsearch returned tid "<<txlog->tid<<std::endl;

    //create ll_entry and log all new values for subsequent ops
    int ll_pos = txlog->createLLentry(key);
    txlog->setPredsnCurrs(ll_pos, preds, currs);
    txlog->setOpnName(ll_pos, LOOKUP);
    txlog->setValue(ll_pos, *value);
    txlog->setKey(ll_pos, key);
    txlog->setOpStatus(ll_pos, op_status);

//    unlock(preds[0]->mtx);
//    unlock(preds[1]->mtx);
//    //unlock(preds[1]->red_next->mtx);
//    unlock(currs[0]->mtx);
//    unlock(currs[1]->mtx);


    unlock(preds[0]->mtx);
    if(preds[0] != preds[1])
        unlock(preds[1]->mtx);

    unlock(currs[0]->mtx);
    if(currs[0] != currs[1])
        unlock(currs[1]->mtx);
}

//check if lslSearch works fine
//hash_table->lslSearch(0);

return op_status;
}


/*Delete*/
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

    //temp debug
//    if(key == 6)
//    {
//        op_status = ABORT;
//    }


    if(ABORT == op_status)
    {
        tryAbort(txlog);
        std::cout<<"t_lookup::ABORT returned "<<std::endl;

        unlock(preds[0]->mtx);
        if(preds[0] != preds[1])
            unlock(preds[1]->mtx);

        unlock(currs[0]->mtx);
        if(currs[0] != currs[1])
            unlock(currs[1]->mtx);

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
            preds[1]->red_next->max_ts.look_ts = txlog->tid; //setting ts of new created node
            *value = BAD_VALUE;
        }
    }

    #ifdef DEBUG
    std::cout<<"t_delete:: lslsearch returned preds "<<preds[0]->key<<" "<<preds[1]->key<<std::endl;
    std::cout<<"t_delete:: lslsearch returned currs "<<currs[0]->key<<" "<<currs[1]->key<<std::endl;
    std::cout<<"t_delete:: lslsearch returned tid "<<txlog->tid<<std::endl;
#endif // DEBUG

    //create ll_entry and log all new values for subsequent ops
    int ll_pos = txlog->createLLentry(key);
    txlog->setPredsnCurrs(ll_pos, preds, currs);
    txlog->setOpnName(ll_pos, DELETE);
    txlog->setValue(ll_pos, *value);
    txlog->setKey(ll_pos, key);
    txlog->setOpStatus(ll_pos, op_status);

//    unlock(preds[0]->mtx);
//    unlock(preds[1]->mtx);
//    //unlock(preds[1]->red_next->mtx);
//    unlock(currs[0]->mtx);
//    unlock(currs[1]->mtx);


    unlock(preds[0]->mtx);
    if(preds[0] != preds[1])
        unlock(preds[1]->mtx);

    unlock(currs[0]->mtx);
    if(currs[0] != currs[1])
        unlock(currs[1]->mtx);
}

//check if lslSearch works fine
//hash_table->lslSearch(0);

return op_status;
}


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
    else
    {
     //create ll_entry and log all new values for subsequent ops
        int ll_pos = txlog->createLLentry(key);
     //   txlog->setPredsnCurrs(ll_pos, preds, currs);
        txlog->setOpnName(ll_pos, INSERT);
        txlog->setValue(ll_pos, value);
        txlog->setKey(ll_pos, key);
        txlog->setOpStatus(ll_pos, OK);
    }
    return OK;
}


STATUS OSTM::tryAbort(trans_log* txlog)
{
    //causes abort at po validation if release mem here
    std::cout<<"abort called"<<std::endl;
    //txlog->~trans_log();

}

STATUS OSTM::tryCommit(trans_log* txlog)
{
    STATUS tx_status = ABORT;
    STATUS op_status = DEFAULT_OP_STATUS;
    int tid = txlog->tid;
    //STATUS op_status = RETRY;
    sort(txlog->ll.begin(), txlog->ll.end());  //use sorted list now on as irrelevant the order of opn due to locks

{
    LinkedHashNode* preds[2];
    LinkedHashNode* currs[2];
//  int  *value = BAD_VALUE;

    for(int i = 0; i < txlog->ll.size(); i++)
    {
        //get key and obj id
        int key = txlog->ll[i].first;
        int obj_id = txlog->ll[i].second->obj_id;
        op_status = hash_table->lslSch(obj_id, key, NULL, preds, currs, TRYCOMMIT, tid); //??test value is null

        txlog->setPredsnCurrs(i, preds, currs);

        if(ABORT == op_status)
        {
            //release locks and memory
            std::cout<<"trycommit::abort called"<<std::endl;
            tryAbort(txlog);//delete all dynamic allocation of transaction

            for(int j = 0; j <= i; j++)
            {
                //        unlock(txlog->ll[i].second->preds[0]->mtx);
                //        unlock(txlog->ll[i].second->preds[1]->mtx);
                //        unlock(txlog->ll[i].second->currs[0]->mtx);
                //        unlock(txlog->ll[i].second->currs[1]->mtx);

                unlock(txlog->ll[j].second->preds[0]->mtx);
                if(txlog->ll[j].second->preds[0] != txlog->ll[j].second->preds[1])
                unlock(txlog->ll[j].second->preds[1]->mtx);

                unlock(txlog->ll[j].second->currs[0]->mtx);
                if(txlog->ll[j].second->currs[0] != txlog->ll[j].second->currs[1])
                unlock(txlog->ll[j].second->currs[1]->mtx);
            }

// unlock(preds[0]->mtx);
//    if(preds[0] != preds[1])
//        unlock(preds[1]->mtx);
//
//    unlock(currs[0]->mtx);
//    if(currs[0] != currs[1])
//        unlock(currs[1]->mtx);

            std::cout<<"tryCommit:: lslsearch returned preds "<<preds[0]->key<<" "<<preds[1]->key<<std::endl;
        std::cout<<"tryCommit:: lslsearch returned currs "<<currs[0]->key<<" "<<currs[1]->key<<std::endl;
        std::cout<<"tryCommit:: lslsearch returned tid "<<txlog->tid<<std::endl;

            return ABORT;
        }

      //  txlog->setPredsnCurrs(i, preds, currs);

#if DEBUG
        std::cout<<"tryCommit:: lslsearch returned preds "<<preds[0]->key<<" "<<preds[1]->key<<std::endl;
        std::cout<<"tryCommit:: lslsearch returned currs "<<currs[0]->key<<" "<<currs[1]->key<<std::endl;
        std::cout<<"tryCommit:: lslsearch returned tid "<<txlog->tid<<std::endl;
#endif // DEBUG
    }
}

    for(int i = 0; i < txlog->ll.size(); i++)
    {
        //get key and obj id
        int key = txlog->ll[i].first;
        int obj_id = txlog->ll[i].second->obj_id;
        OPN_NAME opn = txlog->ll[i].second->opn;


        //poval
       /* if((txlog->ll[i].second->preds[0]->marked) || (txlog->ll[i].second->preds[0]->blue_next != txlog->ll[i].second->currs[1]))
        {
            if(INSERT == txlog->ll[i-1].second->opn)
            {
                if(txlog->ll[i-1].second->preds[0]->blue_next->key == txlog->ll[i-1].second->key ) //check to verify correct pred is assigned
                    txlog->ll[i].second->preds[0] = txlog->ll[i-1].second->preds[0]->blue_next;
            }
            else{
                txlog->ll[i].second->preds[0] = txlog->ll[i-1].second->preds[0];
            }
        }

        if(txlog->ll[i].second->preds[1]->red_next != txlog->ll[i].second->currs[0])
        {
            txlog->ll[i-1].second->key;
            if(txlog->ll[i-1].second->preds[1]->red_next->key == txlog->ll[i-1].second->key )
                txlog->ll[i].second->preds[1] = txlog->ll[i-1].second->preds[1]->red_next;
        }

*/



        if(INSERT == opn)
        {

            //fetch value from local log
            int value = txlog->getValue(i);//txlog->ll[i].second->value; //? test if value is bad value or use get value---txlog->getValue(i);

            if(key == txlog->ll[i].second->currs[1]->key)
            {
                //fetch value from local log
                //int value = txlog->getValue(i);//txlog->ll[i].second->value; //? test if value is bad value or use get value---txlog->getValue(i);

                //set value to underlying table node
                txlog->ll[i].second->currs[1]->value = value;
                txlog->ll[i].second->currs[1]->max_ts.ins_ts = txlog->tid;
                txlog->setOpStatus(i, OK);
            }
            else if(key == txlog->ll[i].second->currs[0]->key)
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

                txlog->ll[i].second->preds[0]->blue_next->max_ts.ins_ts = txlog->tid;
                txlog->setOpStatus(i, OK);
            }

        }
        else if(DELETE == opn)
        {
            if(key == txlog->ll[i].second->currs[1]->key)
            {
                hash_table->lslDel(txlog->ll[i].second->preds, txlog->ll[i].second->currs);
                txlog->ll[i].second->currs[1]->max_ts.del_ts = txlog->tid;

                txlog->setOpStatus(i, OK);
            }
            else
            {
                txlog->ll[i].second->currs[0]->max_ts.del_ts = txlog->tid;

                txlog->setOpStatus(i, FAIL);
            }
        }

    }

    //release locks code
     for(int i = 0; i < txlog->ll.size(); i++)
     {
//        unlock(txlog->ll[i].second->preds[0]->mtx);
//        unlock(txlog->ll[i].second->preds[1]->mtx);
//        unlock(txlog->ll[i].second->currs[0]->mtx);
//        unlock(txlog->ll[i].second->currs[1]->mtx);

        unlock(txlog->ll[i].second->preds[0]->mtx);
        if(txlog->ll[i].second->preds[0] != txlog->ll[i].second->preds[1])
            unlock(txlog->ll[i].second->preds[1]->mtx);

        unlock(txlog->ll[i].second->currs[0]->mtx);
        if(txlog->ll[i].second->currs[0] != txlog->ll[i].second->currs[1])
            unlock(txlog->ll[i].second->currs[1]->mtx);
     }


    tx_status = COMMIT;
    txlog->tx_status = COMMIT;

    //release memory of local log
    //tryAbort(txlog);
   // txlog->ll[1].second->preds[0]->key;
    return tx_status;
}


#endif
