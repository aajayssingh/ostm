/*
*DESCRIPTION        :   file containing OSTM underlying table utility code
*AUTHOR             :   AJAY SINGH (M.TECH. Student)
*INSTITUTE          :   IIT Hyderabad
*DATE               :   Jan 2018
*/

#include "tablsl.h"

using namespace std; // for cout


/*
* DESCP:    hash table list node (table being closed addressed has rb-list.).
* AUTHOR:   Ajay Singh
*/
LinkedHashNode::LinkedHashNode(int key, int value)
{
    this->key = key;
    this->value = value;
    this->lock_count = 0;
    this->red_next = NULL;
    this->blue_next = NULL;
    this->marked = DEFAULT_MARKED;
    this->max_ts.look_ts = DEFAULT_TS; //use thread ID ot TX id
    this->max_ts.ins_ts = DEFAULT_TS; //use thread ID ot TX id
    this->max_ts.del_ts = DEFAULT_TS; //use thread ID ot TX id

}

/*
* DESCP:    hash table constructor init resources here.
* AUTHOR:   Ajay Singh
*/
HashMap::HashMap()
{
    htable = new LinkedHashNode* [TABLE_SIZE];
    for (int i = 0; i < TABLE_SIZE; i++)        /*init hash-tab with head and tail*/
    {
        htable[i] = NULL;
        htable[i] = new LinkedHashNode(INT_MIN, INT_MIN);
        htable[i]->red_next = new LinkedHashNode(INT_MAX, INT_MAX);
        htable[i]->blue_next = htable[i]->red_next;
    }
}

/*
* DESCP:    hash table destructor free dynamic resources here.
* AUTHOR:   Ajay Singh
*/
HashMap::~HashMap()
{
    for (int i = 0; i < TABLE_SIZE; i++)
    {
        if (htable[i] != NULL)
        {
            LinkedHashNode *prev = NULL;
            LinkedHashNode *entry = htable[i];
            while (entry != NULL)
            {
                prev = entry;
                entry = entry->red_next;
                delete prev;
            }
        }
        delete[] htable;
    }
}

/*
* DESCP:    cal table index.
* AUTHOR:   Ajay Singh
*/
int HashMap::HashFunc(int key)
{
    return key % TABLE_SIZE;
}

/*
* DESCP:    inserts a node into the rblist of the hash table.
* AUTHOR:   Ajay Singh
*/
void HashMap::lslIns(int key, int value, LinkedHashNode** preds, LinkedHashNode** currs, LIST_TYPE lst_type)
{
    if(RL_BL == lst_type)
    {
        currs[0]->blue_next = currs[1];
        preds[0]->blue_next = currs[0];
        currs[0]->marked = false;
        assert(key == currs[0]->key);
    }
    else if(RL == lst_type)
    {
        LinkedHashNode *node = new LinkedHashNode(key, value); //node inited and locked
        node->marked = true;

       // lock(node->mtx); //lock needed so that other might not travel this node and lock remeber to unlock in major mthod \& deadlock may occur if locked while creating the node if head
                            //is already locked it might request lock again.
        node->red_next = currs[0];
        preds[1]->red_next = node;
    }
    else
    {
        LinkedHashNode *node = new LinkedHashNode(key, value);

       // lock(node->mtx);   no need to take lock on node bcse no othe tx will be able to change it unless it takes lock obn both predn curr
        node->lmutex.lock();  //lock the node before inserting to the ds
        node->red_next = currs[0];
        node->blue_next = currs[1];
        preds[1]->red_next = node;
        preds[0]->blue_next = node;
    }
}


/*
* DESCP:    Search Element at a key. Key must strictly lie b\w head and tail that is INT_MIN and INT_MAX
* AUTHOR:   Ajay Singh
*/
//#ifdef DEBUG_LOGS
STATUS HashMap::lslSch(int obj_id, int key, int* value, LinkedHashNode** preds, LinkedHashNode** currs, VALIDATION_TYPE val_type, int tid)
/*preds and currs are arrays of size two with pointers to object LinkedHashNode
inside func we can access as  Foo* fooPtr = fooPtrArray[i]; and declare as in t_lookup, insert Foo* fooPtrArray[4];*/
{
    //cout<<"inside lslsch "<<endl;

    STATUS op_status = RETRY;
    LinkedHashNode *head = NULL;

    int bucket_num = HashFunc(key);  /*right now one has tab so this version of hasfunc is enough*/

    if (htable[bucket_num] == NULL) //if bucket is empty
    {
       elog("bucket is empty \n");
       return BUCKET_EMPTY;                      //verify probab if enum can take negative int?????????????
    }
    else
    {
        head = htable[bucket_num];
    }

    if(preds == NULL || currs == NULL)
    {
        elog("preds and currs is NULL \n");
        return VARIABLE_NULL;
    }

    if((key <= INT_MIN) && (key >= INT_MAX))
    {
        assert((key > INT_MIN) && (key < INT_MAX));
    }

    while(RETRY == op_status)
    {
        preds[0] = head;
        currs[1] = preds[0]->blue_next;

        while(currs[1]->key < key) //search blue pred and curr
        {
            preds[0] = currs[1];
            currs[1] = currs[1]->blue_next;
        }

        if(value)
        {
            *value = currs[1]->value;
        }

        preds[1] = preds[0];
        currs[0] = preds[0]->red_next;
        while(currs[0]->key < key) //search red pred and curr
        {
            preds[1] = currs[0];
            currs[0] = currs[0]->red_next;
        }

        preds[0]->lmutex.lock();
        preds[1]->lmutex.lock();

        currs[0]->lmutex.lock();
        currs[1]->lmutex.lock();


        preds[0]->lock_count++;
        preds[1]->lock_count++;
        currs[0]->lock_count++;
        currs[1]->lock_count++;

//        std::stringstream msg;
//        msg<<"[KEY:BUCKET:tid] entering Val" << key<<" "<<bucket_num<<" "<<tid<<endl;
//        msg<<"preds " <<preds[0]->key<<":"<<preds[1]->key<<endl;
//        msg <<" currs "<<currs[0]->key<<":"<<currs[1]->key<<endl;
//        cout<<msg.str();
    //validation
        op_status = validation(key, preds, currs, val_type, tid);

// NOTE (ajay#1#): Release lock for abort here only no need to do that seperately in lookup or delete. ...
//
        if(RETRY == op_status)
        {

            preds[0]->lock_count--;
            preds[1]->lock_count--;
            currs[0]->lock_count--;
            currs[1]->lock_count--;


            preds[0]->lmutex.unlock();
            preds[1]->lmutex.unlock();

            currs[0]->lmutex.unlock();
            currs[1]->lmutex.unlock();



        }
//        else
//        {
//            std::stringstream msg;
//            msg<<"[KEY:BUCKET:tid] completed Val" << key<<" "<<bucket_num<<" "<<tid<<endl;
//            msg<<"preds " <<preds[0]->key<<":"<<preds[1]->key<<endl;
//            msg <<" currs "<<currs[0]->key<<":"<<currs[1]->key<<endl;
//            cout<<msg.str();
//        }
       // cout<< "opstatus" <<op_status<<endl;
    }

    #if DEBUG_LOGS
    cout<< endl<<"lslsearch:: nodes " <<preds[0]->key<<" "<< preds[1]->key<<" "<<currs[0]->key<<" "<<currs[1]->key<<endl;
    #endif // DEBUG_LOGS
    return op_status;
}
//#endif // DEBUG_LOGS

/*
* DESCP:    Prints table with red & blue nodes both
* AUTHOR:   Ajay Singh
*/
void HashMap::printTable()
{
    int bucket_num = 0;//HashFunc(key);
    int red_node_count = 0; //count total red nodes in hashtable
    int blue_node_count = 0; //count total blue nodes in hashtable

    for(bucket_num = 0; bucket_num < TABLE_SIZE; bucket_num++)
    {
        if (htable[bucket_num] == NULL)
            return ;
        else
        {
            LinkedHashNode *entry = htable[bucket_num];
            while (entry != NULL)
            {
                //cout<< "(" << entry->key<<" - "<< entry->red_next->key<<") -->";
                //assert(entry->key < entry->red_next->key);

                //assert(entry->red_next != NULL);
                int k1 = entry->key;
                std::stringstream msg;
                msg<< "(" << entry->key<<" - "<< entry->marked/*entry */<<") -->";
                cout<<msg.str();
                (entry->marked)?red_node_count++: blue_node_count++;
                entry = entry->red_next;

                if (entry == NULL)
                {
                    blue_node_count -= 2; // to reduce the count for head & tail node as they are unmarked
                    cout<< endl <<endl;
                    break;
                }
                else{
                int k2 = entry->key;
                    assert(k1<k2);

                }
            }
        }
    }
    std::stringstream msg;
    msg <<"!!!!!!!!!!!!!!!!!!!!red node count : " <<red_node_count <<endl;
    msg <<"!!!!!!!!!!!!!!!!!!!!blue node count : " <<blue_node_count <<endl;
    cout<<msg.str();
}


/*
* DESCP:    Prints table with blue list alone
* AUTHOR:   Ajay Singh
*/
void HashMap::printBlueTable()
{
    int bucket_num = 0;//HashFunc(key);
    int node_count = 0;
    for(bucket_num = 0; bucket_num < TABLE_SIZE; bucket_num++)
    {
        if (htable[bucket_num] == NULL)
            return ;
        else
        {
            LinkedHashNode *entry = htable[bucket_num];
            while (entry != NULL)
            {
                cout<< "(" << entry->key<<" - "<< entry->marked <<") -->";
                node_count++;

                if(entry->marked)
                    assert(!entry->marked); //check if any blue node is not marked

                entry = entry->blue_next;

                if (entry == NULL)
                {
                    node_count -= 2; // to reduce the count for head & tail node
                    cout<< endl <<endl;
                }
            }
        }
    }
    cout <<"blue node count : " <<node_count <<endl;
    //elog("hari bol\n");
}

/*
* DESCP:    Delete Element at a key. In paper renamed to rblDel
* AUTHOR:   Ajay Singh
*/
void HashMap::lslDel(LinkedHashNode** preds, LinkedHashNode** currs)
{
    currs[1]->marked = true;
    preds[0]->blue_next = currs[1]->blue_next;
}


/*
* DESCP:    Method to identify any concurrent and conflicting modification. This method was later renamed to 
            methodValidation() in the paper.
* AUTHOR:   Ajay Singh
*/
STATUS interferenceValidation(LinkedHashNode** preds, LinkedHashNode** currs)
{
//    std::stringstream msg;
//    msg <<" preds0 currs1 "<<preds[0]->key<<":"<<currs[1]->key<<endl;
//    msg<<"preds0:preds0->bn:currs1 address" <<preds[0]<<":"<<preds[0]->blue_next<<":"<<currs[1]<<endl;
//    cout<<msg.str();

    if((preds[0]->marked) || (currs[1]->marked) || (preds[0]->blue_next != currs[1]) || (preds[1]->red_next != currs[0]))
    {
        return RETRY;
    }
    else
        return OK;
}

/*
* DESCP:    Method to identify any time order violation to enusre opacity. This method was later renamed to 
            transValidation() in the paper.
* AUTHOR:   Ajay Singh
*/
STATUS toValidation(int key, LinkedHashNode** currs, VALIDATION_TYPE val_type, int tid)
{
    //int tid = 0; //get thread or tx id part of stub code
    STATUS op_status = OK;

    LinkedHashNode* curr = NULL;

    //getaptCurr
    if(key == currs[1]->key)
        curr = currs[1];
    else if(key == currs[0]->key)
        curr = currs[0];
   #if DEBUG_LOGS
    else
        cout <<"getAptCurr has null curr"<<endl;
   #endif // DEBUG_LOGS
// NOTE (ajay#1#): Sanity check for Default Time stamp of the node. ...
//Check if TS of node is not default.

    if((NULL != curr) && (key == curr->key))
    {
        if((RV == val_type) && ((tid < curr->max_ts.ins_ts) || (tid < curr->max_ts.del_ts) ) )
        {
            op_status = ABORT;
        }
        else if( (tid < curr->max_ts.ins_ts) || (tid < curr->max_ts.del_ts) || (tid < curr->max_ts.look_ts) )
        {
            op_status = ABORT;
        }
        else
        {
            op_status = OK;
        }
    }

    return op_status;

}

/*
* DESCP:    Method to to invoke 2way validations.
* AUTHOR:   Ajay Singh
*/
STATUS validation(int key, LinkedHashNode** preds, LinkedHashNode** currs, VALIDATION_TYPE val_type, int tid)
{
//    std::stringstream msg;
//    msg<<"[KEY:tid] entering INTF" << key<<" "<<tid<<endl;
//    msg<<"preds " <<preds[0]->key<<":"<<preds[1]->key<<endl;
//    msg <<" currs "<<currs[0]->key<<":"<<currs[1]->key<<endl;
//    cout<<msg.str();

    STATUS op_status = interferenceValidation(preds, currs);

    if(RETRY != op_status)
    {
        op_status = toValidation(key, currs, val_type, tid);
//        std::stringstream msg;
//        msg<<"tid "<<tid <<"succ intrfV"<<std::endl;
//        std::cout<<msg.str();
    }

    return op_status;
}

/*TEST functionality of regular Hash Map serially*/
int HashMap::lslSearch(int key)
{
    int bucket_num = HashFunc(key);
    if (htable[bucket_num] == NULL)
        return -1;
    else
    {
        LinkedHashNode *entry = htable[bucket_num];
        while (entry != NULL && entry->key != key)
            entry = entry->red_next;
        if (entry == NULL)
            return -1;
        else
            return entry->value;
    }
}

/*
* DESCP:    serial utility.
* AUTHOR:   Ajay Singh
*/
void HashMap::lslInsert(int key, int value)
{
    int bucket_num = HashFunc(key);
    if (htable[bucket_num] == NULL)
        htable[bucket_num] = new LinkedHashNode(key, value);
    else
    {
        LinkedHashNode *entry = htable[bucket_num];
        while (entry->red_next != NULL)
            entry = entry->red_next;
            if (entry->key == key)
                entry->value = value;
            else
                entry->red_next = new LinkedHashNode(key, value);
    }
}

/*
* DESCP:    serial utility.
* AUTHOR:   Ajay Singh
*/
void HashMap::lslDelete(int key)
{
    int bucket_num = HashFunc(key);
    if (htable[bucket_num] != NULL)
    {
        LinkedHashNode *entry = htable[bucket_num];
        LinkedHashNode *prev = NULL;
        while (entry->red_next != NULL && entry->key != key)
        {
            prev = entry;
            entry = entry->red_next;
        }
        if (entry->key == key)
        {
            if (prev == NULL)
            {
                LinkedHashNode *red_next = entry->red_next;
                delete entry;
                htable[bucket_num] = red_next;
            }
            else
            {
                LinkedHashNode *red_next = entry->red_next;
                delete entry;
                prev->red_next = red_next;
            }
        }
    }
}
