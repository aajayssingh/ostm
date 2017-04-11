#include "tablsl.h"

using namespace std; // for cout


LinkedHashNode::LinkedHashNode(int key, int value)
{
    this->key = key;
    this->value = value;
    this->red_next = NULL;
    this->blue_next = NULL;
    this->marked = DEFAULT_MARKED;
    this->max_ts.look_ts = DEFAULT_TS; //use thread ID ot TX id
    this->max_ts.ins_ts = DEFAULT_TS; //use thread ID ot TX id
    this->max_ts.del_ts = DEFAULT_TS; //use thread ID ot TX id

}

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

int HashMap::HashFunc(int key)
{
    return key % TABLE_SIZE;
}

/*
 * Insert Element at a key
 */
void HashMap::lslIns(int key, int value, LinkedHashNode** preds, LinkedHashNode** currs, LIST_TYPE lst_type)
{
    if(RL_BL == lst_type)
    {
        currs[0]->blue_next = currs[1];
        preds[0]->blue_next = currs[0];
        currs[0]->marked = false;
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

        node->red_next = currs[0];
        node->blue_next = currs[1];
        preds[1]->red_next = node;
        preds[0]->blue_next = node;
    }
}

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
 * Search Element at a key
 key must strictly lie b\w head and tail that is INT_MIN and INT_MAX
 */
//#ifdef DEBUG
STATUS HashMap::lslSch(int obj_id, int key, int* value, LinkedHashNode** preds, LinkedHashNode** currs, VALIDATION_TYPE val_type, int tid) /*preds and currs are arrays of size two with pointers to object LinkedHashNode
inside func we can access as  Foo* fooPtr = fooPtrArray[i]; and declare as in t_lookup, insert Foo* fooPtrArray[4];*/
{
    //cout<<"inside lslsch "<<endl;

    STATUS op_status = RETRY;
    LinkedHashNode *head = NULL;

    int bucket_num = HashFunc(key);  /*right now one has tab so this version of hasfunc is enough*/
    if (htable[bucket_num] == NULL) //if bucket is empty
        return BUCKET_EMPTY;                      //verify probab if enum can take negative int?????????????
    else
    {
        head = htable[bucket_num];
//        while (entry != NULL && entry->key != key)
//            entry = entry->red_next;
//        if (entry == NULL)
//            return -1;
//        else
//            return entry->value;
    }

    if(preds == NULL || currs == NULL)
    {
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

        while(currs[1]->key < key)
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
        while(currs[0]->key < key)
        {
            preds[1] = currs[0];
            currs[0] = currs[0]->red_next;
        }

        lock(preds[0]->mtx);
        if(preds[0] != preds[1])
            lock(preds[1]->mtx);

        lock(currs[0]->mtx);
        if(currs[0] != currs[1])
            lock(currs[1]->mtx);

        //validation
        op_status = validation(key, preds, currs, val_type, tid);

        if(RETRY == op_status)
        {
            unlock(preds[0]->mtx);
            if(preds[0] != preds[1])
                unlock(preds[1]->mtx);

            unlock(currs[0]->mtx);
            if(currs[0] != currs[1])
                unlock(currs[1]->mtx);
        }
       // cout<< "opstatus" <<op_status<<endl;
    }

    #ifdef DEBUG
    cout<< endl<<"lslsearch:: nodes " <<preds[0]->key<<" "<< preds[1]->key<<" "<<currs[0]->key<<" "<<currs[1]->key<<endl;
    #endif // DEBUG
    return op_status;
}
//#endif // DEBUG

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


void HashMap::printTable()
{
    int bucket_num = 0;//HashFunc(key);

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
                entry = entry->red_next;
            }

            if (entry == NULL)
                cout<< endl;
        }
    }

}

void HashMap::printBlueTable()
{
    int bucket_num = 0;//HashFunc(key);

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
                entry = entry->blue_next;
            }

            if (entry == NULL)
                cout<< endl;
        }
    }

}
/*
 * Delete Element at a key
 */
void HashMap::lslDel(LinkedHashNode** preds, LinkedHashNode** currs)
{
    currs[1]->marked = true;
    preds[0]->blue_next = currs[1]->blue_next;
}

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



/*
Validation
*/

STATUS interferenceValidation(LinkedHashNode** preds, LinkedHashNode** currs)
{
    if((preds[0]->marked) && (currs[1]->marked) && (preds[0]->blue_next != currs[1]) && preds[1]->red_next != currs[0])
        return RETRY;
    else
        return OK;
}

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
   #ifdef DEBUG
    else
        cout <<"getAptCurr has null curr"<<endl;
   #endif // DEBUG

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

STATUS validation(int key, LinkedHashNode** preds, LinkedHashNode** currs, VALIDATION_TYPE val_type, int tid)
{
    STATUS op_status = interferenceValidation(preds, currs);

    if(RETRY != op_status)
    {
        op_status = toValidation(key, currs, val_type, tid);
    }

    return op_status;
}

