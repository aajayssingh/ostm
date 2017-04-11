#ifndef TABLSL_H
#define TABLSL_H
#include <pthread.h>
#include "common.h"
#include <climits>
#include <assert.h>


#define lock(_mtx_) pthread_mutex_lock(&_mtx_)
#define unlock(_mtx_) pthread_mutex_unlock(&_mtx_)


/*
 * Link List Class Declaration
 */


class LinkedHashNode
{
	public:
        int key, value;
        bool marked;

        struct
        {
            int look_ts;
            int ins_ts;
            int del_ts;
        } max_ts;               /*max_ts DS*/

        pthread_mutex_t mtx = PTHREAD_ERRORCHECK_MUTEX_INITIALIZER_NP;    /*lock*/

        LinkedHashNode *red_next;   /*next red node*/
        LinkedHashNode *blue_next;  /*next blue node*/

        LinkedHashNode(int key, int value); /*init the node with key, value*/

};
/*
 * HashMap Class Declaration
 */
class HashMap
{
    private:
        LinkedHashNode **htable;
    public:
        HashMap();
        ~HashMap();
        /*
	     * Hash Function
         */
        int HashFunc(int key);
	   /*
	     * Insert Element at a key
         */
        void lslInsert(int key, int value);
        void lslIns(int key, int value, LinkedHashNode** preds, LinkedHashNode** currs, LIST_TYPE lst_type);
        /*
	     * Search Element at a key
         */
        int lslSearch(int key);
        STATUS lslSch(int obj_id, int key, int* value, LinkedHashNode** preds, LinkedHashNode** currs, VALIDATION_TYPE val_type, int tid);
        /*
	     * Delete Element at a key
         */
        void lslDelete(int key);
        void lslDel(LinkedHashNode** preds, LinkedHashNode** currs);

        void printTable();
        void printBlueTable();

};


STATUS interferenceValidation(LinkedHashNode** preds, LinkedHashNode** currs);

STATUS toValidation(int key, LinkedHashNode** currs, VALIDATION_TYPE val_type, int tid);

STATUS validation(int key, LinkedHashNode** preds, LinkedHashNode** currs, VALIDATION_TYPE val_type, int tid);


#endif //TABLSL_H
