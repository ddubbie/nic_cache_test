#include "complete_bin_tree.h"
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <assert.h>
#include <math.h>

static uint64_t n_items = 0;
static pthread_mutex_t _treeLock = PTHREAD_MUTEX_INITIALIZER;
static kv_hashtable_item_t _sentinel;
static kv_hashtable_item_t *root = &_sentinel;

inline static void InsertItem(kv_hashtable_item_t *it);
inline static void DeleteItem(kv_hashtable_item_t *it);
inline static kv_hashtable_item_t *SearchItem(const uint64_t seq);

inline static void
InsertItem(kv_hashtable_item_t *it) {

    if (root == &_sentinel) {
        root = it;
        it->_parent = root;
    } else {
        int i;
        uint64_t next_seq = n_items + 1;
        uint64_t n_edges = (uint64_t)log2(next_seq);
        kv_hashtable_item_t  *temp = root;

        for (i = n_edges - 1; i > 0; i--) {
            assert(temp);
            if (next_seq & (uint64_t)(1 << i)) {
                temp = temp->_right;
            } else {
                temp = temp->_left;
            }
        }

        if (next_seq & (uint64_t)1) {
            temp->_right = it;
        } else {
            temp->_left = it;
        }

        it->_parent = temp;
    }

    it->_left = &_sentinel;
    it->_right = &_sentinel;

    n_items++;

    //printf("inset item %lu\n", n_items);
}

inline static void
DeleteItem(kv_hashtable_item_t *it) {

    kv_hashtable_item_t *succ = SearchItem(n_items);
    assert(succ);

    if (it == root) {
        if (succ == it) {
            assert(n_items == 1);
            root = &_sentinel;
        } else {

            if (succ->_parent->_left == succ) {
                succ->_parent->_left = &_sentinel;
            } else {
                succ->_parent->_right = &_sentinel;
            }

            root = succ;
            succ->_left = it->_left;
            succ->_right = it->_right;
            succ->_parent = &_sentinel;

            succ->_left->_parent = succ;
            succ->_right->_parent = succ;
        }

    } else {
        if (succ == it) {
            if (it->_parent->_left == it) {
                it->_parent->_left = &_sentinel;
            } else {
                it->_parent->_right = &_sentinel;
            }

        } else {
            if (succ->_parent->_left == succ) {
                succ->_parent->_left = &_sentinel;
            } else {
                succ->_parent->_right = &_sentinel;
            }

            if (it->_parent->_left == it) {
                it->_parent->_left = succ;
            } else {
                it->_parent->_right = succ;
            }

            succ->_left = it->_left;
            succ->_right = it->_right;
            succ->_parent = it->_parent;

            succ->_left->_parent = succ;
            succ->_right->_parent = succ;
        }
    }

    //printf("delete item %lu\n", n_items);

    it->_left = NULL;
    it->_right = NULL;
    it->_parent = NULL;
    n_items--;
}

inline static kv_hashtable_item_t *
SearchItem(const uint64_t seq) {

    kv_hashtable_item_t *it = root;
    if (seq == 1) return it;

    int i;
    uint64_t n_edges = (uint64_t)log2(seq);

    for (i = n_edges - 1; i >= 0; i--) {
        if (seq & (uint64_t)(1 << i)) {
            it = it->_right;
        } else {
            it = it->_left;
        }
    }

    return it;
}

void 
complete_bin_tree_insert(kv_hashtable_item_t *it) {

    pthread_mutex_lock(&_treeLock);
    InsertItem(it);
    pthread_mutex_unlock(&_treeLock);
}

void 
complete_bin_tree_delete(kv_hashtable_item_t *it) {

    pthread_mutex_lock(&_treeLock);
    DeleteItem(it);
    pthread_mutex_unlock(&_treeLock);

}

kv_hashtable_item_t *
complete_bin_tree_get_random_item(void) {

    kv_hashtable_item_t *it = NULL;
    pthread_mutex_lock(&_treeLock);

    assert(n_items > 0);

    do {
        uint64_t seq = rand() % n_items;  
        it = SearchItem(seq);
    } while (!__atomic_load_n(&it->active, __ATOMIC_RELAXED));

    pthread_mutex_unlock(&_treeLock);

    return it;
}
