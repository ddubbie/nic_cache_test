#ifndef __COMPLETE_BIN_TREE_H__
#define __COMPLETE_BIN_TREE_H__

#include "hashtable.h"

void complete_bin_tree_insert(kv_hashtable_item_t *it);

void complete_bin_tree_delete(kv_hashtable_item_t *it);

kv_hashtable_item_t * complete_bin_tree_get_random_item(void);

#endif
