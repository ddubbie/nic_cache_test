#include "hashtable.h"
#include "complete_bin_tree.h"
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/time.h>
#include <string.h>
#include <errno.h>

#define log_error(f, m...) do{\
    fprintf(stderr, "[%10s:%10s:%4d] " f, __FILE__, __FUNCTION__, __LINE__, ##m);\
}while(0);

#define trace_log(f, m...) do{\
    fprintf(stdout, "[TRACE_LOG][%10s:%10s:%4d] " f, __FILE__, __FUNCTION__, __LINE__, ##m);\
}while(0);


#define GET_BUCKET_IDX(_hash_val) (_hash_val & table.hash_mask)
#define MEMORY_LIMITATION   (uint64_t)(24 *(1LU << 30))

extern uint64_t GetRandomInterArrivalTime(void);

static bool is_hashtable_setup = false;
static kv_hashtable_t table;
static uint64_t totalItem = 0;
static bool teardown = false;

#ifdef _DEBUG_LOG
static FILE *hashtable_log = NULL;
#endif
static uint64_t totalUsedMemory = 0;

inline static kv_hashtable_item_t *
GetItem(void *key, uint16_t key_len) {

	uint64_t hash_val = CAL_HASH_VAL(key, key_len);
	uint16_t hash_tag = GET_TAG(hash_val);
	uint32_t hash_idx = GET_BUCKET_IDX(hash_val);

	kv_hashtable_bucket_t *b = &table.bucket[hash_idx];	
	kv_hashtable_item_t *item;
	
	TAILQ_FOREACH(item, b->chain, link) {
		if (item->tag == hash_tag)
			return item;
	}
	return NULL;
}

inline static kv_hashtable_item_t *
CreateHashTableItem(void *key, void *value, uint16_t key_len, uint32_t value_len, 
        uint16_t tag, uint64_t hv) {

	kv_hashtable_item_t *item;

	item = malloc(sizeof(kv_hashtable_item_t));
	if(!item) {
		log_error("malloc error()\n");
		return NULL;
	}
    item->key_len = key_len;
    item->value_len = value_len;
    item->tag = tag;
    item->refCount = 0;
    item->n_requests = 0;
    item->hv = hv;

    item->data = malloc(key_len + value_len);
    if (!item->data) {
        free(item);
        return NULL;
    }

    __atomic_store_n(&item->active, 1, __ATOMIC_RELAXED);

    memcpy(item->data, key, key_len);
    memcpy(item->data + key_len, value, value_len);

    __atomic_fetch_add(&totalUsedMemory, 
            key_len + value_len + sizeof(kv_hashtable_item_t), __ATOMIC_RELAXED);


#ifdef _DEBUG_LOG
    fprintf(hashtable_log, "Create item, mem_usage:%lu, n_items:%lu\n",
            totalUsedMemory, totalItem);
#endif

	return item;
}

inline static void
DestroyHashTableItem(kv_hashtable_item_t **item) {

    __atomic_store_n(&(*item)->active, 0, __ATOMIC_RELAXED);

    if (teardown)
        goto TeardownHashTable;

    do {
        usleep(10);
    } while ((*item)->refCount > 0 && !teardown);


    __atomic_fetch_sub(&totalUsedMemory, 
            (*item)->key_len + (*item)->value_len + sizeof(kv_hashtable_bucket_t), __ATOMIC_RELAXED);

#ifdef _DEBUG_LOG
    fprintf(hashtable_log, "Destroy item, mem_usage:%lu, n_items:%lu\n",
            totalUsedMemory, totalItem);
#endif

TeardownHashTable :
    free((*item)->data);
    free(*item);
    *item = NULL;
}

inline static void
CreateHashTableBucket(kv_hashtable_bucket_t *b) {

#if _USE_SPINLOCK	
	b->lock = malloc(sizeof(pthread_spinlock_t));
#else
	b->lock = malloc(sizeof(pthread_mutex_t));
#endif
	if(!b->lock) {
		log_error("malloc error()\n");
		exit(EXIT_FAILURE);
	}
	
	LOCK_INIT(b->lock);

	b->chain = malloc(sizeof(kv_hashtable_chain_t));
	if(!b->chain) {
		log_error("malloc error()\n");
		exit(EXIT_FAILURE);
	}

	TAILQ_INIT(b->chain);
}

inline static void
DestroyHashTableBucket(kv_hashtable_bucket_t *b) {

	kv_hashtable_item_t *p_cur, *p_next;
	
	p_cur = TAILQ_FIRST(b->chain);
	while(p_cur) {
		p_next = TAILQ_NEXT(p_cur, link);
		DestroyHashTableItem(&p_cur);
		p_cur = p_next;
	}

	free(b->chain);
	free(b->lock);
}

void
hashtable_setup(const uint16_t hash_power) {

	int i, ret;

	if (is_hashtable_setup) {
		log_error("hashtable has already been setupt\n");
		return;
	}

	table.hash_table_size = (1U << hash_power);
	table.hash_mask = table.hash_table_size - 1;

	ret = posix_memalign((void **)&table.bucket, 
			table.hash_table_size, table.hash_table_size * sizeof(kv_hashtable_bucket_t));

	if (ret < 0) {
		log_error("malloc error()\n");
		exit(EXIT_FAILURE);
	}

	for (i = 0; i < table.hash_table_size; i++)
		CreateHashTableBucket(&table.bucket[i]);


	is_hashtable_setup = true;

#ifdef _DEBUG_LOG

    hashtable_log = fopen("log/test_hashtable.log", "w");
    if (!hashtable_log) {
        int err = errno;
        log_error("%s\n", strerror(err));
        hashtable_teardown();
        return;
    }

#endif
    trace_log("hashtable setup completes\n");
}

void 
hashtable_teardown(void) {
	int i;	

	if (!is_hashtable_setup) {
		log_error("hashtable is not setup yet\n");
		return;
	}

    teardown = true;

	for (i = 0; i < table.hash_table_size; i++) 
		DestroyHashTableBucket(&table.bucket[i]);

	free(table.bucket);

#ifdef _DEBUG_LOG
    if (hashtable_log) {
        fclose(hashtable_log);
    }
    trace_log("remained item in hash table : %lu\n", totalItem);
#endif
    trace_log("hashtable teardown\n");
}

kv_hashtable_item_t *
hashtable_start_to_access(void *key, const uint16_t key_len) {

    kv_hashtable_item_t *it = GetItem(key, key_len);

    if (!__atomic_load_n(&it->active, __ATOMIC_RELAXED))
        return NULL;

    __atomic_fetch_add(&it->refCount, 1, __ATOMIC_RELAXED);

    return it;
}

void
hashtable_start_to_access_directly(kv_hashtable_item_t *it) {
    __atomic_fetch_add(&it->refCount, 1, __ATOMIC_RELAXED);
}

kv_hashtable_item_t *
hashtable_put(void *key, const uint16_t key_len, void *value, const uint32_t value_len, uint16_t *flags) {

	uint64_t hash_val = CAL_HASH_VAL(key, key_len);
	uint16_t tag = GET_TAG(hash_val);
	uint32_t bucket_idx = GET_BUCKET_IDX(hash_val);
	
	LOCK(table.bucket[bucket_idx].lock);
	kv_hashtable_item_t *item = GetItem(key, key_len);
    *flags = 0;

	if (!item) {

        if (totalUsedMemory + sizeof(kv_hashtable_item_t) + 
                key_len + value_len >= MEMORY_LIMITATION)
        {
#ifdef _DEBUG_LOG
            fprintf(hashtable_log, "[Memory limitation], put fail\n");
#endif
            *flags |= HASHTABLE_FLAGS_PUT_NEW_ITEM_FAIL_MEM_LIMIT;
		    UNLOCK(table.bucket[bucket_idx].lock);
            return NULL;
        }

		kv_hashtable_item_t *new_item = CreateHashTableItem(key, value, key_len, value_len, tag, hash_val);
        if (!new_item) {
#ifdef _DEBUG_LOG
            fprintf(hashtable_log, "[Out of Memory error], not enough memory\n");
#endif
            *flags |= HASHTABLE_FLAGS_PUT_NEW_ITEM_FAIL_OOM;
		    UNLOCK(table.bucket[bucket_idx].lock);
            return NULL;
        }

		TAILQ_INSERT_HEAD(table.bucket[bucket_idx].chain, new_item, link);

        complete_bin_tree_insert(new_item);

		UNLOCK(table.bucket[bucket_idx].lock);

        *flags |= HASHTABLE_FLAGS_PUT_NEW_ITEM_SUCC;

        __atomic_fetch_add(&totalItem, 1, __ATOMIC_RELAXED);

		return new_item;

	} else {
        int diff = value_len - item->value_len;

        if (totalUsedMemory + diff >= MEMORY_LIMITATION) {

		    TAILQ_REMOVE(table.bucket[bucket_idx].chain, item, link);

            complete_bin_tree_delete(item);

            DestroyHashTableItem(&item);

            __atomic_fetch_sub(&totalItem, 1, __ATOMIC_RELAXED);

            *flags |= HASHTABLE_FLAGS_UPDATE_ITEM_FAIL_MEM_LIMIT;

		    UNLOCK(table.bucket[bucket_idx].lock);
            return NULL;
        }

        __atomic_store_n(&item->active, 0, __ATOMIC_RELAXED);

        do {
            usleep(30);
        } while (item->refCount > 0);

        free(item->data);

        item->data = malloc(key_len + value_len);
        if (!item->data) {
            
		    TAILQ_REMOVE(table.bucket[bucket_idx].chain, item, link);

            complete_bin_tree_delete(item);

            DestroyHashTableItem(&item);

		    UNLOCK(table.bucket[bucket_idx].lock);

            *flags |= HASHTABLE_FLAGS_UPDATE_ITEM_FAIL_OOM;

            return NULL;
        }

        item->key_len = key_len;
        item->value_len = value_len;

        memcpy(item->data, key, key_len);
        memcpy(item->data + key_len, value, value_len);

        __atomic_store_n(&item->active, 1, __ATOMIC_RELAXED);

        __atomic_fetch_add(&totalUsedMemory, diff, __ATOMIC_RELAXED);

        *flags |= HASHTABLE_FLAGS_UPDATE_ITEM_SUCC;

		UNLOCK(table.bucket[bucket_idx].lock);

		return item;
	}
}

bool
hashtable_delete(void *key, const uint16_t key_len) {

	uint64_t hash_val = CAL_HASH_VAL(key, key_len);
	//uint16_t tag = GET_TAG(hash_val);
	uint32_t bucket_idx = GET_BUCKET_IDX(hash_val);
	
	LOCK(table.bucket[bucket_idx].lock);
	kv_hashtable_item_t *item = GetItem(key, key_len);

	if (!item) {

		UNLOCK(table.bucket[bucket_idx].lock);
		return KV_DEL_NO_VALUE;

	} else {

		TAILQ_REMOVE(table.bucket[bucket_idx].chain, item, link);

        complete_bin_tree_delete(item);

		DestroyHashTableItem(&item);

        __atomic_fetch_sub(&totalItem, 1, __ATOMIC_RELAXED);

		UNLOCK(table.bucket[bucket_idx].lock);
        
		return KV_DEL_SUCC;
	}
}

uint32_t
hashtable_get_size(void) {
    return table.hash_table_size;
}

uint32_t
hashtable_get_hashmask(void) {
    return table.hash_mask;
}

kv_hashtable_item_t *
hashtable_start_to_access_random_item(void) {

    kv_hashtable_item_t *it = complete_bin_tree_get_random_item();
    __atomic_fetch_add(&it->refCount, 1, __ATOMIC_RELAXED);
    return it;
}

void
hashtable_stop_to_access(kv_hashtable_item_t *it) {
    __atomic_fetch_sub(&it->refCount, 1, __ATOMIC_RELAXED);
}

uint64_t
hashtable_get_number_of_objects(void) {
    return totalItem;
}

hash_iterator_t *
hashtable_get_bucket_iterator(const uint32_t bucketIdx) {

    hash_iterator_t *iter = malloc(sizeof(hash_iterator_t));
    if (!iter)  return NULL;

    iter->bucketIdx = bucketIdx;

    LOCK(table.bucket[bucketIdx].lock);
    iter->b = &table.bucket[bucketIdx];

    iter->cur = TAILQ_FIRST(iter->b->chain);

    if (!iter->cur) { 
        free(iter);
        UNLOCK(table.bucket[bucketIdx].lock);
        return NULL;
    }

    iter->index = 0;

    return iter;
}

void
hashtable_free_bucket_iterator(hash_iterator_t *iter) {

    free(iter);
    UNLOCK(iter->b->lock);
}

void
hash_bucket_iterator_next(hash_iterator_t *iter) {

    iter->cur = TAILQ_NEXT(iter->cur, link);
    iter->index++;
}
