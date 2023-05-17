#ifndef __HASH_TABLE_H__
#define __HASH_TABLE_H__

#include <stdint.h>
#include <stdbool.h>
#include <sys/queue.h>
#include <pthread.h>
#include <xxhash.h>

#define CAL_HASH_VAL(_key, _key_len) (XXH3_64bits(_key, _key_len))
#define GET_TAG(_val) ((uint16_t)(_val >> 32) & 0xffff)

#if _USE_SPINLOCK
#define LOCK_INIT(_lock)    pthread_spin_init(_lock, PTHREAD_PROCESS_PRIVATE)
#define LOCK_DESTROY(_lock) pthread_spin_destroy(_lock)
#define LOCK(_lock)         pthread_spin_lock(_lock)
#define UNLOCK(_lock)       pthread_spin_unlock(_lock)
#else
#define LOCK_INIT(_lock)    pthread_mutex_init(_lock, NULL)
#define LOCK_DESTROY(_lock) pthread_mutex_destroy(_lock)
#define LOCK(_lock)         pthread_mutex_lock(_lock)
#define UNLOCK(_lock)       pthread_mutex_unlock(_lock)
#endif

#define	KV_PUT_CHANGE_VALUE	false
#define KV_PUT_NEW_VALUE	true
#define KV_DEL_SUCC			true
#define	KV_DEL_NO_VALUE		false

#define HASHTABLE_FLAGS_UPDATE_ITEM_SUCC 0x01
#define HASHTABLE_FLAGS_UPDATE_ITEM_FAIL_OOM 0x02
#define HASHTABLE_FLAGS_UPDATE_ITEM_FAIL_MEM_LIMIT 0x04 
#define HASHTABLE_FLAGS_PUT_NEW_ITEM_SUCC 0x08       
#define HASHTABLE_FLAGS_PUT_NEW_ITEM_FAIL_OOM 0x10   
#define HASHTABLE_FLAGS_PUT_NEW_ITEM_FAIL_MEM_LIMIT  0x20

typedef struct kv_hashtable_item_s kv_hashtable_item_t;
typedef struct kv_hashtable_bucket_s kv_hashtable_bucket_t;
typedef struct kv_hashtable_chain_s kv_hashtable_chain_t;
typedef struct kv_hashtable_s kv_hashtable_t;
typedef struct hash_iterator_s hash_iterator_t;

struct kv_hashtable_item_s {
	uint32_t key_len : 12;		
	uint32_t value_len : 20;
	uint16_t tag;
    void *data;
    uint8_t active;
    uint32_t refCount;
    uint64_t lastAccessedTime;
    uint64_t interArrivalTime;
    uint64_t n_requests;
    uint64_t hv;
    kv_hashtable_item_t *_left;
    kv_hashtable_item_t *_right;
    kv_hashtable_item_t *_parent;
	TAILQ_ENTRY(kv_hashtable_item_s) link;
};

TAILQ_HEAD(kv_hashtable_chain_s, kv_hashtable_item_s);

#define item_key(_it)   (_it->data)
#define item_value(_it) (void *)((uint64_t)_it->data + _it->key_len)
#define item_keyLen(_it)    (_it->key_len)
#define item_valueLen(_it)  (_it->value_len)
#define item_tag(_it)   (_it->tag)

struct kv_hashtable_bucket_s {
#ifdef _USE_SPINLOCK
	pthread_spinlock_t *lock;
#else
	pthread_mutex_t *lock;
#endif
	kv_hashtable_chain_t *chain;
};

struct kv_hashtable_s {
	uint32_t hash_table_size;
	uint32_t hash_mask;
	kv_hashtable_bucket_t *bucket;
};

struct hash_iterator_s {
    uint32_t bucketIdx;
    uint32_t index;
    kv_hashtable_bucket_t *b;
    kv_hashtable_item_t *cur;
};

void hashtable_setup(const uint16_t hash_power);

kv_hashtable_item_t *hashtable_start_to_access(void *key, const uint16_t key_len);

void hashtable_start_to_access_directly(kv_hashtable_item_t *it);

kv_hashtable_item_t *hashtable_put(void *key, const uint16_t key_len, void *value, const uint32_t value_len, uint16_t *flags);

bool hashtable_delete(void *key, const uint16_t key_len);

uint32_t hashtable_get_size(void);

uint32_t hashtable_get_hashmask(void);

kv_hashtable_item_t *hashtable_start_to_access_random_item(void);

void hashtable_stop_to_access(kv_hashtable_item_t *it);

void hashtable_teardown(void);

//void hashtable_item_update_time_meta(kv_hashtable_item_t *it);

uint64_t hashtable_get_number_of_objects(void);

hash_iterator_t *hashtable_get_bucket_iterator(const uint32_t bucketIdx);

void hashtable_free_bucket_iterator(hash_iterator_t *iter);

void hash_bucket_iterator_next(hash_iterator_t *iter);

#endif /* __HASH_TABLE_H__ */
