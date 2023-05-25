#ifndef __CONNECTION_H__
#define __CONNECTION_H__

#include <stdint.h>
#include <stddef.h>
#include <sys/types.h>
#include <wchar.h>
#include <time.h>
#include "hashtable.h"

enum connection_state {
    CONNECTION_USED                 =   0,
    CONNECTION_AGAIN                =   1,
    CONNECTION_ESTABLISEHD          =   2,
    CONNECTION_WAIT_FOR_REPLY       =   3,
    CONNECTION_RCV_REPLY_AGAIN      =   4,
    CONNECTION_UNUSED               =   5,
};

typedef struct connection_s {
    int fd;
    enum connection_state state;
    struct connection_s *next;
    struct timespec ts;
    kv_hashtable_item_t *it;
    uint8_t buf[16384];
    uint16_t buflen;
} connection_t;

typedef struct connection_pool_s {
    connection_t *mem;
    connection_t *head;
    ssize_t num_total_elements;
    ssize_t num_free_elements;
}connection_pool_t;

connection_pool_t *connection_create_pool(const ssize_t num_total_elements);
connection_t *connection_allocate(connection_pool_t *cp);
void connection_deallocate(connection_pool_t *cp, connection_t *c);
void connection_destroy_pool(connection_pool_t **cp);

#endif
