#include "connection.h"
#include <hugetlbfs.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <assert.h>

connection_pool_t *
connection_create_pool(const ssize_t num_total_elements) 
{
    connection_pool_t *cp;
    int i;
    
    cp = malloc(sizeof(connection_pool_t));
    if (!cp){
        fprintf(stderr, "malloc() error, %s\n", strerror(errno));
        exit(EXIT_FAILURE);
    }

    long hugepagesize = gethugepagesize();
    size_t len = (sizeof(connection_t) * num_total_elements / hugepagesize + 1) * hugepagesize;

    cp->mem = get_huge_pages(len ,GHP_DEFAULT);
    if (!cp->mem) {
        fprintf(stderr, "get_huge_pages() error, %s\n", strerror(errno));
        exit(EXIT_FAILURE);
    }

    for (i = 0; i < num_total_elements - 1; i++) {
        cp->mem[i].state = CONNECTION_UNUSED;
        cp->mem[i].next = &cp->mem[i+1];
    }

    cp->mem[num_total_elements - 1].next = NULL;

    cp->head = &cp->mem[0];
    cp->num_total_elements = num_total_elements;
    cp->num_free_elements = num_total_elements;

    return cp;
}

connection_t *
connection_allocate(connection_pool_t *cp)
{
    connection_t *c;

    if (!cp->head) {
        printf("%ld\n", cp->num_free_elements);
        assert(cp->num_free_elements == 0);
        return NULL;
    }

    c = cp->head;
    cp->head = cp->head->next;

    c->state = CONNECTION_USED;
    c->next = NULL;

    cp->num_free_elements--;

    return c;
}

void
connection_deallocate(connection_pool_t *cp, connection_t *c)
{
    if (!cp->head) {
        cp->head = c;
        c->next = NULL;
    } else {
        c->next = cp->head;
        cp->head = c;
    }

    c->state = CONNECTION_UNUSED;
    cp->num_free_elements++;
}

void
connection_destroy_pool(connection_pool_t **cp)
{
    free_huge_pages((*cp)->mem);
    free(*cp);
    *cp = NULL;
}
