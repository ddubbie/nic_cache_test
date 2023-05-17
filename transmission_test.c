#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <string.h>
#include <stddef.h>
#include <unistd.h>
#include <assert.h>
#include <stdbool.h>
#include <stdint.h>
#include <errno.h>
#include <arpa/inet.h>
#include <sched.h>
#include <fcntl.h>
#include <signal.h>

#include "hashtable.h"
#include "connection.h"

#define log_error(_f, _m...) do{\
    fprintf(stderr, "[Error][%10s:%4d]" _f, __FUNCTION__, __LINE__, ##_m);\
} while(0)

#define log_trace(_f, _m...) do{\
    fprintf(stdout, "[TRACE][%10s:%4d]" _f, __FUNCTION__, __LINE__, ##_m);\
} while(0)

#define GET 0

typedef struct req_hdr_ req_hdr;
typedef struct rep_hdr_ rep_hdr;

struct req_hdr_ {
    uint8_t reqtype;
    uint8_t keyLen;
} __attribute__((packed));

struct rep_hdr_ {
    uint8_t replyType;
    uint16_t valLen;
    uint8_t val[];
} __attribute__((packed));

static uint8_t num_threads_;
static uint16_t max_concurrency_;
static bool *run_;
static pthread_t *transmission_thread_tid_;
static uint8_t *thread_no_;
/*
static uint32_t rx_bytes_[1<<13] = {0};
static uint32_t tx_bytes_[1<<13] = {0};
static uint32_t time_idx_ = 0;
static bool run_incr_idx_thread_ = true;
static pthread_mutex_t incr_idx_mtx_ = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t incr_idx_cond_ = PTHREAD_COND_INITIALIZER;
*/
static struct sockaddr_in daddr_;

static void SetupTransmissionTest(void);
static void TeardownTransmissionTest(void);
static void *RunTransmissionTestThread(void *arg);

static void SetCoreAffinity(const int thread_no);

static connection_t *CreateConnection(connection_pool_t *cp, int *thread_concurrency, int ep);
static connection_t *TryConnection(connection_t *c, int *thread_concurrency);
static int SendRandomGetRequest(connection_t *c);
static int ReceiveReply(connection_t *c, uint8_t *buf, const ssize_t buf_size, 
        connection_pool_t *cp, int *thread_concurrency);
static void CloseConnection(connection_t *c, connection_pool_t *cp, int *thread_concurrency);


/* (keyLen,key,valLen,val)*/
static void
SetupTransmissionTest(void) {

    FILE *sample_key_value_file = NULL;
    char line[1 << 12];
    char *key, *val, *saveptr, *endptr, *p;
    uint16_t keyLen;
    uint32_t valLen;
    uint16_t flags;

    hashtable_setup(20);

    sample_key_value_file = fopen("sample_key_value.txt", "r");
    if (!sample_key_value_file) {
        log_error("fopen() error, %s\n", strerror(errno));
        exit(EXIT_FAILURE);
    }

    while(true) {
        if (!fgets(line, 1 << 12, sample_key_value_file))
            break;
        p = strtok_r(line, ",", &saveptr);
        keyLen = strtol(p, &endptr, 10);
        key = strtok_r(NULL, ",", &saveptr);
        p = strtok_r(NULL, ",", &saveptr);
        valLen = strtol(p, &endptr, 10);
        val = strtok_r(NULL, ",", &saveptr);
        hashtable_put(key, keyLen, val, valLen, &flags);
    }

    fclose(sample_key_value_file);

    run_ = malloc(sizeof(bool) * num_threads_);
    if (!run_) {
        log_error("malloc() error, %s\n", strerror(errno));
        exit(EXIT_FAILURE);
    }

    transmission_thread_tid_ = malloc(sizeof(pthread_t) * num_threads_);
    if (!transmission_thread_tid_) {
        log_error("malloc() error, %s\n", strerror(errno));
        exit(EXIT_FAILURE);
    }

    thread_no_ = malloc((sizeof(uint8_t) * num_threads_));
    if (!thread_no_) {
        log_error("malloc() error, %s\n", strerror(errno));
        exit(EXIT_FAILURE);
    }
}

static void
TeardownTransmissionTest(void) {
    free(transmission_thread_tid_);
    free(run_);
    free(thread_no_);
    hashtable_teardown();
}

static connection_t *
TryConnection(connection_t *c, int *thread_concurrency) 
{
    if (connect(c->fd, (struct sockaddr *)&daddr_, sizeof(struct sockaddr_in) < 0)) {
        if (errno != EINPROGRESS) {
            return NULL;
        } else {
            c->state = CONNECTION_AGAIN;
        }
    } else {
        c->state = CONNECTION_ESTABLISEHD;
    }

    *thread_concurrency = *thread_concurrency + 1;

    return c;
}

static connection_t *
CreateConnection(connection_pool_t *cp, int *thread_concurrency, int ep) 
{
    connection_t *c;
    struct epoll_event ev;
    int flags;

    c = connection_allocate(cp);
    if (!c) {
        return NULL;
    }

    c->fd = socket(AF_INET, SOCK_STREAM, 0);
    if (c->fd < 0) {
        log_error("socket() faIl, %s\n", strerror(errno));
        goto fail;
    }

    flags = fcntl(c->fd, F_GETFL, 0);
    if (fcntl(c->fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        log_error("fcntl() fail, %s\n", strerror(errno));
        goto fail;
    }

    ev.data.ptr = c;
    ev.events = EPOLLIN | EPOLLOUT;
    if (epoll_ctl(ep, EPOLL_CTL_ADD, c->fd, &ev) < 0) {
        log_error("epoll_ctl() fail, %s\n", strerror(errno));
        goto fail;
    }

    if(!TryConnection(c, thread_concurrency))
        goto fail;

    return c;

fail :
    close(c->fd);
    connection_deallocate(cp, c);
    return NULL;
}

static void
CloseConnection(connection_t *c, connection_pool_t *cp, int *thread_concurrency)
{
    *thread_concurrency = *thread_concurrency - 1;
    close(c->fd);
    connection_deallocate(cp, c);
}

static int
SendRandomGetRequest(connection_t *c)
{
    int ret;
    req_hdr hdr;
    struct iovec vec[2];
    if (c->state != CONNECTION_ESTABLISEHD)
        return -1;

    c->it = hashtable_start_to_access_random_item();

    hdr.reqtype = GET;
    hdr.keyLen = item_keyLen(c->it);

    vec[0].iov_base = &hdr;
    vec[0].iov_len = sizeof(req_hdr);
    vec[1].iov_base = item_key(c->it);
    vec[1].iov_len = item_keyLen(c->it);

    ret = writev(c->fd, vec, 2);
    if (ret < 0) 
        return -1;

    c->state = CONNECTION_WAIT_FOR_REPLY;;

    return 0;
}

static int
ReceiveReply(connection_t *c, uint8_t buf[], const ssize_t buf_size, 
        connection_pool_t *cp, int *thread_concurrency)
{
    int len, offset = 0;

    if (c->state != CONNECTION_WAIT_FOR_REPLY || c->state != CONNECTION_RCV_REPLY_AGAIN) {
        return -1;
    }

    while((len = read(c->fd, buf + offset, buf_size - offset)))
    {
        offset += len;
    }

    if (len == 0) {
        CloseConnection(c, cp, thread_concurrency);
        return 0;
    } else {
        if (errno == EAGAIN) {
            c->state = CONNECTION_RCV_REPLY_AGAIN;
            return -2;
        } else {
            CloseConnection(c, cp, thread_concurrency);
            return -1;
        }
    }
}

static void
SetCoreAffinity(const int thread_no) 
{
#ifdef _GNU_SOURCE
    cpu_set_t cpuset;
    pthread_t tid = pthread_self();

    CPU_ZERO(&cpuset);

    CPU_SET(thread_no, &cpuset);

    if (pthread_setaffinity_np(tid, sizeof(cpu_set_t), &cpuset) != 0) {
        log_error("pthread_setaffinity_np() error, %s\n", strerror(errno));
        exit(EXIT_FAILURE);
    }
#else
    (void)thread_no;
#endif
}

static void *
RunTransmissionTestThread(void *arg) 
{
    int i;
    int ep;
    int nevents;
    struct epoll_event events[max_concurrency_ / num_threads_ * 3];
    connection_t *c;
    uint8_t buf[1 << 13];

    uint8_t thread_number = *(uint8_t *)arg;
    const int thread_max_conncurrency = max_concurrency_ / num_threads_;
    const int num_max_events = thread_max_conncurrency * 3;
    int thread_concurrency = 0;
    connection_pool_t *cp = connection_create_pool(thread_max_conncurrency);

    run_[thread_number] = true;

    SetCoreAffinity(thread_number);

    ep = epoll_create(num_max_events);
    if (ep < 0) {
        log_error("epoll_create() error, %s\n", strerror(errno));
        exit(EXIT_FAILURE);
    }

    while (run_[thread_number]) 
    {
        while (thread_concurrency < thread_max_conncurrency)
            CreateConnection(cp, &thread_concurrency, ep);

        nevents = epoll_wait(ep, events, num_max_events, -1);
        if (nevents < 0) {
            break;
        }

        for (i = 0; i < nevents; i++) {
            c = events[i].data.ptr;
            if (events[i].events & EPOLLERR || events[i].events & EPOLLHUP) {
                CloseConnection(c, cp, &thread_concurrency);
            } else if (c->state == CONNECTION_AGAIN) {
                TryConnection(c, &thread_concurrency);
            } else if (c->state == CONNECTION_ESTABLISEHD) {
                if (SendRandomGetRequest(c) < 0) {
                    CloseConnection(c, cp, &thread_concurrency);
                }
            } else if (c->state == CONNECTION_WAIT_FOR_REPLY || 
                            c->state == CONNECTION_RCV_REPLY_AGAIN) {
                ReceiveReply(c, buf, 1U << 13, cp, &thread_concurrency);

            } else {
                CloseConnection(c ,cp, &thread_concurrency);
            }
        }
    }
    connection_destroy_pool(&cp);
    pthread_exit(NULL);
    return NULL;
}
/*
static void *
RunIncrIndexThread(void *arg) {

    struct timespec ts;

    sleep(2);

    while(run_incr_idx_thread_) 
    {
        pthread_mutex_lock(&incr_idx_mtx_);
        clock_gettime(CLOCK_REALTIME, &ts);

        ts.tv_sec += 1;

        pthread_cond_timedwait(&incr_idx_cond_, &incr_idx_mtx_, &ts);
        time_idx_++;
        pthread_mutex_unlock(&incr_idx_mtx_);
    }
    pthread_exit(NULL);
    return NULL;
}*/

int 
main(const int argc, char *argv[]) {

    int opt, i;

    if (argc != 5) {
        log_error("invalide number of arguments, %d\n", argc);
        return -1;
    }

    while((opt = getopt(argc, argv, "n:c:"))) 
    {
        switch(opt) {
            case 'n' :
                num_threads_ = atoi(optarg);
                break;
            case 'c' :
                max_concurrency_ = atoi(optarg);
                break;
            default :
                log_error("invalid argument %c error\n", (char)opt);
                return -1;
        }
    }
    daddr_.sin_family = AF_INET;
    daddr_.sin_addr.s_addr = inet_addr("10.0.30.110");
    daddr_.sin_port = htons(65000);

    SetupTransmissionTest();

//    pthread_t incrIdxThread;

    for (i = 0; i < num_threads_; i++) {
        thread_no_[i] = i;
        if (pthread_create(&transmission_thread_tid_[i], 
                    NULL, RunTransmissionTestThread, &thread_no_[i]) != 0) {
            log_error("pthread_create() error, %s\n", strerror(errno));
            exit(EXIT_FAILURE);
        }
    }
/*
    if (pthread_create(&incrIdxThread, NULL, RunIncrIndexThread, NULL) != 0) {
            log_error("pthread_create() error, %s\n", strerror(errno));
            exit(EXIT_FAILURE);
    }*/

    for (i = 0; i < num_threads_; i++) {
        pthread_join(transmission_thread_tid_[i], NULL);
    }

    //pthread_join(incrIdxThread, NULL);

    TeardownTransmissionTest();

    return 0;
}
