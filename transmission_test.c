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
#include <xxhash.h>

#include "hashtable.h"
#include "connection.h"
#include "rng.h"

#define log_error(_f, _m...) do{\
    fprintf(stderr, "[Error][%10s:%4d]" _f, __FUNCTION__, __LINE__, ##_m);\
} while(0)

#define log_trace(_f, _m...) do{\
    fprintf(stdout, "[TRACE][%10s:%4d]" _f, __FUNCTION__, __LINE__, ##_m);\
} while(0)

#define GET 0

#define FIRST_BITMASK       (UINT32_MAX)
#define SECOND_BITMASK      ((1LU << 48) - 1) & (~((1LU << 16) - 1))
#define THIRD_BITMASK       (UINT64_MAX) & ~((1LU << 32) - 1)

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
static uint32_t num_items_;
static kv_hashtable_item_t **items_;
static struct timespec global_test_start_ts_;
static uint32_t num_close_ = 0;
static uint32_t num_connect_ = 0;

static struct sockaddr_in daddr_;
static in_port_t dport;
static in_addr_t dIp;

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

static void SignalInterruptHandler(int signo);
static bool CheckReply(connection_t *c, uint8_t *buf, const ssize_t buf_size);

static void MixItems(const uint64_t hv_bitmask);
static void QuickSort(const uint64_t hv_bitmask, const int left, const int right);

static void *PrintLog(void *arg);
static pthread_mutex_t logMtx_ = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t logCnd_ = PTHREAD_COND_INITIALIZER;
static uint64_t total_rx_bytes = 0;
static uint64_t total_tx_bytes = 0;
static bool run_log_ = true;
static int *per_thread_concurrency[16];

static void
SignalInterruptHandler(int signo)
{
    int i;

    for (i = 0; i < num_threads_; i++) {
        run_[i] = false;
    }
    run_log_ = false;
}
/* (keyLen,key,valLen,val)*/
static void
SetupTransmissionTest(void) {

    FILE *sample_key_value_file = NULL;
    char line[1 << 12];
    char *key, *val, *saveptr, *endptr, *p;
    uint16_t keyLen;
    uint32_t valLen;
    uint16_t flags;
    kv_hashtable_item_t *it;
    uint32_t count = 0;

    hashtable_setup(20);

    sample_key_value_file = fopen("sample_key_value.txt", "r");
    if (!sample_key_value_file) {
        log_error("fopen() error, %s\n", strerror(errno));
        exit(EXIT_FAILURE);
    }

    items_ = malloc(sizeof(kv_hashtable_item_t *) * num_items_);
    if (!items_) {
        log_error("malloc() error, %s\n", strerror(errno));
        exit(EXIT_FAILURE);
    }

    while (count < num_items_) {
        if (!fgets(line, 1 << 12, sample_key_value_file))
            break;
        p = strtok_r(line, ",", &saveptr);
        keyLen = strtol(p, &endptr, 10);
        key = strtok_r(NULL, ",", &saveptr);
        p = strtok_r(NULL, ",", &saveptr);
        valLen = strtol(p, &endptr, 10);
        val = strtok_r(NULL, ",", &saveptr);
        it = hashtable_put(key, keyLen, val, valLen, &flags);
        items_[count] = it;
        count++;
    }

    MixItems(FIRST_BITMASK);

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

    signal(SIGINT, SignalInterruptHandler);

}

static void
MixItems(const uint64_t hv_bitmask) 
{
    QuickSort(hv_bitmask, 0, num_items_ - 1);
    /*
    for (int i = 0; i < num_items_ - 1; i++) {
        if((items_[i]->hv & hv_bitmask) > (items_[i+1]->hv & hv_bitmask)) {
            printf("fail\n");
            exit(EXIT_FAILURE);
        }
    }*/
}

static void
QuickSort(const uint64_t hv_bitmask, const int left, const int right) 
{
    if (left >= right) return;

    int mid, i, j;
    uint64_t pivot_hv;

    mid = (left + right) / 2;
    i = left;
    j = right;

    pivot_hv = items_[mid]->hv  & hv_bitmask;

    while (i <= j) 
    {
        while (((items_[i]->hv & hv_bitmask) < pivot_hv) && i < right) {
            i++;
        }
        while (((items_[j]->hv & hv_bitmask) > pivot_hv) && j > left) {
            j--;
        }

        if (i <= j) {
            kv_hashtable_item_t *temp = items_[i];
            items_[i] = items_[j];
            items_[j] = temp;
            i++;
            j--;
        }
    }

    QuickSort(hv_bitmask, left, j);
    QuickSort(hv_bitmask, i, right);
}

static void
TeardownTransmissionTest(void) {
    free(transmission_thread_tid_);
    free(run_);
    free(thread_no_);
    free(items_);
    hashtable_teardown();
}

static connection_t *
TryConnection(connection_t *c, int *thread_concurrency) 
{
    if (connect(c->fd, (struct sockaddr *)&daddr_, sizeof(struct sockaddr_in)) < 0) {
        if (errno == EINPROGRESS) {
            c->state = CONNECTION_AGAIN;
            *thread_concurrency = *thread_concurrency + 1;
            return c;
        } else {
            return NULL;
        }
    } else {
        if (c->state == !CONNECTION_AGAIN)
            *thread_concurrency = *thread_concurrency + 1;
        c->state = CONNECTION_ESTABLISEHD;
        num_connect_++;
    }
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
    num_close_++;
}

static int
SendRandomGetRequest(connection_t *c)
{
    int ret;
    uint32_t prio;
    req_hdr hdr;
    struct iovec vec[2];
    if (c->state != CONNECTION_ESTABLISEHD)
        return -1;

//    c->it = hashtable_start_to_access_random_item();

    prio = rng_zipf(1.0, num_items_) - 1;
    c->it = items_[prio];
    hdr.reqtype = GET;
    hdr.keyLen = item_keyLen(c->it);

    vec[0].iov_base = &hdr;
    vec[0].iov_len = sizeof(req_hdr);
    vec[1].iov_base = item_key(c->it);
    vec[1].iov_len = item_keyLen(c->it);

    ret = writev(c->fd, vec, 2);
    if (ret < 0) 
        return -1;

    total_tx_bytes += ret;
    c->state = CONNECTION_WAIT_FOR_REPLY;

    return 0;
}

static int
ReceiveReply(connection_t *c, uint8_t buf[], const ssize_t buf_size, 
        connection_pool_t *cp, int *thread_concurrency)
{
    int len, offset = 0;

    if (c->state != CONNECTION_WAIT_FOR_REPLY && c->state != CONNECTION_RCV_REPLY_AGAIN) {
        return -1;
    }

    while((len = read(c->fd, buf + offset, buf_size - offset)) > 0)
    {
        total_rx_bytes += len;
        offset += len;
    }

    if (len == 0) {
        CloseConnection(c, cp, thread_concurrency);
        return 0;
    } else {
        if (errno == EAGAIN) {
            if (offset == (sizeof(rep_hdr) + item_valueLen(c->it))) {
                CheckReply(c, buf, buf_size);
                CloseConnection(c, cp ,thread_concurrency);
                return 0;
            } else {
                c->state = CONNECTION_RCV_REPLY_AGAIN;
            }
            return -2;
        } else {
            CloseConnection(c, cp, thread_concurrency);
            return -1;
        }
    }
}

static bool
CheckReply(connection_t *c, uint8_t *buf, const ssize_t buf_size)
{
    int ret;
    rep_hdr *hdr = (rep_hdr *)buf;
    if (buf_size < hdr->valLen) {
        log_trace("buffer size : %lu, received value size : %u\n",
                buf_size, hdr->valLen);
    }

    hdr->val[hdr->valLen] = '\0';

    if (item_valueLen(c->it) == hdr->valLen) {
        if ((ret = memcmp(hdr->val, item_value(c->it), hdr->valLen)) != 0) {
            log_trace("Received reply error, ret:%d\n", ret);

            log_trace("original : %s\n", (char *)item_value(c->it));
            log_trace("rcvd : %s\n", hdr->val);
            return false;
        }
    } else {
        log_trace("Value size error, (%u, %u)\n", hdr->valLen, item_valueLen(c->it));
        return false;
    }

    return true;
}

static void
SetCoreAffinity(const int thread_no) 
{
#ifdef _GNU_SOURCE
    cpu_set_t cpuset;
    pthread_t tid = pthread_self();

    CPU_ZERO(&cpuset);
    CPU_SET(thread_no, &cpuset);

    if (pthread_setaffinity_np(tid, sizeof(cpu_set_t), &cpuset) < 0) {
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

    per_thread_concurrency[thread_number] = &thread_concurrency;

    run_[thread_number] = true;

    SetCoreAffinity(thread_number);

    ep = epoll_create(num_max_events);
    if (ep < 0) {
        log_error("epoll_create() error, %s\n", strerror(errno));
        exit(EXIT_FAILURE);
    }

    while (run_[thread_number]) 
    {
        while (thread_concurrency < thread_max_conncurrency) {
            assert(thread_concurrency >= 0);
            CreateConnection(cp, &thread_concurrency, ep);
        }
        nevents = epoll_wait(ep, events, num_max_events, -1);
        if (nevents < 0) {
            break;
        }

        for (i = 0; i < nevents; i++) {
            c = events[i].data.ptr;
            //log_trace("%u\n", c->state);
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
                CloseConnection(c, cp, &thread_concurrency);
            }
        }
    }

    connection_destroy_pool(&cp);
    pthread_exit(NULL);
    return NULL;
}

static void *
PrintLog(void *arg) {
    int i;
    struct timespec ts;
    double rx_byte_ratio;
    double tx_byte_ratio;
    uint32_t sec;
    sleep(10);
    sec = 10;

    while(run_log_) {
        pthread_mutex_lock(&logMtx_);
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += 1;
        sec++;
        rx_byte_ratio = (double)total_rx_bytes / (sec * (1 << 20));
        tx_byte_ratio = (double)total_tx_bytes / (sec * (1 << 20));
        fprintf(stdout, "rx:%-10lf(MB/sec)   tx:%-10lf(MB/sec), "
                        "# connects : %-8u    # closes : %-8u\n", 
                rx_byte_ratio, tx_byte_ratio,
                num_connect_, num_close_);

        for (i = 0; i < num_threads_; i++) {
            fprintf(stdout, "[Thread%d] #flows:%d\n", i, *per_thread_concurrency[i]);
        }

        pthread_cond_timedwait(&logCnd_, &logMtx_, &ts);
        pthread_mutex_unlock(&logMtx_);
    }
    pthread_exit(NULL);
    return NULL;
}

int 
main(const int argc, char *argv[]) {

    int opt, i;
    pthread_t printLogThread;
    bool print_log = true;

    if (argc != 7 && argc != 8) {
        log_error("invalide number of arguments, %d\n", argc);
        return -1;
    }

    while((opt = getopt(argc, argv, "t:n:c:p")) != -1) 
    {
        switch(opt) {
            case 't' :
                num_threads_ = atoi(optarg);
                break;
            case 'c' :
                max_concurrency_ = atoi(optarg);
                break;
            case 'n' :
                num_items_ = atoi(optarg);
                break;
            case 'p' :
                print_log = true;
                break;
            default :
                log_error("invalid argument %c error\n", (char)opt);
                return -1;
        }
    }

    clock_gettime(CLOCK_REALTIME, &global_test_start_ts_);

    dIp = inet_addr("10.0.30.110");
    dport = htons(65000);

    daddr_.sin_family = AF_INET;
    daddr_.sin_addr.s_addr = dIp;
    daddr_.sin_port = dport;

    SetupTransmissionTest();

    for (i = 0; i < num_threads_; i++) {
        thread_no_[i] = i;
        if (pthread_create(&transmission_thread_tid_[i], 
                    NULL, RunTransmissionTestThread, &thread_no_[i]) != 0) {
            log_error("pthread_create() error, %s\n", strerror(errno));
            exit(EXIT_FAILURE);
        }
    }

    if (print_log) {
        if (pthread_create(&printLogThread, NULL, PrintLog, NULL) != 0) {
                log_error("pthread_create() error, %s\n", strerror(errno));
                exit(EXIT_FAILURE);
        }
    }

    pthread_join(printLogThread, NULL);

    for (i = 0; i < num_threads_; i++) {
        pthread_join(transmission_thread_tid_[i], NULL);
    }

    TeardownTransmissionTest();

    return 0;
}
