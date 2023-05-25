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
#include <signal.h>

#include "hashtable.h"

#define log_error(_f, _m...) do{\
    fprintf(stderr, "[Error][%10s:%4d]" _f, __FUNCTION__, __LINE__, ##_m);\
} while(0)

#define log_trace(_f, _m...) do{\
    fprintf(stdout, "[TRACE][%10s:%4d]" _f, __FUNCTION__, __LINE__, ##_m);\
} while(0)

#define GET 0
#define SAMPLE_OBJECT_SIZE  128

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

static uint8_t app_rcv_buf[1<<16];

static void SetupKeyValue(void);
static void DestroyKeyValue(void);
static int CreateConnection(void);
static int SendGetRequest(const int fd);
static int ReceiveReply(const int fd);
static void CloseConnection(const int fd);
static void SigInteruuptHandler(int signo);

static in_port_t dport;
static in_addr_t daddr;

static void **key_;
static void **value_;
static uint32_t num_key_values_ = 1;

static bool run_test_ = true;

static void
SetupKeyValue(void) {

    int count = 0;
    char line[1U << 16];
    FILE *sample_key_value_file;
    char *key, *val, *saveptr, *endptr, *p;
    uint16_t keyLen;
    uint32_t valLen;

    sample_key_value_file = fopen("sample_key_value.txt", "r");
    if (!sample_key_value_file) {
        log_error("fopen() error, %s\n", strerror(errno));
        exit(EXIT_FAILURE);
    }

    key_ = malloc(sizeof(void *) * num_key_values_);
    if (!key_) {
        log_error("malloc() error, %s\n", strerror(errno));
        exit(EXIT_FAILURE);
    }
    value_ = malloc(sizeof(void *) * num_key_values_);
    if (!value_) {
        log_error("malloc() error, %s\n", strerror(errno));
        exit(EXIT_FAILURE);
    }

    while (count < num_key_values_) {
        if (!fgets(line, 1U << 16, sample_key_value_file)) {
            break;
        }
        p = strtok_r(line, ",", &saveptr);
        keyLen = strtol(p, &endptr, 10);
        key = strtok_r(NULL, ",", &saveptr);
        p = strtok_r(NULL, ",", &saveptr);
        valLen = strtol(p, &endptr, 10);
        val = strtok_r(NULL, ",", &saveptr);

        key_[count] = malloc(keyLen + 1);
        if (!key_[count]) {
            log_error("malloc() error, %s\n", strerror(errno));
            exit(EXIT_FAILURE);
        }

        memcpy(key_[count], key, keyLen);
        *(char *)(key_[count] + keyLen) = '\0';

        value_[count] = malloc(valLen + 1);
        if (!value_[count]) {
            log_error("malloc() error, %s\n", strerror(errno));
            exit(EXIT_FAILURE);
        }
        memcpy(value_[count], val, valLen);
        *(char *)(value_[count] + valLen) = '\0';

        count++;
    }
}

static void
DestroyKeyValue(void) {
    int i;
    for (i = 0; i < num_key_values_; i++) {
        free(key_[i]);
        free(value_[i]);
    }

    free(key_);
    free(value_);
}

static int
CreateConnection(void) 
{
    struct sockaddr_in addr;
    int fd;
    int ret;

    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = daddr;
    addr.sin_port = dport;

    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        log_error("Failed to create socket\n");
        return -1;
    }
    ret = connect(fd, (struct sockaddr *)&addr, sizeof(struct sockaddr_in));
    if (ret < 0) {
        if (errno != EINPROGRESS) {
            perror("mtcp_connect error");
            close(fd);
        }
        log_trace("connect() fd:%d,ret:%d, errno:%d\n", fd, ret, errno);
    } 
    return fd;
}

static int
SendGetRequest(const int fd)
{
    int ret;
    req_hdr req = {
        GET,
        strlen((char *)key_[0])
    };

    struct iovec vec[2] = {
        {&req, 2},
        {key_[0], req.keyLen}
    };

    //log_trace("Send request packet\n");

    ret = writev(fd, vec, 2);
    if (ret < 0) {
        if (errno != EINPROGRESS) {
            perror("connect");
            close(fd);
            return -1;
        }
    }
    return ret; 
}

static int
ReceiveReply(const int fd)
{
    int len;;
    int off = 0;
    int to_be_rcvd = strlen((char *)value_[0]) + sizeof(rep_hdr);

    while((len = read(fd, app_rcv_buf + off, (1 << 16) - off)) > 0) {
        off += len;
        log_trace("rcvd_byte:%d\n", off);
        
        if (to_be_rcvd == off)
            break;
    } 

    if (len <= 0) {
        close(fd);
    }

    rep_hdr *rep = (rep_hdr *)app_rcv_buf;
    if (rep->valLen != strlen((char *)value_[0])) 
    {
        log_error("value length error, valLen:%u, object len:%lu\n",
                rep->valLen, strlen((char *)value_[0]));
        close(fd);
        return -1;
    }

    if (memcmp(rep->val, value_[0], rep->valLen) != 0) 
    {
        char *ptr = (char *)rep->val;
        ptr[rep->valLen] = '\0';
        log_error("received object error, received:%s, sample object:%s\n",
                ptr, (char *)value_[0]);
        close(fd);
        return -1;
    }

    return off; 
}

static void
CloseConnection(const int fd) {
    close(fd);
}

static void
SigInteruuptHandler(int signo)
{
    run_test_ = false;
}

int 
main(const int argc, char *argv[]) {

    int fd, len, opt;
    daddr = inet_addr("10.0.30.110");
    dport = htons(65000);

    signal(SIGINT, SigInteruuptHandler);

    while((opt = getopt(argc, argv, "n:")) != -1)
    {
        switch(opt) {
            case 'n' :
                num_key_values_ = atoi(optarg);
                break;
        }
    }

    SetupKeyValue();
    fd = CreateConnection();

    while (run_test_) {
        len = SendGetRequest(fd);
        log_trace("Send byte length : %d\n", len);
        len = ReceiveReply(fd);
        if (len < 0) {
            exit(EXIT_FAILURE);
        }
        log_trace("Received byte length : %d\n", len);
        break;
    }
    CloseConnection(fd);
    DestroyKeyValue();
    return 0;
}
