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

static char *sample_key = "helloworld";
static char *sample_object;
static uint8_t app_recv_buf[1<<12];

static int CreateConnection(void);
static int SendGetRequest(const int fd);
static int ReceiveReply(const int fd);
static void CloseConnection(const int fd);

static void SetSampleObject(const ssize_t sample_object_size);
static void DestroySampleObject(void);

static in_port_t dport;
static in_addr_t daddr;

static void 
SetSampleObject(const ssize_t sample_object_size) {

    int i;
    sample_object = malloc(sample_object_size + 1);
    if (!sample_object) {
        log_error("malloc() error %s\n", strerror(errno));
        exit(EXIT_FAILURE);
    }

    for (i = 0; i <= SAMPLE_OBJECT_SIZE; i++)
        sample_object[i] = 'a';

    sample_object[SAMPLE_OBJECT_SIZE] = '\0';
}

static void
DestroySampleObject(void) {
    free(sample_object);
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
    }

    log_trace("connect() fd:%d,ret:%d, errno:%d\n", fd, ret, errno);

    return fd;

}

static int
SendGetRequest(const int fd)
{
    int ret;
    req_hdr req = {
        GET,
        strlen(sample_key)
    };

    struct iovec vec[2] = {
        {&req, 2},
        {sample_key, strlen(sample_key)}
    };

    log_trace("Send request packet\n");

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
    int len;
    
    len = read(fd, app_recv_buf, 1 << 12);

    if (len <= 0) {
        close(fd);
    }

    rep_hdr *rep = (rep_hdr *)app_recv_buf;
    if (rep->valLen != strlen(sample_object)) 
    {
        log_error("value length error, valLen:%u, object len:%lu\n",
                rep->valLen, strlen(sample_object));
        close(fd);
        return -1;
    }

    if (memcmp(rep->val, sample_object, strlen(sample_object)) != 0) 
    {
        char *ptr = (char *)rep->val;
        ptr[rep->valLen] = '\0';
        log_error("received object error, received:%s, sample object:%s\n",
                ptr, sample_object);
        close(fd);
        return -1;
    }

    return len; 
}

static void
CloseConnection(const int fd) {
    close(fd);
}

int 
main(const int argc, char *argv[]) {

    int fd, len, opt;
    ssize_t sample_object_size;
    daddr = inet_addr("10.0.30.110");
    dport = htons(65000);

    while((opt = getopt(argc, argv, "l:")) != -1)
    {
        switch(opt) {
            case 'l' :
                sample_object_size = atoi(optarg);
                break;
        }
    }

    SetSampleObject(sample_object_size);

    fd = CreateConnection();
    len = SendGetRequest(fd);
    log_trace("Send byte length : %d\n", len);
    len = ReceiveReply(fd);
    log_trace("Received byte length : %d\n", len);
    CloseConnection(fd);
    DestroySampleObject();
    return 0;
}
