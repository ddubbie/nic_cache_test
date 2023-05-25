#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stddef.h>
#include <xxhash.h>
#include <stdbool.h>

#include "rng.h"

static void GenerateRandomString(char *buf, const ssize_t buflen, const ssize_t keyLen);

static void
GenerateRandomString(char *buf, const ssize_t buflen, const ssize_t keyLen) 
{
    int i, rn1, rn2;

    if (buflen < keyLen)  {
        perror("buflen < keyLen");
        return;
    }

    buf[keyLen] = '\0';

    for (i = 0; i < keyLen; i++) {
        rn1 = rng_int32() % 2;
        rn2 = rng_int32() % 26;

        if (rn1) {
            buf[i] = 'a' + rn2;
        } else {
            buf[i] = 'A' + rn2;
        }
    }
}
static void
PrintOption(void) {

    printf("-n : number of key-value tuples to generate\n" \
           "-k : maximum key size\n" \
           "-v : maximum value size\n"  \
           "-u : generate uniform sized value\n");
}
/* 64 704*/
int
main(const int argc, char *argv[])
{
    int opt;
    int num_tuples;
    FILE *file;
    int i;
    unsigned short max_keyLen, keyLen;
    unsigned int max_valueLen, valueLen;
    char key_[1024];
    char value_[8192];
    bool uniform_value_size = false;

    if (argc != 7 && argc != 8) {
        if (argc != 2) {
            fprintf(stderr, "# of arguments error : %d\n", argc);
            return -1;
        }
    }

    while ((opt = getopt(argc, argv, "n:v:k:hu")) != -1)
    {
        switch(opt) {
            case 'n' :
                num_tuples = atoi(optarg);
                break;
            case 'v' :
                max_valueLen = atoi(optarg);
                break;
            case 'k' :
                max_keyLen = atoi(optarg);
                break;
            case 'u' :
                uniform_value_size = true;
                break;
            case 'h' :
                PrintOption();
                break;

            default :
                printf("Wrong option %c, enter -h option for help\n", (char)opt);
                return -1;
        }
    }

    if(!(file = fopen("sample_key_value.txt", "w"))) 
    {
        perror("fopen() error");
        return -1;
    }

    for (i = 0; i < num_tuples; i++) {
        keyLen =  rng_gev(30.7984, 8.20449, 0.078688) % (max_keyLen - 1) + 1;
        valueLen = uniform_value_size? max_valueLen :
                                       rng_gpd(0, 214.476, 0.348238) % max_valueLen;
        GenerateRandomString(key_, 1024, keyLen);
        GenerateRandomString(value_, 8192, valueLen);

        fprintf(file, "%u,%s,%u,%s\n", keyLen, key_, valueLen, value_);
    }

    fclose(file);

    return 0;
}
