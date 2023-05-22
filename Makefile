TRANSMISSION_TEST = transmission_test
SAMPLE_TRANSMISSION_TEST = sample_transmission_test
GEN_RANDOM_KEY_VALUE = gen_random_key_value
CC = gcc
CFLAGS = -g -Wall  #-O3
LDFLAGS = -lpthread -lxxhash -lm -lhugetlbfs
DEFINE = -D_GNU_SOURCE

all : $(TRANSMISSION_TEST) $(SAMPLE_TRANSMISSION_TEST) $(GEN_RANDOM_KEY_VALUE)

$(TRANSMISSION_TEST) : transmission_test.c \
					   hashtable.o \
					   complete_bin_tree.o \
					   connection.o \
					   rng.o \
					   mt19937ar.o \
					   genzipf.o
	$(CC) $(CFLAGS) -o $@ $^  $(LDFLAGS) $(DEFINE)

$(SAMPLE_TRANSMISSION_TEST) : sample_transmission_test.c
	$(CC) $(CFLAGS) -o $@ $^

$(GEN_RANDOM_KEY_VALUE) : gen_random_key_value.c \
						  mt19937ar.o \
						  rng.o \
						  genzipf.o
	$(CC) $(CFLAGS) -o $@ $^ -lm

hashtable.o : hashtable.c
	$(CC) $(CFLAGS) -c -o $@ $^ 

complete_bin_tree.o : complete_bin_tree.c
	$(CC) $(CFLAGS) -c -o $@ $^

connection.o : connection.c
	$(CC) $(CFLAGS) -c -o $@ $^

mt19937ar.o : mt19937ar.c
	$(CC) $(CFLAGS) -c -o $@ $^

rng.o : rng.c
	$(CC) $(CFLAGS) -c -o $@ $^

genzipf.o : genzipf.c
	$(CC) $(CFLAGS) -c -o $@ $^

clean :
	rm -f $(TRANSMISSION_TEST) $(SAMPLE_TRANSMISSION_TEST) $(GEN_RANDOM_KEY_VALUE) *.o 
