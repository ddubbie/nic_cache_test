TRANSMISSION_TEST = transmission_test
SAMPLE_TRANSMISSION_TEST = sample_transmission_test
CC = gcc
CFLAGS = -g -Wall
LDFLAGS = -lpthread -lxxhash -lm -lhugetlbfs

$(TRANSMISSION_TEST) : transmission_test.c \
					   hashtable.o \
					   complete_bin_tree.o \
					   connection.o
	$(CC) $(CFLAGS) -o $@ $^  $(LDFLAGS)

$(SAMPLE_TRANSMISSION_TEST) : sample_transmission_test.c
	$(CC) $(CFLAGS) -o $@ $^

hashtable.o : hashtable.c
	$(CC) $(CFLAGS) -c -o $@ $^ 

complete_bin_tree.o : complete_bin_tree.c
	$(CC) $(CFLAGS) -c -o $@ $^

connection.o : connection.c
	$(CC) $(CFLAGS) -c -o $@ $^

clean :
	rm -f $(TRANSMISSION_TEST) $(SAMPLE_TRANSMISSION_TEST) *.o 
