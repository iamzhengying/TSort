CC := gcc
LIBS := -lstdc++ -lpmem -lm -pthread -ltbb -latomic -fopenmp
CFLAGS := -std=c++17 -Wall -m64 #-O3 #-g3 # -fno-stack-protector -Wwrite-strings
APPS := count

all: $(APPS)

count: ips4o_write.cpp
	$(CC) ${CFLAGS} -I../include/ -I./ips4o_write/include/ -o $@ $^ ${LIBS}

clean:
	rm -f *.o $(APPS)