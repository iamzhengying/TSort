CC := gcc
LIBS := -lstdc++ -lpmem -lm -pthread -ltbb -latomic -fopenmp
CFLAGS := -std=c++17 -Wall -m64 -O3 #-g3 # -fno-stack-protector -Wwrite-strings
APPS := sort_gensort

all: $(APPS)

sort_gensort: ./*.cpp ../utils/*
	$(CC) ${CFLAGS} -I../include/ -I../include/ips4o/include/ -o $@ $^ ${LIBS}

clean:
	rm -f *.o $(APPS)