#include "utils.h"

struct timespec startTimer, endTimer;
double total_latency;
uint32_t total_count;

void singleLatency() {
    double latency = (endTimer.tv_sec - startTimer.tv_sec) * NS_RATIO + (double)(endTimer.tv_nsec - startTimer.tv_nsec) / 1000;
    printf("latency: %f (ms)\n", latency / 1000);
    // return latency;
}

void addToTotalLatency(double* total_latency) {
    double latency = (endTimer.tv_sec - startTimer.tv_sec) * NS_RATIO + (double)(endTimer.tv_nsec - startTimer.tv_nsec) / 1000;
    *total_latency += latency;
}

void printTotalLatency(double* total_latency) {
    printf("total latency: %f (ms)\n", *total_latency/1000);
}

double addToTotalLatency() {
    double latency = (endTimer.tv_sec - startTimer.tv_sec) * NS_RATIO + (double)(endTimer.tv_nsec - startTimer.tv_nsec) / 1000;
    total_latency += latency;
    total_count++;
    return total_latency / 1000000;  // unit: second  
}

double printTotalLatency() {
    printf("total latency (ms): %f\n", total_latency/1000);
    return total_latency;
}

void resetTotalLatency() {
    total_latency = 0;
    total_count = 0;
}

double printAverageLatency() {
    double average_lat = total_latency / total_count;
    printf("average latency (us): %f\n", average_lat);
    return average_lat;
}

double printThroughput() {
    double throughput = total_count / total_latency;
    printf("throughput (MOps/s): %f\n", throughput);
    return throughput;
}
