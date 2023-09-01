#pragma once

#include <time.h>
#include <tuple>

#define NANOSECONDS_IN_SECOND 1000000000

// bandwidth
typedef struct
{
    int tid;
    uint64_t start_time;
    uint64_t end_time;
} threadargs_t;

__inline__ uint64_t nano_time(void)
{
    struct timespec ts;
    if (clock_gettime(CLOCK_REALTIME, &ts) == 0)
        return ts.tv_sec * NANOSECONDS_IN_SECOND + ts.tv_nsec;
}

// Finds the min start time and max end time for given set of times.
static std::tuple<uint64_t, uint64_t> time_min_max(threadargs_t *threadargs, int threads)
{
    uint64_t min_start_time = threadargs[0].start_time;
    uint64_t max_end_time = 0;
    for (int i = 0; i < threads; i++)
    {
        min_start_time = (threadargs[i].start_time < min_start_time) ? threadargs[i].start_time : min_start_time;
        max_end_time = (threadargs[i].end_time > max_end_time) ? threadargs[i].end_time : max_end_time;
    }
    return std::make_tuple(min_start_time, max_end_time);
}

__inline__ uint64_t rdtsc(void)
{
   uint64_t a, d;
   //        uint64_t cput_clock_ticks_per_ns = 2600000000000LL;
   double cput_clock_ticks_per_ns = 2.9; // 2.9 Ghz TSC
   //        _mm_lfence();
   //	__asm__ volatile ("lfence; rdtsc" : "=a" (a), "=d" (d));
   //        _mm_lfence();
   // Using rdtscp
   uint64_t c;
   __asm__ volatile("rdtscp"
                    : "=a"(a), "=c"(c), "=d"(d)
                    :
                    : "memory");
   return ((d << 32) | a) / cput_clock_ticks_per_ns;
}