#include <vector>
#include <string>
#include <map>
#include "record_split.h"
#include "thread_pool.h"
#include "timer.h"

#define NANOSECONDS_IN_SECOND 1000000000

class DataManager
{
public:
   Timer *timer;
   ThreadPool pool;
   std::vector<int> file_ptr_;
   std::vector<uint64_t> file_size_;
   std::map<size_t, int> file_index_;

   char *input_mapped_buffer_ = NULL;
   char *output_mapped_buffer_ = NULL;

   DataManager(std::vector<std::string> &file_list, Timer *timer_cpy);
   size_t RunRead(size_t block_num, size_t len, std::vector<in_record_t> &keys_idx);
   size_t RunReadPMSort(size_t rec_num, size_t read_arr_count, std::vector<in_record_t> &keys_idx);
   void OpenAndMapOutputFile(std::string output_file, size_t file_size);
   size_t RunWrite(std::vector<in_record_t> keys_idx, size_t read_recs);
   int MMapFile(size_t file_size, int type, int fd, char *&mapped_buffer);
   size_t MergeRead(in_record_t *read_buffer, size_t read_buf_rec_off,
                    char *run_file_map, size_t file_off,
                    size_t read_length);
   void MergeRandomRead(record_t *write_buffer, std::vector<read_offs> &offset_vec);
   size_t MergeWrite(in_record_t *write_buffer, size_t write_len_recs);
   size_t MergeWrite(record_t *write_buffer, size_t write_len_recs);

private:
   int output_fd_ = 0;
   size_t output_count_ = 0;
   size_t output_file_off_ = 0;
};

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