#include "data_manager.h"
#include "unistd.h"
#include "fcntl.h"
#include <sys/stat.h>
#include <assert.h>
#include <sys/mman.h>
#include <x86intrin.h>
#include <iostream>

using std::string;
using std::vector;

#ifdef pmdk
#include <libpmem.h>
#endif

#ifdef __cplusplus
#include <stdint.h>
extern "C"
{
    void *__memmove_chk_avx512_no_vzeroupper(void *dest, void *src, size_t s);
}
#endif

// #define bandwidth true

// open and initialize files
DataManager::DataManager(vector<string> &file_list, Timer *timer_cpy) : pool(std::thread::hardware_concurrency())
{
    timer = timer_cpy;
    file_ptr_.resize(file_list.size());
    file_size_.resize(file_list.size());
    size_t rolling_sum = 0;

    for (size_t i = 0; i < file_list.size(); i++)
    {
        int fd = open(file_list[i].c_str(), O_RDWR | O_DIRECT);
        if (fd < 0)
        {
            printf("Couldn't open file %s: %s\n", file_list[i].c_str(), strerror(errno));
            exit(1);
        }
        file_ptr_[i] = fd;

        struct stat st;
        stat(file_list[i].c_str(), &st);
        off_t size = st.st_size;
        file_size_[i] = size;
        // This will hold the number of records in the file and file num
        file_index_.insert(std::make_pair(rolling_sum, i));
        rolling_sum += size / RECORD_SIZE;
    }
    file_index_.insert(std::make_pair(rolling_sum, -1));
}

// READ = type = 0, WRITE = type = 1.
int DataManager::MMapFile(size_t file_size, int type, int fd, char *&mapped_buffer)
{
    if (!type) // READ part
    {
        mapped_buffer = (char *)mmap(NULL, file_size, PROT_READ,
                                     MAP_SHARED, fd, 0);
        if (mapped_buffer == MAP_FAILED)
        {
            printf("Failed to mmap read file of size %lu: %s\n",
                   file_size, strerror(errno));
            return -1;
        }
        return 1;
    }
    else // WRITE part
    {
        mapped_buffer = (char *)mmap(NULL, file_size, PROT_WRITE,
                                     MAP_SHARED, fd, 0);
        if (mapped_buffer == MAP_FAILED)
        {
            printf("Failed to mmap write file of size %lu: %s\n",
                   file_size, strerror(errno));
            return -1;
        }
        return 1;
    }
}

// bandwidth
typedef struct
{
    int tid;
    uint64_t start_time;
    uint64_t end_time;
} threadargs_t;

uint64_t nano_time(void)
{
    struct timespec ts;
    if (clock_gettime(CLOCK_REALTIME, &ts) == 0)
        return ts.tv_sec * NANOSECONDS_IN_SECOND + ts.tv_nsec;
}

// Finds the min start time and max end time for given set of times.
std::tuple<uint64_t, uint64_t> time_min_max(threadargs_t *threadargs, int threads)
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

// Each block belongs to a paricular file, read from the right file.
// read_buff_B Number of blocks that fit the read buffer.
size_t DataManager::RunRead(size_t rec_num, size_t read_arr_count, std::vector<in_record_t> &keys_idx)
{
    // Find the file the block number belongs to
    auto file_it = file_index_.upper_bound(rec_num);
    file_it--;

    // rec belongs to no file
    if (file_it->second < 0)
        return 0;
    size_t file_num = file_it->second;  // 0
    off_t file_off = rec_num - file_it->first;  // 0
    // size_t file_num = 0;
    // size_t file_rec_num = file_size_[file_num] / conf.record_size;

    // 0 because READ
    if (!MMapFile(file_size_[file_num], 0, file_ptr_[file_num], input_mapped_buffer_))
        return -1;

    vector<std::future<size_t>> results;
    // read_arr_count = std::min(file_rec_num - rec_num, read_arr_count);
    size_t thrd_chunk_rec = read_arr_count / conf.read_thrds;
#ifdef bandwidth
    threadargs_t *threadargs;
    threadargs = (threadargs_t *)malloc(conf.read_thrds * sizeof(threadargs_t));
#endif
    // clang-format off
    for (int thrd = 0; thrd < conf.read_thrds; thrd++)
    {
        results.emplace_back(
            pool.enqueue([=, &keys_idx]()->size_t
            {
                size_t reads = 0;
                size_t read_keys = thrd_chunk_rec * thrd;
                size_t thrd_off = rec_num + thrd_chunk_rec * thrd;
#ifdef bandwidth
                threadargs[thrd].tid = thrd;
                threadargs[thrd].start_time = rdtsc();
#endif
                while (reads < thrd_chunk_rec)
                {
                    char* rec_tmp = input_mapped_buffer_ + thrd_off * conf.record_size;
                    memcpy((void *)(&keys_idx[read_keys]), rec_tmp, KEY_SIZE);
                    // v_off = (read_keys * conf.record_size) + // For each record
                    //         rec_num * conf.record_size;      // For values beyond first read buffer
                    keys_idx[read_keys].r_off = reinterpret_cast<record_t*>(rec_tmp);      // TODO: 此处数据类型对吗？右值可以不用指针吗？
                    read_keys++;
                    reads++;
                    thrd_off++;
                }
#ifdef bandwidth
                threadargs[thrd].end_time = rdtsc();
#endif
                return reads;           
            })
        );
    } 
    // clang-format on
    size_t total_read = 0;
    for (auto &&result : results)
        total_read += result.get();
    
    // while (total_read < read_arr_count)
    // {
    //     char* rec_tmp = input_mapped_buffer_ + total_read * conf.record_size;
    //     memcpy((void *)(&keys_idx[total_read]), rec_tmp, KEY_SIZE);
    //     keys_idx[total_read].r_off = reinterpret_cast<record_t*>(rec_tmp);      // TODO: 此处数据类型对吗？右值可以不用指针吗？
    //     total_read++;
    // }

#ifdef bandwidth
    timer->end("checkpoints");
    uint64_t min_start_time, max_end_time;
    std::tie(min_start_time, max_end_time) = time_min_max(threadargs, conf.read_thrds);
    printf("%f,READ,%f,%lu\n", timer->get_overall_time("checkpoints"), ((float)(max_end_time - min_start_time) / NANOSECONDS_IN_SECOND), read_arr_count * KEY_SIZE);
    fflush(stdout);
    free(threadargs);
    timer->start("checkpoints");
#endif
    return total_read;
}

// PMSort loading step
size_t DataManager::RunReadPMSort(size_t rec_num, size_t read_arr_count, std::vector<in_record_t> &keys_idx)
{
    // Find the file the block number belongs to
    auto file_it = file_index_.upper_bound(rec_num);
    file_it--;

    // rec belongs to no file
    if (file_it->second < 0)
        return 0;
    size_t file_num = file_it->second;
    off_t file_off = rec_num - file_it->first;

    // 0 because READ
    if (!MMapFile(file_size_[file_num], 0, file_ptr_[file_num], input_mapped_buffer_))
        return -1;

    vector<std::future<size_t>> results;
    size_t thrd_chunk_size = (read_arr_count * conf.record_size) / conf.read_thrds;
#ifdef bandwidth
    threadargs_t *threadargs;
    threadargs = (threadargs_t *)malloc(conf.read_thrds * sizeof(threadargs_t));
#endif
    // clang-format off
    for (int thrd = 0; thrd < conf.read_thrds; thrd++)
    {
        results.emplace_back(
            pool.enqueue([=, &keys_idx]()->size_t
            {
                size_t read_size = 0;
                size_t read_keys = (thrd_chunk_size/conf.record_size) * thrd;
                size_t v_off = 0;
                size_t thrd_file_off = file_off;
#ifdef bandwidth
                threadargs[thrd].tid = thrd;
                threadargs[thrd].start_time = rdtsc();
#endif
                char *tmp_buf = (char *) malloc(4000);
                while (read_size < thrd_chunk_size)
                {
                    // memcpy((void *)(&keys_idx[read_keys]),
                    //     &input_mapped_buffer_[thrd_file_off + (thrd_chunk_rec * conf.record_size * thrd)],
                    //     KEY_SIZE);
                    memcpy(tmp_buf, &input_mapped_buffer_[thrd_file_off + (thrd_chunk_size * thrd)],
                        4000);
                    for(int tmp_k = 0; tmp_k < 4000; tmp_k+=conf.record_size)
                    {
                        memcpy((void *) (&keys_idx[read_keys]), &tmp_buf[tmp_k], KEY_SIZE);
                        v_off = (read_keys * conf.record_size) + // For each record
                                rec_num * conf.record_size;      // For values beyond first read buffer
                        keys_idx[read_keys].r_off = (record_t*) v_off;
                        // Lets convert address to hex later for 
                        // now just append v_off to an array.
                        // tmp_off.push_back(v_off);
                        read_keys++;
                    }
                    read_size += 4000;
                    thrd_file_off += 4000;
                }
#ifdef bandwidth
                threadargs[thrd].end_time = rdtsc();
#endif
    /* This is for seperating reads and offset to hex convertion
                // reset read keys and reads
                read_keys = thrd_chunk_rec * thrd;
                reads = 0;
                // Now convert offset to hex
                while(reads < thrd_chunk_rec)
                {
                    keys_idx[read_keys].r_off = tmp_off[reads];
                    read_keys++;
                    reads++;
                }
        */
                return read_size/conf.record_size;         
            })
        );
    }
    // clang-format on
    size_t total_read = 0;
    for (auto &&result : results)
        total_read += result.get();

#ifdef bandwidth
    timer->end("checkpoints");
    uint64_t min_start_time, max_end_time;
    std::tie(min_start_time, max_end_time) = time_min_max(threadargs, conf.read_thrds);
    printf("%f,READ,%f,%lu\n", timer->get_overall_time("checkpoints"), ((float)(max_end_time - min_start_time) / NANOSECONDS_IN_SECOND), read_arr_count * KEY_SIZE);
    free(threadargs);
    timer->start("checkpoints");
#endif

    return total_read;
}

void DataManager::OpenAndMapOutputFile(string output_file, size_t file_size)
{
    int ofd = open(output_file.c_str(), O_RDWR | O_CREAT | O_DIRECT, S_IRWXU);
    if (ofd < 0)
    {
        printf("Couldn't open file %s: %s\n", output_file.c_str(), strerror(errno));
        exit(1);
    }
    assert(ftruncate(ofd, file_size) == 0);
    output_fd_ = ofd;
    if (!MMapFile(file_size, 1, ofd, output_mapped_buffer_))
        exit(1);
    output_count_ = 0;
}

// The responsiblity of this function is to simply write to file from write buffer
// Must write all read/sorted data
// Assuming everythign is aligned
size_t DataManager::RunWrite(vector<in_record_t> keys_idx, size_t read_recs)
{
    vector<std::future<size_t>> results;
    size_t thrd_chunk_rec = read_recs / conf.write_thrds;

#ifdef bandwidth
    threadargs_t *threadargs;
    threadargs = (threadargs_t *)malloc(conf.read_thrds * sizeof(threadargs_t));
#endif
    // clang-format off
    for (int thrd = 0; thrd < conf.write_thrds; thrd++)
    {
        results.emplace_back(
            pool.enqueue([=, &keys_idx]
            {
                size_t pending_recs = 0;
                size_t writes = 0;
                size_t write_records = thrd_chunk_rec * thrd;
                size_t blk_idx = thrd_chunk_rec * conf.idx_record_size * thrd;
#ifdef bandwidth
                threadargs[thrd].tid = thrd;
                threadargs[thrd].start_time = rdtsc();
#endif
                while (writes < thrd_chunk_rec)
                {
                    pending_recs = thrd_chunk_rec - writes;
                    if (pending_recs < conf.idx_recs_per_blk)
                    {
                        // This is the last write which is smaller than idx_recs_per_blk
                        memcpy(&output_mapped_buffer_[blk_idx],
                            &keys_idx[write_records], pending_recs * conf.idx_record_size);
                        write_records += pending_recs;
                        blk_idx += pending_recs * conf.idx_record_size;
                        writes += pending_recs;
                        break;
                    }
#ifdef avx512
                    __memmove_chk_avx512_no_vzeroupper(&output_mapped_buffer_[blk_idx],
                        &keys_idx[write_records], conf.block_size);
#elif pmdk
                    pmem_memcpy_nodrain(&output_mapped_buffer_[blk_idx],
                        &keys_idx[write_records], conf.block_size);
#else
                    memcpy(&output_mapped_buffer_[blk_idx],
                        &keys_idx[write_records], conf.block_size);
#endif                        
                    blk_idx += conf.block_size;
                    write_records += conf.idx_recs_per_blk;
                    writes += conf.idx_recs_per_blk;
                }
#ifdef bandwidth
                threadargs[thrd].end_time = rdtsc();
#endif
                return writes;
            })
        );
    }
    // clang-format on
    size_t total_write = 0;
    for (auto &&result : results)
        total_write += result.get();

    // if (total_write < read_recs) {
    //     size_t pending_recs = read_recs - total_write;
    //     size_t blk_idx = total_write * conf.idx_record_size;
    //     memcpy(&output_mapped_buffer_[blk_idx],
    //         &keys_idx[total_write], pending_recs * conf.idx_record_size);
    //     total_write += pending_recs;
    // }

#ifdef bandwidth
    timer->end("checkpoints");
    uint64_t min_start_time, max_end_time;
    std::tie(min_start_time, max_end_time) = time_min_max(threadargs, conf.write_thrds);
    printf("%f,WRITE,%f,%lu\n", timer->get_overall_time("checkpoints"), ((float)(max_end_time - min_start_time) / NANOSECONDS_IN_SECOND), read_recs * conf.idx_record_size);
    fflush(stdout);
    free(threadargs);
    timer->start("checkpoints");
#endif
    return total_write;
}

// Read records from RUN file to the appropriate buffer offset
size_t DataManager::MergeRead(in_record_t *read_buffer, size_t read_buf_rec_off,
                              char *run_file_map, size_t file_off,
                              size_t read_length_blk)
{
    vector<std::future<size_t>> results;
    // FIXME: potential bug since number of blocks don't evenly divide the number of threads.
    size_t thrd_chunk_blk = read_length_blk / conf.read_thrds;
#ifdef bandwidth
    threadargs_t *threadargs;
    threadargs = (threadargs_t *)malloc(conf.read_thrds * sizeof(threadargs_t));
#endif
    // clang-format off
    for (int thrd = 0; thrd < conf.read_thrds; thrd++)
    {
        results.emplace_back(
            pool.enqueue([=, &read_buffer, &run_file_map]()->size_t
            {
                size_t read_recs = 0;
                size_t pending_recs_sz = 0;
                size_t thrd_file_off = (file_off + thrd_chunk_blk * thrd) * conf.block_size ;
                size_t thrd_read_buf_rec_off = read_buf_rec_off + thrd_chunk_blk * conf.idx_recs_per_blk * thrd;
#ifdef bandwidth
                threadargs[thrd].tid = thrd;
                threadargs[thrd].start_time = rdtsc();
#endif
                while (read_recs < thrd_chunk_blk * conf.idx_recs_per_blk)
                {
                    pending_recs_sz = (thrd_chunk_blk * conf.block_size - read_recs * conf.idx_record_size);
                    if(conf.block_size > pending_recs_sz)
                    {
                        memcpy((void *)(read_buffer + thrd_read_buf_rec_off),
                                &run_file_map[thrd_file_off],
                                pending_recs_sz);
                        read_recs += pending_recs_sz/conf.idx_record_size;
                        break;
                    }
                    // FIXME: What if the file does not have conf.block_size of data anymore
#ifdef avx512
                    __memmove_chk_avx512_no_vzeroupper((void *)(read_buffer + thrd_read_buf_rec_off),
                                                    &run_file_map[thrd_file_off],
                                                    conf.block_size);
// #elif pmdk
//                   pmem_memcpy_nodrain((void *)(read_buffer + thrd_read_buf_rec_off),
//                                       &run_file_map[thrd_file_off],
//                                       conf.block_size);
#else
                    // printf("[%d] thrd_read_buf_rec_off: %lu, thrd_file_off: %lu \n", thrd, thrd_read_buf_rec_off, thrd_file_off);
                    // printf("[%d] read: %x\n", thrd, read_buffer[thrd_read_buf_rec_off + conf.idx_recs_per_blk].k.key);
                    // printf("[%d] file: %x\n", thrd, run_file_map[thrd_file_off+conf.block_size]);
                    memcpy((void *)(read_buffer + thrd_read_buf_rec_off),
                        &run_file_map[thrd_file_off],
                        conf.block_size);
#endif
                    thrd_file_off += conf.block_size;
                    read_recs += conf.idx_recs_per_blk;
                    thrd_read_buf_rec_off += conf.idx_recs_per_blk;
                }
#ifdef bandwidth
                threadargs[thrd].end_time = rdtsc();
#endif
                return read_recs;
            })
        );
    }
    // clang-format on
    size_t total_read = 0;
    for (auto &&result : results)
        total_read += result.get();
    
    // size_t total_blk = total_read / conf.idx_recs_per_blk;
    // while (total_blk < read_length_blk) {
    //     size_t thrd_file_off = (file_off + total_blk) * conf.block_size ;
    //     size_t thrd_read_buf_rec_off = read_buf_rec_off + total_read;
    //     // 此处默认runfile是能完整被block切分的
    //     memcpy((void *)(read_buffer + thrd_read_buf_rec_off),
    //         &run_file_map[thrd_file_off], conf.block_size);
    //     total_blk++;
    //     total_read += conf.idx_recs_per_blk;
    // }

#ifdef bandwidth
    timer->end("checkpoints");
    uint64_t min_start_time, max_end_time;
    std::tie(min_start_time, max_end_time) = time_min_max(threadargs, conf.read_thrds);
    printf("%f,READ,%f,%lu\n", timer->get_overall_time("checkpoints"), ((float)(max_end_time - min_start_time) / NANOSECONDS_IN_SECOND), read_length_blk * conf.block_size);
    fflush(stdout);
    free(threadargs);
    timer->start("checkpoints");
#endif
    return total_read;
}

size_t DataManager::MergeWrite(record_t *write_buffer, size_t write_len_recs)
{
    // 此处需要写入的recs num和thread num必须是整除的关系；每个thread待处理的blk可以不整除
    vector<std::future<size_t>> results;
    size_t write_recs = write_len_recs / conf.write_thrds;
    size_t thrd_blk = write_recs / conf.recs_per_blk;
    size_t thrd_blk_extra = write_recs % conf.recs_per_blk;
#ifdef bandwidth
    threadargs_t *threadargs;
    threadargs = (threadargs_t *)malloc(conf.read_thrds * sizeof(threadargs_t));
#endif
    // clang-format off
    for (int thrd = 0; thrd < conf.write_thrds; thrd++)
    {
        results.emplace_back(
            pool.enqueue([=]
            {
                size_t writes = 0;
                size_t wrt_recs = write_recs * thrd;
                size_t ofile_off = output_file_off_ + 
                                write_recs * conf.record_size * thrd;
#ifdef bandwidth
                threadargs[thrd].tid = thrd;
                threadargs[thrd].start_time = rdtsc();
#endif
                while (writes < thrd_blk * conf.recs_per_blk)
                {
#ifdef avx512
                    __memmove_chk_avx512_no_vzeroupper(&output_mapped_buffer_[ofile_off],
                                                    &write_buffer[wrt_recs], conf.block_size_2);
#elif pmdk
                    pmem_memcpy_nodrain(&output_mapped_buffer_[ofile_off],
                                        &write_buffer[wrt_recs], conf.block_size_2);
#else
                    memcpy(&output_mapped_buffer_[ofile_off],
                        &write_buffer[wrt_recs], conf.block_size_2);
#endif
                    ofile_off += conf.block_size_2;
                    writes += conf.recs_per_blk;
                    wrt_recs += conf.recs_per_blk;
                }

                // extra
#ifdef avx512
                __memmove_chk_avx512_no_vzeroupper(&output_mapped_buffer_[ofile_off],
                                                &write_buffer[wrt_recs], thrd_blk_extra * conf.record_size);
#elif pmdk
                pmem_memcpy_nodrain(&output_mapped_buffer_[ofile_off],
                                    &write_buffer[wrt_recs], thrd_blk_extra * conf.record_size);
#else
                memcpy(&output_mapped_buffer_[ofile_off],
                    &write_buffer[wrt_recs], thrd_blk_extra * conf.record_size);
#endif
                ofile_off += thrd_blk_extra * conf.record_size;
                writes += thrd_blk_extra;
                wrt_recs += thrd_blk_extra;

#ifdef bandwidth
                threadargs[thrd].end_time = rdtsc();
#endif
                return writes;                
            })
        );
    }
    // clang-format on
    size_t total_write = 0;
    for (auto &&result : results)
        total_write += result.get();
    
    // if (total_write < write_len_recs) {
    //     size_t ofile_off = output_file_off_ + total_write * conf.idx_record_size;
    //     memcpy(&output_mapped_buffer_[ofile_off],
    //         &write_buffer[total_write], (write_len_recs - total_write) * conf.idx_record_size);
    //     total_write += (write_len_recs - total_write);
    // }

#ifdef bandwidth
    timer->end("checkpoints");
    uint64_t min_start_time, max_end_time;
    std::tie(min_start_time, max_end_time) = time_min_max(threadargs, conf.write_thrds);
    printf("%f,WRITE,%f,%lu\n", timer->get_overall_time("checkpoints"), ((float)(max_end_time - min_start_time) / NANOSECONDS_IN_SECOND), write_len_recs * conf.record_size);
    fflush(stdout);
    free(threadargs);
    timer->start("checkpoints");
#endif
    // Update output file offset
    output_file_off_ += write_len_recs * conf.record_size;
    return total_write;
}

void DataManager::MergeRandomRead(record_t *write_buffer, vector<read_offs> &offset_vec)
{
    vector<std::future<void>> results;
    size_t thread_chunk_rec = offset_vec.size() / conf.read_thrds;
#ifdef bandwidth
    threadargs_t *threadargs;
    threadargs = (threadargs_t *)malloc(conf.read_thrds * sizeof(threadargs_t));
#endif
    // clang-format off
    for(int thrd=0; thrd < conf.read_thrds; thrd++)
    {
        results.emplace_back(
            pool.enqueue([=, &offset_vec]()
            {
	    	    size_t read_idx = 0;
                size_t reads = thread_chunk_rec * thrd;
#ifdef bandwidth
                threadargs[thrd].tid = thrd;
                threadargs[thrd].start_time = rdtsc();
#endif
                while(read_idx < thread_chunk_rec)
                {
#ifdef avx512
                    __memmove_chk_avx512_no_vzeroupper(&write_buffer[offset_vec[reads].output_buff_off],
                        offset_vec[reads].input_off,
                        conf.record_size);
// #elif pmdk
//                     pmem_memcpy_nodrain(&write_buffer[offset_vec[reads].output_buff_off],
//                         offset_vec[reads].input_off,
//                         conf.record_size);
#else
                    memcpy(&write_buffer[offset_vec[reads].output_buff_off],
                        offset_vec[reads].input_off,
                        conf.record_size);
#endif
                    read_idx++;
		            reads++;
                }
#ifdef bandwidth
                threadargs[thrd].end_time = rdtsc();
#endif
            })
        );
    }
    // clang-format on
    for (auto &&result : results)
        result.get();
#ifdef bandwidth
    timer->end("checkpoints");
    uint64_t min_start_time, max_end_time;
    std::tie(min_start_time, max_end_time) = time_min_max(threadargs, conf.read_thrds);
    printf("%f,READ,%f,%lu\n", timer->get_overall_time("checkpoints"), ((float)(max_end_time - min_start_time) / NANOSECONDS_IN_SECOND), conf.write_buffer_size);
    fflush(stdout);
    free(threadargs);
    timer->start("checkpoints");
#endif
}

