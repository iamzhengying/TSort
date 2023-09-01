#include "ext_sort.h"
#include <chrono>
#include <sys/types.h>
#include <sys/stat.h>
#include <assert.h>
#include <algorithm>
#include <sys/mman.h>
#include "timer.h"
#include <iostream>

#include "ips4o.hpp"
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

#define min(a, b) (((a) < (b)) ? (a) : (b))

// #define bandwidth true

Timer timer;
ExtSort::ExtSort(string mount_path)
{
    folder_path_ = mount_path + std::to_string(std::time(nullptr));
    mkdir(folder_path_.c_str(), 0777);
}

// return number of records processed
size_t ExtSort::InMemorySort(size_t rec_off, DataManager &data_manger, string output_file)
{
    vector<in_record_t> keys_idx;
    keys_idx.resize(conf.read_buffer_size / conf.idx_record_size);

    timer.start("RUN read");
    size_t read_records = data_manger.RunRead(rec_off, conf.read_buffer_size / conf.idx_record_size, keys_idx);
    // size_t read_records = data_manger.RunReadPMSort(rec_off, conf.read_buffer_size / conf.idx_record_size, keys_idx);
    timer.end("RUN read");

    if (!read_records)
        return 0;
    // printf("Finished reading!\n");

    timer.start("SORT");
// #ifdef bandwidth
//     uint64_t sort_start_time = rdtsc();
// #endif
    ips4o::parallel::sort(keys_idx.begin(), keys_idx.begin() + read_records, std::less<>{});
// #ifdef bandwidth
//     uint64_t sort_delta = rdtsc() - sort_start_time;
//     timer.end("checkpoints");
//     printf("%f,SORT,%f,%lu\n", timer.get_overall_time("checkpoints"), ((float)(sort_delta) / NANOSECONDS_IN_SECOND), keys_idx.size() * (KEY_SIZE + INDEX_SIZE));
//     timer.start("checkpoints");
// #endif
    timer.end("SORT");
    // printf("Finished sorting %lu!\n", read_records);

    // create a file equal to the length of the index
    data_manger.OpenAndMapOutputFile(output_file, read_records * conf.idx_record_size);

    size_t write_records = 0;
    // Write all the read records to a file directly from the sorted array.
    timer.start("RUN write");
    write_records += data_manger.RunWrite(keys_idx, read_records);
    timer.end("RUN write");

    return write_records;
}

vector<string> ExtSort::RunGeneration(vector<string> files)
{
    DataManager data_manager(files, &timer);
    uint64_t rec_off = 0;
    string file_name = folder_path_ + "/run_";
    size_t run_num = 0;
    vector<string> run_names;
    // REVIEW: Loop until all data is processed? ex: index >= total number of records?
    while (1)
    {
        uint64_t rec_processed = InMemorySort(rec_off, data_manager,
                                              file_name + std::to_string(run_num));
        if (!rec_processed)
            break;
        rec_off += rec_processed;
        run_names.push_back(file_name + std::to_string(run_num));
        run_num++;
    }
    return std::move(run_names);
}

void ExtSort::MergeRuns(vector<string> runs, string input_file)
{
    // Game plan!
    // 1. Open all run files and create a new file of size sum of all runs.
    // 2. Create a read buffer that is evenly divisible by the number of files
    // 3. Create current pointer and end pointers (or atleast know end for each file's buffer)
    // 4a. Open the orginal input_file and mmap it
    // 4b. First mmap runs and fill the read buffer from their respective files.
    // 5. find min of keys pointed by current pointers (ideally a min on list of (key,file_num)
    // 6. Memcpy the offset from input_file to output_buffer directly
    // 6. If output buffer is full flush it and reset pointer.
    // 7. increment the pointer of the respective run_buffer
    // 8. if current pointer reach the end pointer load the new data to the part of that buffer.
    // 9. if the last block of a file is read and current pointer for that file buffer is at the
    // end then stop doing reads.
    // 10. If only one file buffer hasn't reached it's last block then just copy the rest of that
    // file to the output buffer.

    // [1]
    DataManager data_manager(runs, &timer);
    string output_file_name = folder_path_ + "/sorted";
    size_t output_file_size = 0;
    for (auto i : data_manager.file_size_)
        output_file_size += ((i / conf.idx_record_size) * conf.record_size);
    data_manager.OpenAndMapOutputFile(output_file_name, output_file_size);
    // Here the write_buffer size should evenly divide the output_file_size
    conf.write_buffer_size = ((conf.write_buffer_size / runs.size()) / conf.block_size_2) * conf.block_size_2 * runs.size();
    record_t *write_buffer = (record_t *)malloc(conf.write_buffer_size);    // malloc uses bytes
    assert(write_buffer);

    // [2]
    // The read buffer must be evenly divisible by runs.size() and IDX_BLOCK_SIZE
    size_t READ_BUFFER_SIZE_M = ((conf.merge_read_buffer_size / runs.size()) / conf.block_size / conf.read_thrds) * conf.block_size * runs.size() * conf.read_thrds;
    in_record_t *read_buffer = (in_record_t *)malloc(READ_BUFFER_SIZE_M);
    assert(read_buffer);

    // [3]
    vector<size_t> cur_rec_off(runs.size(), 0);   // Holds the rec offset of key that needs comparison per file
    vector<size_t> end_rec_off(runs.size(), 0);   // Holds the last rec offset of the space allocated per file
    vector<size_t> rfile_blk_off(runs.size(), 0); // Has the current blk offset of the file which is to be read
    vector<char *> mapped_run_files(runs.size()); // mmap all the run files

    size_t space_per_run = READ_BUFFER_SIZE_M / runs.size();    // unit: byte

    // [4a] - REVIEW: Perhaps just pass run's data_manager object where file is already opened and mapped?
    // open file and mmap it
    int fd = open(input_file.c_str(), O_RDWR | O_DIRECT);
    if (fd < 0)
    {
        printf("Couldn't open input file %s: %s\n", input_file.c_str(), strerror(errno));
        exit(1);
    }
    struct stat st;
    stat(input_file.c_str(), &st);
    // mmap file
    if (!data_manager.MMapFile(st.st_size, 0, fd, data_manager.input_mapped_buffer_))
        exit(1);

    // [4b]
    size_t read_records = 0;
    size_t read_size_blk = space_per_run / conf.block_size;     // unit: 1, 每个run要读取的blk个数
    for (uint64_t file_num = 0; file_num < runs.size(); ++file_num)
    {
        // // Initialize pointers
        cur_rec_off[file_num] = file_num * (space_per_run / conf.idx_record_size);
        end_rec_off[file_num] = (file_num + 1) * (space_per_run / conf.idx_record_size) - 1;

        // mmap files
        data_manager.MMapFile(data_manager.file_size_[file_num], 0,
                              data_manager.file_ptr_[file_num], mapped_run_files[file_num]);

        // // Initialize pointers
        // cur_rec_off[file_num] = file_num * (space_per_run / conf.idx_record_size);
        // // Read the next set of blocks from the file
        // size_t read_size = min(read_size_blk, 
        //                     data_manager.file_size_[file_num] / conf.block_size - 
        //                     rfile_blk_off[file_num]);
        // end_rec_off[file_num] = cur_rec_off[file_num] +  
        //                         ((read_size * conf.block_size) / conf.idx_record_size) - 1;
        size_t read_size = read_size_blk;
        timer.start("MERGE read");
        data_manager.MergeRead(read_buffer, cur_rec_off[file_num],
                                mapped_run_files[file_num], rfile_blk_off[file_num],
                                read_size);
        timer.end("MERGE read");
        
#ifdef checkpoints
        timer.end("checkpoints");
        printf("MERGE seq read: %f\n", timer.get_overall_time("checkpoints"));
        timer.start("checkpoints");
#endif
        rfile_blk_off[file_num] += read_size;
    }

    vector<int64_t> min_vec(runs.size());
    // Load the vec with first key from each run
    for (uint64_t file_num = 0; file_num < runs.size(); ++file_num)
    {
        min_vec[file_num] = read_buffer[cur_rec_off[file_num]].k;
    }

    size_t write_buff_rec_off = 0;      // unit: 1, 统计record个数
    uint64_t min_index = 0;
    size_t recs_written = 0;
    size_t run_file_sz = 0;
    record_t* input_file_off;
    int64_t tmp_k;
    tmp_k = UINT32_MAX;
    bool random_reads_complete = false;
    vector<read_offs> off_vec;
    // Loop until all the recs are written
    while (recs_written < output_file_size / conf.record_size)
    {
        // [5]
        min_index = std::min_element(min_vec.begin(), min_vec.end()) - min_vec.begin();

        // [6] // This is the additional random read to fetch the value.
        input_file_off = read_buffer[cur_rec_off[min_index]].r_off;

        // check if all RUN files reached the end
        if (count(min_vec.begin(), min_vec.end(), tmp_k) == (uint64_t)runs.size())
        {
            // What if write buffer is not completely full but all records are processed.
            // Here min_vec indicates all the records are processed for all runs
            // Dump remaining write_buffer and break
            timer.start("RECORD read");
            data_manager.MergeRandomRead(write_buffer, off_vec);
            timer.end("RECORD read");
            timer.start("MERGE write");
            recs_written += data_manager.MergeWrite(write_buffer, write_buff_rec_off);
            timer.end("MERGE write");
#ifdef checkpoints
            timer.end("checkpoints");
            printf("MERGE write: %f\n", timer.get_overall_time("checkpoints"));
            timer.start("checkpoints");
#endif
            break;
        }

        /**
         * @brief List all the offsets reads must be made to and then submit
         * this to threadpool at once. This is to avoid making random read blocking.
         */

        if (write_buff_rec_off < conf.write_buffer_size / conf.record_size)
        {
            // submit job
            off_vec.emplace_back((read_offs){input_file_off, write_buff_rec_off});
            write_buff_rec_off++;
        }

        if (write_buff_rec_off >= conf.write_buffer_size / conf.record_size)
        {
            // Submit task for random reads and wait until complete
            timer.start("RECORD read");
            data_manager.MergeRandomRead(write_buffer, off_vec);
            timer.end("RECORD read");
            // clearing queue for next set of offsets.
            off_vec.clear();
            random_reads_complete = true;
        }

        // Write buffer is full so flush it.
        if (write_buff_rec_off >= conf.write_buffer_size / conf.record_size && random_reads_complete == true)
        {
            // printf("The write_buff_rec_off is %lu\n", write_buff_rec_off);
            timer.start("MERGE write");
            recs_written += data_manager.MergeWrite(write_buffer, write_buff_rec_off);
            timer.end("MERGE write");
#ifdef checkpoints
            timer.end("checkpoints");
            printf("MERGE write: %f\n", timer.get_overall_time("checkpoints"));
            timer.start("checkpoints");
#endif
            write_buff_rec_off = 0;
            random_reads_complete = false;
        }

        // [7]
        // Now replace min_vec with new value from respective buffer space.
        cur_rec_off[min_index]++;
        min_vec[min_index] = read_buffer[cur_rec_off[min_index]].k;

        // Now check if corresponding chunk has reached the end
        if (cur_rec_off[min_index] > end_rec_off[min_index])
        {
            // [9]
            // Also see if you have hit the end of file for that run
            run_file_sz = data_manager.file_size_[min_index] / conf.block_size;
            if (rfile_blk_off[min_index] >= run_file_sz)
            {
                // [10]
                // if (count(min_vec.begin(), min_vec.end(), tmp_k) == (uint64_t)runs.size() - 1)
                // {
                //     for (uint64_t file_id = 0; file_id < min_vec.size(); ++file_id)
                //     {
                //         if (!(min_vec[file_id] == tmp_k))
                //         {
                //             // First flush the write_buffer to file
                //             // Read data to read buffer and write it directly to outputfile
                //             recs_written += data_manager.MoveRemainingInputToOuputBuff();
                //             break;
                //         }
                //     }
                // }

                // if you reached the end of a file then set a max value for it in min_vec index.
                min_vec[min_index] = tmp_k; // 0xffffffffffffffffffff
            }
            else
            {
                // [8]
                // reset current rec offset for that RUN
                cur_rec_off[min_index] = min_index * (space_per_run / conf.idx_record_size);
                // Read the next set of blocks from the file
                size_t read_size = min(read_size_blk, run_file_sz - rfile_blk_off[min_index]);
                timer.start("MERGE read");
                data_manager.MergeRead(read_buffer, cur_rec_off[min_index],
                                       mapped_run_files[min_index], rfile_blk_off[min_index],
                                       read_size);
                timer.end("MERGE read");
#ifdef checkpoints
                timer.end("checkpoints");
                printf("MERGE read: %f\n", timer.get_overall_time("checkpoints"));
                timer.start("checkpoints");
#endif
                // Update end record in the case where read_buffer is not completely full
                // because no more records to read from for that RUN file.
                end_rec_off[min_index] = cur_rec_off[min_index] + 
                                    ((read_size * conf.block_size) / conf.idx_record_size) - 1;
                rfile_blk_off[min_index] += read_size;

                min_vec[min_index] = read_buffer[cur_rec_off[min_index]].k;
            }
        }
    }

    // printf("====================================:\n");
    // const bool sorted = std::is_sorted((record_t*)data_manager.output_mapped_buffer_, 
    //                                 ((record_t*)data_manager.output_mapped_buffer_) + output_file_size / conf.record_size,
    //                                 std::less<>{});
    
    // std::cout << "Check manually: ";
    // int64_t tmp = 0;
    // bool flag = true;
    // for (size_t i = 0; i < output_file_size / conf.record_size; ++i) {
    //     if (((record_t*)data_manager.output_mapped_buffer_)[i].k < tmp) {
    //         std::cout << "Wrong !! (between " << i-1 << "and " << i << ")" << std::endl;
    //         flag = false;
    //         // break;
    //     }
    //     tmp = ((record_t*)data_manager.output_mapped_buffer_)[i].k;
    // }
    // if (flag)
    //     std::cout << "Correct !!" << std::endl;

    // std::cout << "Elements are sorted: " << std::boolalpha << sorted << std::endl;
    // std::cout << " -- The first element is: " << ((record_t*)data_manager.output_mapped_buffer_)->k << std::endl;
    // std::cout << " -- The last element is: " << (((record_t*)data_manager.output_mapped_buffer_) + output_file_size / conf.record_size - 1)->k << std::endl;
}

void ExtSort::Sort(vector<string> &files)
{
#ifdef bandwidth
    timer.start("checkpoints");
#endif
    timer.start("RUN");
    vector<string> runs = RunGeneration(files);
    timer.end("RUN");

    timer.start("MERGE");
    MergeRuns(runs, files[0]);
    timer.end("MERGE");
#ifdef bandiwdth
    timer.stop("checkpoints");
#endif

    // check if all records are sorted
    // printf("====================================:\n");
    // vector<string> sorted_file;
    // sorted_file.emplace_back(folder_path_ + "/sorted");
    
    // DataManager data_manager(sorted_file, &timer);
    // int fd = open(sorted_file[0].c_str(), O_RDWR | O_DIRECT);
    // if (fd < 0)
    // {
    //     printf("Couldn't open input file %s: %s\n", sorted_file[0].c_str(), strerror(errno));
    //     exit(1);
    // }
    // struct stat st;
    // stat(sorted_file[0].c_str(), &st);
    // // mmap file
    // if (!data_manager.MMapFile(st.st_size, 0, fd, data_manager.output_mapped_buffer_))
    //     exit(1);
    // record_t* output = reinterpret_cast<record_t*>(data_manager.output_mapped_buffer_);
    // const bool sorted = std::is_sorted(output, output + st.st_size / conf.record_size,
    //                                 std::less<>{});
    // std::cout << "Elements are sorted: " << std::boolalpha << sorted << std::endl;
    // std::cout << " -- The first element is: " << output->k << std::endl;
    // std::cout << " -- The last element is: " << (output + st.st_size / conf.record_size - 1)->k << std::endl;


    printf("====================================:\n");
    // printf("\t RUN read: %f\n", timer.get_overall_time("RUN read"));
    // printf("\t RUN sort: %f\n", timer.get_overall_time("SORT"));
    // printf("\t RUN write: %f\n", timer.get_overall_time("RUN write"));
    printf("Total RUN: %f\n", timer.get_time("RUN"));
    // printf("\t MERGE read: %f\n", timer.get_overall_time("MERGE read"));
    // printf("\t MERGE write: %f\n", timer.get_overall_time("MERGE write"));
    // // printf("\t RECORD read: %f\n", timer.get_overall_time("RECORD read"));
    printf("Total MERGE: %f\n", timer.get_time("MERGE"));
    printf("Total: %f\n", timer.get_time("RUN") + timer.get_time("MERGE"));
    printf("\n");

}
