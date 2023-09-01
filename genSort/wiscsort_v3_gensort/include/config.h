#pragma once
#include <vector>
#include <string>
#define GIB(x) (x * 1024 * 1024 * 1024ull)
#define GB(x) (x * 1000 * 1000 * 1000ull)
#define MIB(x) (x * 1024 * 1024ull)
#define MB(x) (x * 1000 * 1000ull)

#define KEY_SIZE 10   // 250
#define VALUE_SIZE 90 // 502 // 6
#define INDEX_SIZE 8

#define BLOCK_SIZE_MERGEWRITE 4000

// To sort 159999840000 gb (160GB) or 199999800000 gb (200)
// #define BLOCK_SIZE 4096          // 4096
// #define READ_ARRAY_SIZE GIB(5)   // Should be a multiple of IDX_RECORD_SIZE.
// #define READ_BUFFER_SIZE GIB(5)  // RUNs are equal to READ buffer size
// #define WRITE_BUFFER_SIZE GIB(5) // NOTE: This has no use in WiscSort
// #define READ_SIZE 1              // in BLOCK_SIZE
// #define WRITE_SIZE 1
////////////////////////////////////////////////////////////////////

#define RECORD_SIZE sizeof(record_t)
// #define REC_PER_BLOCK (BLOCK_SIZE / RECORD_SIZE)
// #define READ_BUFFER_COUNT (READ_BUFFER_SIZE / BLOCK_SIZE)
// #define WRITE_BUFFER_COUNT (WRITE_BUFFER_SIZE / BLOCK_SIZE)
// #define REC_PER_WRITE_BUFFER ((WRITE_BUFFER_COUNT * BLOCK_SIZE) / RECORD_SIZE) // Not required.
// #define REC_PER_READ_BUFFER ((READ_BUFFER_COUNT * BLOCK_SIZE) / RECORD_SIZE)

// //// Index
#define IDX_RECORD_SIZE sizeof(in_record_t)
// #define IDX_REC_PER_BLK (WRITE_BLOCK_SIZE / IDX_RECORD_SIZE)

// //// Beta
// #define READ_ARRAY_COUNT (READ_ARRAY_SIZE / RECORD_SIZE)

struct Config
{
    std::vector<std::string> files;
    std::string output_dir;
    size_t read_buffer_size;
    size_t merge_read_buffer_size;
    size_t write_buffer_size; // Maybe needed for version 1?
    size_t block_size;
    size_t block_size_2;    // for merge write, the above block size cannot run since divisible limitation
    int mode; // Three concurrency modes
    int read_thrds;
    int sort_thrds;
    int write_thrds;

    // Not from cmdline
    int record_size;
    size_t recs_per_blk;
    size_t read_buff_blk_count;
    size_t write_buff_blk_count;
    size_t read_buff_rec_count;
    size_t write_buff_rec_count;

    int idx_record_size;
    size_t idx_recs_per_blk;
};

// Initialize a global struct, perhaps replace this with a singleton?
extern struct Config conf;