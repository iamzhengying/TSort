////////////////////////////////////////////
//
// Title: ips4o
// Version: write
// Details: write counts
// Author: Zheng Ying
// Comment: 
//
////////////////////////////////////////////

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <thread>
#include <iostream>
#include <algorithm>

#include "utils_pm.h"
#include "Record.h"
#include "ips4o.hpp"

#define FILENAME "/dcpmm/zhengy/pmem_unsorted"
#define FILENAME_IPS4O_PTR "/dcpmm/zhengy/pmem_ips4o_ptr"
#define FILENAME_IPS4O_SORTED "/dcpmm/zhengy/pmem_ips4o_sorted"

#define BLOCK_SIZE 1024 * 32  // 1024 * 1024 / RECORD_SIZE * 4

template<typename T>
bool comparison_fn(T x, T y) {
    return x.key < y.key;
}

int main(int argc, char** argv) {

    printf("IPS4o write counts ... \n");

    uint32_t num = atol(argv[1]);

    Record* records = mmap_pmem_file<Record>(FILENAME, num);

    /*** Preparation ***/
    RecordPtr* recordPtrs = mmap_pmem_file<RecordPtr>(FILENAME_IPS4O_PTR, num);
    Record* records_sorted = mmap_pmem_file<Record>(FILENAME_IPS4O_SORTED, num);
    #pragma omp parallel for num_threads(32)
    for (uint32_t i = 0; i < num; ++i) {
        recordPtrs[i].key = records[i].key;
        recordPtrs[i].ptr = records + i;
    }

    ips4o::sort(recordPtrs, recordPtrs + num, comparison_fn<RecordPtr>);

    unmmap_pmem_file<RecordPtr>(recordPtrs, num);
    unmmap_pmem_file<Record>(records_sorted, num);
    unmmap_pmem_file<Record>(records, num);

    return 0;

}


