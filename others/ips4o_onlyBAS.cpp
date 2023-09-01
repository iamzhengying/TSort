////////////////////////////////////////////
//
// Title: ips4o
// Version: kp_kv
// Details: sort with key-ptr, and generate sorted key-val file (but use only pmem)
// Author: Zheng Ying
// Comment: 
//
////////////////////////////////////////////

#include "utils.h"

#define BLOCK_SIZE 1024 * 32  // 1024 * 1024 / RECORD_SIZE * 4

template<typename T>
bool comparison_fn(T x, T y) {
    return x.key < y.key;
}

void ips4o_onlyBAS(uint32_t num, uint32_t threadNum1) {

    printf("IPS4o only BAS ... \n");
    Tstart(startTimer);

    Record* records = mmap_pmem_file<Record>(FILENAME, num);
    /*** Preparation ***/
    RecordPtr* recordPtrs = mmap_pmem_file<RecordPtr>(FILENAME_IPS4O_PTR, num);
    Record* records_sorted = mmap_pmem_file<Record>(FILENAME_IPS4O_SORTED, num);
    #pragma omp parallel for num_threads(threadNum1)
    for (uint32_t i = 0; i < num; ++i) {
        recordPtrs[i].key = records[i].key;
        recordPtrs[i].ptr = records + i;
    }
    /*** Sort ***/
    ips4o::parallel::sort(recordPtrs, recordPtrs + num, comparison_fn<RecordPtr>);
    Tend(endTimer);
    printf("Sorting ");
    singleLatency();

    Tstart(startTimer);
    for (uint32_t i = 0; i < num; ++i) {
#ifdef avx512
        __memmove_chk_avx512_no_vzeroupper(records_sorted + i, recordPtrs[i].ptr, sizeof(Record));
#else
        memcpy(records_sorted + i, recordPtrs[i].ptr, sizeof(Record));
#endif        
    }
    Tend(endTimer);
    printf("Reading ");
    singleLatency();

    // // validation
    // const bool sorted = std::is_sorted(records_sorted, records_sorted + num, comparison_fn<Record>);
    // std::cout << "Elements are sorted: " << std::boolalpha << sorted << std::endl;
    // std::cout << " -- The first element is: " << records_sorted->key << std::endl;
    // std::cout << " -- The last element is: " << (records_sorted + num - 1)->key << std::endl;

    unmmap_pmem_file<RecordPtr>(recordPtrs, num);
    unmmap_pmem_file<Record>(records_sorted, num);
    unmmap_pmem_file<Record>(records, num);
}


