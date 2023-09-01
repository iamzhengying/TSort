////////////////////////////////////////////
//
// Title: ips4o
// Version: kv_kv
// Details: sort with key-val (in-place sorting), and generate sorted key-val file
// Author: Zheng Ying
// Comment: 
//
////////////////////////////////////////////

#include "utils.h"

template<typename T>
bool comparison_fn(T x, T y) {
    return x.key < y.key;
}

void ips4o_inPlace(uint32_t num) {

    printf("IPS4o in place ... \n");
    Tstart(startTimer);

    Record* records = mmap_pmem_file<Record>(FILENAME, num);
    ips4o::parallel::sort(records, records + num, comparison_fn<Record>);
    Tend(endTimer);
    printf("Sorting ");
    singleLatency();

    // // validation
    // const bool sorted = std::is_sorted(records, records + num, comparison_fn<Record>);
    // std::cout << "Elements are sorted: " << std::boolalpha << sorted << std::endl;
    // std::cout << " -- The first element is: " << records->key << std::endl;
    // std::cout << " -- The last element is: " << (records + num - 1)->key << std::endl;

    unmmap_pmem_file<Record>(records, num);
}


