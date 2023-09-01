////////////////////////////////////////////
//
// Title: ips4o
// Version: kp_kv gensort
// Details: sort with key-ptr, and generate sorted key-val file
// Author: Zheng Ying
// Comment: 
//
////////////////////////////////////////////

#include <chrono>
#include <iostream>
#include <unistd.h>
#include <algorithm>
#include <atomic>

#include "utils.h"

template<typename T>
bool comparison_fn(T x, T y) {
    return x.key < y.key;
}

void ips4o_gensort(const char* inputFile, uint32_t num, uint32_t batchSize, uint32_t threadNum1, uint32_t threadNum2) {

    printf("IPS4o gensort ... \n");
    Tstart(startTimer);

    uint32_t dramNum = batchSize * (num / 4000) * 2 * sizeof(RecordPtr_GenSort) / sizeof(Record_GenSort);
    Record_GenSort* records = mmap_pmem_file<Record_GenSort>(inputFile, num);

    /*** Preparation ***/
    RecordPtr_GenSort* recordPtrs = mmap_pmem_file<RecordPtr_GenSort>(FILENAME_IPS4O_PTR, num);
    Record_GenSort* records_sorted = mmap_pmem_file<Record_GenSort>(FILENAME_IPS4O_SORTED, num);
    #pragma omp parallel for num_threads(threadNum1)
    for (uint32_t i = 0; i < num; ++i) {
        recordPtrs[i].key = records[i].key;
        recordPtrs[i].ptr = records + i;
    }

    /*** Sort ***/
    ips4o::parallel::sort(recordPtrs, recordPtrs + num, comparison_fn<RecordPtr_GenSort>);
    Tend(endTimer);
    printf("Sorting ");
    singleLatency();

    Tstart(startTimer);
    // read
    ThreadPool pool(std::thread::hardware_concurrency());
    Record_GenSort* readBuffer = new Record_GenSort[dramNum];
    uint32_t runNum = (num - 1) / dramNum + 1;
    uint32_t numPerThrd = dramNum / threadNum1;
    uint32_t numPerThrdOnemore = dramNum % threadNum1;
    uint32_t numPerThrd_w = dramNum / threadNum2;
    uint32_t numPerThrdOnemore_w = dramNum % threadNum2;
    for (uint32_t runIdx = 0; runIdx < runNum; ++runIdx) {
        uint32_t runStart = dramNum * runIdx;

        if (runStart + dramNum > num) {
            dramNum = num - runStart;
            numPerThrd = dramNum / threadNum1;
            numPerThrdOnemore = dramNum % threadNum1;
            numPerThrd_w = dramNum / threadNum2;
            numPerThrdOnemore_w = dramNum % threadNum2;
        }
        vector<std::future<void>> readWait;
        for (uint32_t thrd = 0; thrd < threadNum1; ++thrd) {
            readWait.emplace_back(
                pool.enqueue([=, &recordPtrs, &readBuffer]() {
                    uint32_t offset = thrd < numPerThrdOnemore ?
                                    thrd * (numPerThrd + 1) : numPerThrdOnemore + thrd * numPerThrd;
                    uint32_t startIdx = runStart + offset;
                    uint32_t numPerThrdActual = thrd < numPerThrdOnemore ?
                                    numPerThrd + 1 : numPerThrd;
                    RecordPtr_GenSort* filePos = recordPtrs + startIdx;
                    Record_GenSort* bufferPos = readBuffer + offset;
                    // PM -> DRAM
                    for (uint32_t i = 0; i < numPerThrdActual; ++i) {
#ifdef avx512
                    __memmove_chk_avx512_no_vzeroupper(bufferPos + i, filePos[i].ptr, sizeof(Record_GenSort));
#else
                    memcpy(bufferPos + i, filePos[i].ptr, sizeof(Record_GenSort));
#endif
                    }
                })
            );
        }
        for (auto &&t: readWait)
            t.wait();

        vector<std::future<void>> writeWait;
        for (uint32_t thrd = 0; thrd < threadNum2; ++thrd) {
            writeWait.emplace_back(
                pool.enqueue([=, &records_sorted, &readBuffer]() {
                    uint32_t offset = thrd < numPerThrdOnemore_w ?
                                    thrd * (numPerThrd_w + 1) : numPerThrdOnemore_w + thrd * numPerThrd_w;
                    uint32_t startIdx = runStart + offset;
                    uint32_t numPerThrdActual = thrd < numPerThrdOnemore_w ?
                                    numPerThrd_w + 1 : numPerThrd_w;
                    // DRAM -> PM
                    Record_GenSort* bufferPos = readBuffer + offset;
                    Record_GenSort* outputPos = records_sorted + startIdx;
#ifdef avx512
                    __memmove_chk_avx512_no_vzeroupper(outputPos, bufferPos, numPerThrdActual * sizeof(Record_GenSort));
#else
                    memcpy(outputPos, bufferPos, numPerThrdActual * sizeof(Record_GenSort));
#endif
                })
            );
        }
        for (auto &&t: writeWait)
            t.wait();
    }
    Tend(endTimer);
    printf("Reading ");
    singleLatency();

    delete[] readBuffer;
    unmmap_pmem_file<RecordPtr_GenSort>(recordPtrs, num);
    unmmap_pmem_file<Record_GenSort>(records_sorted, num);
    unmmap_pmem_file<Record_GenSort>(records, num);
}

