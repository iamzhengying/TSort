////////////////////////////////////////////
//
// Title: Quick Sort
// Version: write
// Details: count writes
// Author: Zheng Ying
// Comments:
//
////////////////////////////////////////////

#include "utils.h"

#define BLOCK_SIZE 1024 * 32  // 1024 * 1024 / RECORD_SIZE * 4

template<typename T>
bool comparison_fn(T x, T y) {
    return x.key < y.key;
}

uint32_t choosePivotMedianOfThree(RecordPtr* array, uint32_t l, uint32_t r);
int32_t partition_median(RecordPtr* array, int32_t start, int32_t end, WriteCount* writeCount);
void parallelQuickSort(RecordPtr* recordPtrs, int32_t start, int32_t end, WriteCount* writeCount);

void quick_write(uint32_t num, uint32_t batchSize, uint32_t threadNum1, uint32_t threadNum2) {
    
    printf("Quick Sort (count writes)...\n");
    Tstart(startTimer);

    Record* records = mmap_pmem_file<Record>(FILENAME, num);
    uint32_t dramNum = batchSize * (num / 4000) * 2 * sizeof(RecordPtr) / sizeof(Record);
    RecordPtr* recordPtrs = mmap_pmem_file<RecordPtr>(FILENAME_QUICK_PTR, num);
    Record* records_sorted = mmap_pmem_file<Record>(FILENAME_QUICK_SORTED, num);
    WriteCount writeCount;
    writeCount.count = num;
    #pragma omp parallel num_threads(threadNum1)
    {
        #pragma omp for
        for (uint32_t i = 0; i < num; ++i) {
            recordPtrs[i].key = records[i].key;
            recordPtrs[i].ptr = records + i;
        }

        #pragma omp single nowait
        parallelQuickSort(recordPtrs, 0, num-1, &writeCount);
    }
    Tend(endTimer);
    printf("Sorting ");
    singleLatency();

    Tstart(startTimer);
    // read
    ThreadPool pool(std::thread::hardware_concurrency());
    Record* readBuffer = new Record[dramNum];
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
                    RecordPtr* filePos = recordPtrs + startIdx;
                    Record* bufferPos = readBuffer + offset;
                    // PM -> DRAM
                    for (uint32_t i = 0; i < numPerThrdActual; ++i) {
#ifdef avx512
                    __memmove_chk_avx512_no_vzeroupper(bufferPos + i, filePos[i].ptr, sizeof(Record));
#else
                    memcpy(bufferPos + i, filePos[i].ptr, sizeof(Record));
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
                    Record* bufferPos = readBuffer + offset;
                    Record* outputPos = records_sorted + startIdx;
                    uint32_t cpySize;
                    while (numPerThrdActual) {
                        if (numPerThrdActual < BLOCK_SIZE) {
                            cpySize = numPerThrdActual;
                            numPerThrdActual = 0;
                        }
                        else {
                            cpySize = BLOCK_SIZE;
                            numPerThrdActual -= BLOCK_SIZE;
                        }
#ifdef avx512
                        __memmove_chk_avx512_no_vzeroupper(outputPos, bufferPos, cpySize * sizeof(Record));
#else
                        memcpy(outputPos, bufferPos, cpySize * sizeof(Record));
#endif
                        bufferPos += cpySize;
                        outputPos += cpySize;
                    }
                })
            );
        }
        for (auto &&t: writeWait)
            t.wait();
    }

    Tend(endTimer);
    printf("Reading ");
    singleLatency();

    writeCount.count += num;
    printf("Write counts: %u\n", writeCount.count);

    // // validation
    // const bool sorted = std::is_sorted(records_sorted, records_sorted + num, comparison_fn<Record>);
    // std::cout << "Elements are sorted: " << std::boolalpha << sorted << std::endl;
    // std::cout << " -- The first element is: " << records_sorted->key << std::endl;
    // std::cout << " -- The last element is: " << (records_sorted + num - 1)->key << std::endl;

    delete[] readBuffer;
    unmmap_pmem_file<RecordPtr>(recordPtrs, num);
    unmmap_pmem_file<Record>(records_sorted, num);
    unmmap_pmem_file<Record>(records, num);
}


void parallelQuickSort(RecordPtr* recordPtrs, int32_t start, int32_t end, WriteCount* writeCount) {
    if (start < end) {
        int32_t pivot = partition_median(recordPtrs, start, end, writeCount);
        #pragma omp task default(none) firstprivate(recordPtrs, start, pivot, writeCount)
        {
            parallelQuickSort(recordPtrs, start, pivot-1, writeCount);
        }
        #pragma omp task default(none) firstprivate(recordPtrs, pivot, end, writeCount)
        {
            parallelQuickSort(recordPtrs, pivot+1, end, writeCount);
        }
    }
}


int32_t partition_median(RecordPtr* array, int32_t start, int32_t end, WriteCount* writeCount) {
    int32_t l = start;
    int32_t r = end;
    int32_t pivotIdx = choosePivotMedianOfThree(array, start, end);
    RecordPtr pivot;
    memcpy(&pivot, array + pivotIdx, sizeof(RecordPtr));
    memcpy(array + pivotIdx, array + start, sizeof(RecordPtr));
    memcpy(array + start, &pivot, sizeof(RecordPtr));
    writeCount->mutex.lock();
    writeCount->count += 2;
    writeCount->mutex.unlock();
    while (l < r) {
        while (l < r && array[r].key >= pivot.key) {
            --r;
        }
        if (l < r) {
            memcpy(array + l, array + r, sizeof(RecordPtr));
            writeCount->mutex.lock();
            ++writeCount->count;
            writeCount->mutex.unlock();
            ++l;
        }

        while (l < r && array[l].key < pivot.key) {
            ++l;
        }
        if (l < r) {
            memcpy(array + r, array + l, sizeof(RecordPtr));
            writeCount->mutex.lock();
            ++writeCount->count;
            writeCount->mutex.unlock();
            --r;
        }
    }
    memcpy(array + l, &pivot, sizeof(RecordPtr));
    writeCount->mutex.lock();
    ++writeCount->count;
    writeCount->mutex.unlock();
    return l;
}
