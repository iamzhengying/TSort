////////////////////////////////////////////
//
// Title: External Sort
// Version: 
// Details: a normal merge using heap
// Author: Zheng Ying
// Comment: 
//
////////////////////////////////////////////

#include "utils.h"

template<typename T>
bool comparison_fn(T x, T y) {
    return x.key < y.key;
}

void heapify(RecordIdx* heap, uint32_t start, uint32_t end);
void heapMerge(RecordPtr* input, Record* output, uint32_t dramSize, uint32_t runNum);

void external(uint32_t num, uint32_t batchSize, uint32_t threadNum) {
    uint32_t dramSize = num / 4000 * batchSize * 2;
    uint32_t relativeDRAM = num / dramSize;

    if (num % dramSize != 0) {
        printf("Error! The size of each run is different ...");
        return;
    }
    
    Record* records = mmap_pmem_file<Record>(FILENAME, num);

    printf("External Sort...\n");
    Tstart(startTimer);

    RecordPtr* recordPtrs = mmap_pmem_file<RecordPtr>(FILENAME_EXT_PTR, num);
    RecordPtr* dramAddr = new RecordPtr[dramSize];

    for (uint32_t i = 0; i < relativeDRAM; ++i) {
        uint32_t start = i * dramSize;
        for (uint32_t j = 0; j < dramSize; ++j) {
            dramAddr[j].key = records[start + j].key;
            dramAddr[j].ptr = records + start + j;
        }
        ips4o::parallel::sort(dramAddr, dramAddr + dramSize, comparison_fn<RecordPtr>);
#ifdef avx512
        __memmove_chk_avx512_no_vzeroupper(recordPtrs+start, dramAddr, dramSize * sizeof(RecordPtr));
#else
        memcpy(recordPtrs+start, dramAddr, dramSize * sizeof(RecordPtr));
#endif
    }
    Tend(endTimer);
    printf("Sorting ");
    singleLatency();

    // Read Sorted Data ...
    Tstart(startTimer);
    Record* output = mmap_pmem_file<Record>(FILENAME_EXT_PTR_OUT, num);
    heapMerge(recordPtrs, output, dramSize, relativeDRAM); 
    Tend(endTimer);
    printf("Reading ");
    singleLatency();

    // validate
    // const bool sorted = std::is_sorted(output, output + num, comparison_fn<Record>);
    // std::cout << "Elements are sorted: " << std::boolalpha << sorted << std::endl;
    // std::cout << " -- The first element is: " << output->key << std::endl;
    // std::cout << " -- The last element is: " << (output + num - 1)->key << std::endl;
    
    delete[] dramAddr;
    unmmap_pmem_file<Record>(output, num);
    unmmap_pmem_file<RecordPtr>(recordPtrs, num);
    unmmap_pmem_file<Record>(records, num);
}


void heapMerge(RecordPtr* input, Record* output, uint32_t dramSize, uint32_t runNum) { 
    uint32_t readBuffSize =  dramSize / 4 / runNum * runNum;
    uint32_t writeBuffSize = dramSize / 4 * 3 * sizeof(RecordPtr) / sizeof(Record);      // kp->kv
    RecordPtr* readBuffer = new RecordPtr[readBuffSize];
    Record* writeBuffer = new Record[writeBuffSize];
    // 1. load 'chunkEach' records of each sorted sub-sequence into DRAM  
    uint32_t chunkEach = readBuffSize / runNum - 1;
    if (readBuffSize <= runNum) {
        printf("Error! No enough space to run heap merge. \n");
        return;
    }
    RecordIdx* heap = new RecordIdx[runNum];
    uint32_t* pmemStart = new uint32_t[runNum];       // max: dramSize
    uint32_t* dramStart = new uint32_t[runNum];       // max: chunkEach
    // assume the initial run > chunkSize
    for (uint32_t i = 0; i < runNum; ++i) {
        memcpy(readBuffer + i * chunkEach, input + i * dramSize, sizeof(RecordPtr) * chunkEach);
        pmemStart[i] = chunkEach;
        dramStart[i] = 0;
    }
    // 2. select all the first records of sub-sequences in DRAM and build heap
    for (uint32_t i = 0; i < runNum; ++i) {
        heap[i].key = readBuffer[i * chunkEach].key;
        heap[i].idx = i;
    }
    for (uint32_t i = runNum / 2 - 1; i >= 0; --i) {
        heapify(heap, i, runNum-1);
        if (i == 0) break;
    }
    // 3. heap sort
    uint32_t count = 0;
    uint32_t countBuffer = 0;
    while (heap[0].key != UINT32_MAX) {
        //  3.1 read out the minimum
        uint32_t idx = heap[0].idx;
        RecordPtr* recordPtr = readBuffer + (idx * chunkEach + dramStart[idx]);
        memcpy(writeBuffer + countBuffer, recordPtr->ptr, sizeof(Record));
        ++countBuffer;
        if (countBuffer == writeBuffSize) {
            memcpy(output + count, writeBuffer, sizeof(Record) * countBuffer);
            count += countBuffer;
            countBuffer = 0;
        }

        //  3.2 add a new record from the corresponding chunk into heap
        ++dramStart[idx];
        //      if the corresponding chunk is empty, full it with the records from the corresponding pmem file
        if (dramStart[idx] >= chunkEach) {
            dramStart[idx] = 0;
            uint32_t add = min(dramSize - pmemStart[idx], chunkEach);
            if (add > 0) {
                memcpy(readBuffer + idx * chunkEach, input + idx * dramSize + pmemStart[idx], sizeof(RecordPtr) * add);
                pmemStart[idx] += add;
            }
            
            while (add < chunkEach) {
                readBuffer[idx * chunkEach + add].key = UINT32_MAX;
                readBuffer[idx * chunkEach + add].ptr = nullptr;
                ++add;
            }
        }
        heap[0].key = readBuffer[idx * chunkEach + dramStart[idx]].key;
        heapify(heap, 0, runNum-1);
    }
    if (countBuffer > 0) {
        memcpy(output + count, writeBuffer, sizeof(Record) * countBuffer);
    }
    
    delete[] readBuffer;
    delete[] writeBuffer;
    delete[] heap;
    delete[] dramStart;
    delete[] pmemStart;
}


void heapify(RecordIdx* heap, uint32_t start, uint32_t end) {
    // find the left and right sub-node
    uint32_t left = 2 * start + 1;
    uint32_t right = left + 1;

    // find the index of minimum among heap[start], heap[left], heap[right]
    uint32_t minIdx = start;
    if (left <= end && heap[left].key < heap[minIdx].key) {
        minIdx = left;
    }
    if (right <= end && heap[right].key < heap[minIdx].key) {
        minIdx = right;
    }

    // make the root-node minimum
    if (minIdx != start) {
        RecordIdx tmp;
        memcpy(&tmp, heap+minIdx, sizeof(RecordIdx));
        memcpy(heap+minIdx, heap+start, sizeof(RecordIdx));
        memcpy(heap+start, &tmp, sizeof(RecordIdx));
        heapify(heap, minIdx, end);
    }
}
