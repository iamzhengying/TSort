#pragma once

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>
#include <thread>
#include <time.h>
#include <math.h>
#include <queue>
#include <iostream>
#include <algorithm>
#include <mutex>

#include "Record.h"
#include "RTree.h"
#include "Buffer.h"
#include "utils_pm.h"
#include "thread_pool.h"
#include "ips4o.hpp"
// others
#include "TunnelList.h"
#include "WriteCount.h"
// gen
#include "Record_gensort.h"
#include "Tree_gensort.h"
#include "Buffer_gensort.h"

#ifdef __cplusplus
#include <stdint.h>
extern "C"
{
    void* __memmove_chk_avx512_no_vzeroupper(void *dest, void *src, uint32_t s);
    void* __memset_chk_avx512_no_vzeroupper(void *dest, int c, uint32_t s);
}
#endif

using namespace std;

extern uint32_t pmemCountArr[];

extern struct timespec startTimer, endTimer;
extern double total_latency;
extern uint32_t total_count;

#define Tstart(startTimer) (clock_gettime(CLOCK_REALTIME, &startTimer))
#define Tend(endTimer) (clock_gettime(CLOCK_REALTIME, &endTimer))
#define NS_RATIO (1000UL * 1000)

// #define RECORD_NUM_DEFAULT 1000000  //10000000
#define FILENAME "/dcpmm/zhengy/pmem_unsorted"
#define PARTITION_NAME "/dcpmm/zhengy/pmem_partition"
#define PARTITION_SORTED "/dcpmm/zhengy/pmem_partition_sorted"
#define FILENAME_BSTAR_PTR "/dcpmm/zhengy/pmem_bstarSort_ptr"
#define FILENAME_BSTAR_LIST "/dcpmm/zhengy/pmem_bstarSort_list"
#define FILENAME_BSTAR_SORTED "/dcpmm/zhengy/pmem_bstarSort_sorted"
#define FILENAME_EXT_PTR "/dcpmm/zhengy/pmem_external_ptr"
#define FILENAME_EXT_PTR_OUT "/dcpmm/zhengy/pmem_external_ptr_out"
#define FILENAME_INSERT_PTR "/dcpmm/zhengy/pmem_insertion_ptr"
#define FILENAME_INSERT_SORTED "/dcpmm/zhengy/pmem_insertion_sorted"
#define FILENAME_IPS4O_PTR "/dcpmm/zhengy/pmem_ips4o_ptr"
#define FILENAME_IPS4O_SORTED "/dcpmm/zhengy/pmem_ips4o_sorted"
#define FILENAME_QUICK_PTR "/dcpmm/zhengy/pmem_quick_ptr"
#define FILENAME_QUICK_SORTED "/dcpmm/zhengy/pmem_quick_sorted"
#define FILENAME_SELECT_PTR "/dcpmm/zhengy/pmem_selection_ptr"
#define FILENAME_SELECT_SORTED "/dcpmm/zhengy/pmem_selection_sorted"

#define BUCKET_MULTIPLE 10

#define DEBUG 0


// utils_timer
void singleLatency();
void addToTotalLatency(double* total_latency);
void printTotalLatency(double* total_latency);
double addToTotalLatency();
double printTotalLatency();
void resetTotalLatency();
double printAverageLatency();
double printThroughput();


// utils_generator
uint32_t generateData(uint32_t range, uint32_t num, uint32_t size, uint32_t skew);


// utils_tree
template<typename T1, typename T2>
void buildTree(T1* rootAddr, uint32_t idx, T2 key);

uint32_t findTree(uint32_t key, uint32_t* spliter, uint32_t start, uint32_t end);

template <typename T1, typename T2, typename T3>
void batchInsert(T1* node, T2* bufferStart, uint32_t start, uint32_t end, T1* pmemAddr, uint32_t idx);

void batchInsert(TreeNodeLock* node, RecordPtr* bufferStart, uint32_t start, uint32_t end, TreeNodeLock* pmemAddr, uint32_t idx);   // no batching

template <typename T1, typename T2, typename T3, typename T4>
void insert(T1* recordPtr, T2* buffer, uint32_t batchSize, T3* root, uint32_t treeIdx);     // single buffer

void insert(RecordPtr* recordPtr, Buffer* buffer, uint32_t batchSize, TreeNode* root, uint32_t treeIdx);    // double buffer

template<typename T1, typename T2>
void inOrderTraversal(T1* root, T2* pmem, uint32_t* idx);

template<typename T>
void inOrderTraversal(T* root, Record* pmemAddr_sorted, uint32_t* counter, Record* buffer, uint32_t* dramCounter, uint32_t thrd);   // global tree

template<typename T> 
uint32_t levelTraversal(T* root);

void validateFile(Record* sortedFile, uint32_t num);

void validateFile(Record_GenSort* sortedFile, uint32_t num);


// sort algorithm

void et_v1(uint32_t num, uint32_t treeNum, uint32_t batchSize, uint32_t threadNum1, uint32_t threadNum2, uint32_t threadNum3, uint32_t threadNum4, uint32_t threadNum5, uint32_t threadNum6, uint32_t threadNum7);
void et_v2(uint32_t num, uint32_t treeNum, uint32_t batchSize, uint32_t threadNum1, uint32_t threadNum2, uint32_t threadNum3, uint32_t threadNum4, uint32_t threadNum5, uint32_t threadNum6, uint32_t threadNum7);
void et_v3(uint32_t num, uint32_t treeNum, uint32_t batchSize, uint32_t threadNum1, uint32_t threadNum2, uint32_t threadNum3, uint32_t threadNum4, uint32_t threadNum5, uint32_t threadNum6, uint32_t threadNum7);
void et_v4(uint32_t num, uint32_t treeNum, uint32_t batchSize, uint32_t threadNum1, uint32_t threadNum2, uint32_t threadNum3, uint32_t threadNum4, uint32_t threadNum5, uint32_t threadNum6, uint32_t threadNum7);
void et_v5(uint32_t num, uint32_t treeNum, uint32_t batchSize, uint32_t threadNum1, uint32_t threadNum2, uint32_t threadNum3, uint32_t threadNum4, uint32_t threadNum5, uint32_t threadNum6, uint32_t threadNum7);
void et_v6(uint32_t num, uint32_t treeNum, uint32_t batchSize, uint32_t threadNum1, uint32_t threadNum2, uint32_t threadNum3, uint32_t threadNum4, uint32_t threadNum5, uint32_t threadNum6, uint32_t threadNum7);
void et_v7(uint32_t num, uint32_t treeNum, uint32_t batchSize, uint32_t threadNum1, uint32_t threadNum2, uint32_t threadNum3, uint32_t threadNum4, uint32_t threadNum5, uint32_t threadNum6, uint32_t threadNum7);
void et_v8(uint32_t num, uint32_t treeNum, uint32_t batchSize, uint32_t threadNum1, uint32_t threadNum2, uint32_t threadNum3, uint32_t threadNum4, uint32_t threadNum5, uint32_t threadNum6, uint32_t threadNum7);
void et_v9(uint32_t num, uint32_t treeNum, uint32_t batchSize, uint32_t threadNum1, uint32_t threadNum2, uint32_t threadNum3, uint32_t threadNum4, uint32_t threadNum5, uint32_t threadNum6, uint32_t threadNum7);
void et_v10(uint32_t num, uint32_t treeNum, uint32_t batchSize, uint32_t threadNum1, uint32_t threadNum2, uint32_t threadNum3, uint32_t threadNum4, uint32_t threadNum5, uint32_t threadNum6, uint32_t threadNum7);
void et_v11(uint32_t num, uint32_t treeNum, uint32_t batchSize, uint32_t threadNum1, uint32_t threadNum2, uint32_t threadNum3, uint32_t threadNum4, uint32_t threadNum5, uint32_t threadNum6, uint32_t threadNum7);

void bstar(uint32_t num, uint32_t bufferSize, uint32_t threadNum, uint32_t threshold);
void quick(uint32_t num, uint32_t batchSize, uint32_t threadNum1, uint32_t threadNum2);
void insertion(uint32_t num, uint32_t batchSize, uint32_t threadNum1, uint32_t threadNum2);
void ips4oKV(uint32_t num, uint32_t batchSize, uint32_t threadNum1, uint32_t threadNum2);
void external(uint32_t num, uint32_t batchSize, uint32_t threadNum);
void quick_write(uint32_t num, uint32_t batchSize, uint32_t threadNum1, uint32_t threadNum2);
void selection(uint32_t num, uint32_t batchSize, uint32_t threadNum1, uint32_t threadNum2);
void ips4o_onlyBAS(uint32_t num, uint32_t threadNum1);
void ips4o_inPlace(uint32_t num);
void introsort(uint32_t num, uint32_t batchSize, uint32_t threadNum1, uint32_t threadNum2);


// sort_gen

void et_v7_gensort(const char* inputFile, uint32_t num, uint32_t treeNum, uint32_t batchSize, uint32_t threadNum1, uint32_t threadNum2, uint32_t threadNum3, uint32_t threadNum4, uint32_t threadNum5, uint32_t threadNum6);
void ips4o_gensort(const char* inputFile, uint32_t num, uint32_t batchSize, uint32_t threadNum1, uint32_t threadNum2);
