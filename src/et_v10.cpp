////////////////////////////////////////////
//
// Title: TSort
// Version: v10
// Details: (BBMA) Batching (double) + Multi-threading + sampling + asyn
// Author: Zheng Ying
// Comment: 
//
////////////////////////////////////////////

#include <chrono>
#include <atomic>
#include <unistd.h>
#include <numeric>

#include "utils.h"

#define hash_i(i, bucketInterval) (i / bucketInterval)

#define KEY_SIZE 4
#define TREENODE_SIZE sizeof(TreeNode)
#define RECORDPTR_SIZE sizeof(RecordPtr)
#define RECORD_SIZE sizeof(Record)
#define BLOCK_SIZE 1024 * 32  // 1024 * 1024 / RECORD_SIZE * 4

void et_v10(uint32_t num, uint32_t treeNum, uint32_t batchSize, uint32_t threadNum1, 
            uint32_t threadNum2, uint32_t threadNum3, uint32_t threadNum4, 
            uint32_t threadNum5, uint32_t threadNum6, uint32_t threadNum7) {
    
    printf("ETSort v10...\n");
    resetTotalLatency();
    Tstart(startTimer);

    ThreadPool pool(std::thread::hardware_concurrency());
    Record* records = mmap_pmem_file<Record>(FILENAME, num);   
    uint32_t sampleRate = 100;     // = stepLength
    uint32_t sampleNum = num / sampleRate;
    uint32_t numPerThrd = sampleNum / threadNum1;
    uint32_t numPerThrdOnemore = sampleNum % threadNum1;
    uint32_t* sampleArray = new uint32_t[sampleNum];
    vector<std::future<void>> sampleWait;
    srand(104);
    for (uint32_t thrd = 0; thrd < threadNum1; ++thrd) {
        sampleWait.emplace_back(
            pool.enqueue([=, &sampleArray]() {
                uint32_t thrd_off = thrd < numPerThrdOnemore ?
                                thrd * (numPerThrd + 1) : numPerThrdOnemore + thrd * numPerThrd;
                uint32_t numPerThrdActual = thrd < numPerThrdOnemore ? 
                                numPerThrd + 1 : numPerThrd;
                uint32_t* samplePos = sampleArray + thrd_off;
                Record* recordPos = records + thrd_off * sampleRate;
                for (uint32_t i = 0; i < numPerThrdActual; ++i) {
                    samplePos[i] = (recordPos + rand() % sampleRate)->key;
                    recordPos += sampleRate;
                }
            })
        ); 
    }
    for (auto &&s: sampleWait)
        s.wait();
    
    ips4o::parallel::sort(sampleArray, sampleArray + sampleNum, std::less<>{});
    uint32_t sampleStep = sampleNum / treeNum;
    uint32_t* treeUpp = new uint32_t[treeNum];
    uint32_t maxTreeSize = num / treeNum * 2;      // 2x
    TreeNode* pmemAddr = mmap_pmem_file<TreeNode>(PARTITION_NAME, treeNum * maxTreeSize);
    Record* pmemAddr_sorted = mmap_pmem_file<Record>(PARTITION_SORTED, num);
    numPerThrd = treeNum / threadNum3;
    numPerThrdOnemore = treeNum % threadNum3;
    vector<std::future<void>> initialWait;
    for (uint32_t thrd = 0; thrd < threadNum3; ++thrd) {
        initialWait.emplace_back(
            pool.enqueue([=, &sampleArray, &pmemAddr, &treeUpp]() {
                uint32_t thrd_off = thrd < numPerThrdOnemore ?
                                thrd * (numPerThrd + 1) : numPerThrdOnemore + thrd * numPerThrd;
                uint32_t numPerThrdActual = thrd < numPerThrdOnemore ? 
                                numPerThrd + 1 : numPerThrd;
                uint32_t* samplePos = sampleArray + thrd_off * sampleStep;
                uint32_t* uppPos = treeUpp + thrd_off;
                TreeNode* pmemPos = pmemAddr + thrd_off * maxTreeSize;
                for (uint32_t i = 0; i < numPerThrdActual; ++i) {
                    uint32_t root = sampleStep % 2 ? samplePos[sampleStep / 2 + 1] : (samplePos[sampleStep / 2] + samplePos[sampleStep / 2 - 1]) / 2;
                    buildTree<TreeNode, uint32_t>(pmemPos, thrd_off + i, root);
                    uint32_t upp = (i + 1) * sampleStep + thrd_off < sampleNum ? samplePos[sampleStep] : UINT32_MAX;
                    uppPos[i] = upp;
                    samplePos += sampleStep;
                    pmemPos += maxTreeSize;
                }
            })
        );
    }
    for (auto &&i: initialWait)
        i.wait();
    Tend(endTimer);
    printf("Initialization ");
    singleLatency();

    Tstart(startTimer);
    uint32_t bufferSize = batchSize * treeNum / treeNum;
    SingleBuffer* buffer = new SingleBuffer[treeNum];
    for (uint32_t i = 0; i < treeNum; ++i) {
        buffer[i].insertBuffer = new RecordPtr[bufferSize];
    }
    numPerThrd = num / threadNum4;     // runSize = bufferSize
    numPerThrdOnemore = num % threadNum4;
    vector<std::future<void>> insertWait;
    for (uint32_t thrd = 0; thrd < threadNum4; ++thrd) {
        insertWait.emplace_back(
            pool.enqueue([=, &pmemAddr, &buffer, &records]() {
                Record* startThrd = thrd < numPerThrdOnemore ?
                                    records + thrd * (numPerThrd + 1) :
                                    records + numPerThrdOnemore + thrd * numPerThrd;
                uint32_t numPerThrdActual = thrd < numPerThrdOnemore ?
                                    numPerThrd + 1 : numPerThrd;
                RecordPtr kp;
                for (uint32_t i = 0; i < numPerThrdActual; ++i) {
                    kp.key = (startThrd + i)->key;
                    kp.ptr = startThrd + i;
                    uint32_t treeIdx = findTree(kp.key, treeUpp, 0, treeNum-1);
                    SingleBuffer* targetBuffer = buffer + treeIdx;
                    TreeNode* root = pmemAddr + treeIdx * maxTreeSize;
                    insert<RecordPtr, SingleBuffer, TreeNode, uint32_t>(&kp, targetBuffer, bufferSize, root, treeIdx);
                }
            })
        );
    }
    for (auto &&i: insertWait)
        i.wait();
    
    numPerThrd = treeNum / threadNum4;     // runSize = bufferSize
    numPerThrdOnemore = treeNum % threadNum4;
    vector<std::future<void>> clearWait;  
    for (uint32_t thrd = 0; thrd < threadNum4; ++thrd) {
        clearWait.emplace_back(
             pool.enqueue([=, &pmemAddr, &buffer]() {
                uint32_t startThrd = thrd < numPerThrdOnemore ?
                                    thrd * (numPerThrd + 1) :
                                    numPerThrdOnemore + thrd * numPerThrd;
                uint32_t numPerThrdActual = thrd < numPerThrdOnemore ?
                                    numPerThrd + 1 : numPerThrd;
                for (uint32_t i = 0; i < numPerThrdActual; ++i) {
                    uint32_t idx = startThrd + i;
                    if (buffer[idx].insertSize > 0) {
                        TreeNode* root = pmemAddr + idx * maxTreeSize;
                        ips4o::parallel::sort(buffer[idx].insertBuffer, buffer[idx].insertBuffer + buffer[idx].insertSize, std::less<>{});
                        batchInsert<TreeNode, RecordPtr, uint32_t>(root, buffer[idx].insertBuffer, 0, buffer[idx].insertSize-1, root, idx);
                        buffer[idx].insertSize = 0;
                    }
                }
            })
        );
    }
    for (auto &&c: clearWait)
        c.wait();
    Tend(endTimer);
    printf("Construction ");
    singleLatency();

    Tstart(startTimer);
    bufferSize = batchSize * 2 * treeNum * sizeof(RecordPtr) / sizeof(Record);   // reset bufferSize
    // delete[] bucketTreeMap;
    // delete[] readBuffer;
    // vector<uint32_t>().swap(treeRoot);
    Record* readBuffer2 = new Record[bufferSize];
    uint32_t maxTreeNodeNum = *max_element(pmemCountArr, pmemCountArr + treeNum);
    uint32_t runTreeNum = bufferSize / maxTreeNodeNum;
    uint32_t runNum = (treeNum - 1) / runTreeNum + 1;
    uint32_t treeNumPerThrd = runTreeNum / threadNum6;
    uint32_t treeNumPerThrdOnemore = runTreeNum % threadNum6;
    uint32_t treeNumPerThrd_w = runTreeNum / threadNum7;
    uint32_t treeNumPerThrdOnemore_w = runTreeNum % threadNum7;
    for (uint32_t runIdx = 0; runIdx < runNum; ++runIdx) {
        uint32_t runTreeStart = runTreeNum * runIdx;
        if (runTreeStart + runTreeNum > treeNum) {
            runTreeNum = treeNum - runTreeStart;
            treeNumPerThrd = runTreeNum / threadNum6;
            treeNumPerThrdOnemore = runTreeNum % threadNum6;
            treeNumPerThrd_w = runTreeNum / threadNum7;
            treeNumPerThrdOnemore_w = runTreeNum % threadNum7;
        }
        vector<std::future<void>> traversalReadWait;
        for (uint32_t thrd = 0; thrd < threadNum6; ++thrd) {
            traversalReadWait.emplace_back(
                pool.enqueue([=, &pmemAddr, &readBuffer2]() {
                    uint32_t treeOffset = thrd < treeNumPerThrdOnemore ?
                                    thrd * (treeNumPerThrd + 1) :
                                    treeNumPerThrdOnemore + thrd * treeNumPerThrd;
                    uint32_t treeStartIdx = runTreeStart + treeOffset;
                    uint32_t treeNumPerThrdActual = thrd < treeNumPerThrdOnemore ?
                                    treeNumPerThrd + 1 : treeNumPerThrd;
                    uint32_t posSTart = accumulate(pmemCountArr + runTreeStart, pmemCountArr + treeStartIdx, 0) - (treeStartIdx - runTreeStart);   
                    Record* bufferAddr = readBuffer2 + posSTart;
                    TreeNode* rootAddr = pmemAddr + treeStartIdx * maxTreeSize;
                    // PM -> DRAM
                    for (uint32_t i = 0; i < treeNumPerThrdActual; ++i) {
                        uint32_t idx = 0;
                        inOrderTraversal<TreeNode, Record>(rootAddr, bufferAddr, &idx);
                        rootAddr += maxTreeSize;
                        bufferAddr += pmemCountArr[treeStartIdx + i] - 1;
                    }
                })
            );
        }
        for (auto &&t: traversalReadWait)
            t.wait();

        vector<std::future<void>> traversalWriteWait;
        for (uint32_t thrd = 0; thrd < threadNum7; ++thrd) {
            traversalWriteWait.emplace_back(
                pool.enqueue([=, &pmemAddr_sorted, &readBuffer2]() {
                    uint32_t treeOffset = thrd < treeNumPerThrdOnemore_w ?
                                    thrd * (treeNumPerThrd_w + 1) :
                                    treeNumPerThrdOnemore_w + thrd * treeNumPerThrd_w;
                    uint32_t treeStartIdx = runTreeStart + treeOffset;
                    uint32_t treeNumPerThrdActual = thrd < treeNumPerThrdOnemore_w ?
                                    treeNumPerThrd_w + 1 : treeNumPerThrd_w;
                    uint32_t posDramSTart = accumulate(pmemCountArr + runTreeStart, pmemCountArr + treeStartIdx, 0) - (treeStartIdx - runTreeStart);
                    uint32_t posPmemSTart = accumulate(pmemCountArr, pmemCountArr + runTreeStart, posDramSTart) - runTreeStart;
                    uint32_t treeSizeSum = accumulate(pmemCountArr + treeStartIdx, pmemCountArr + treeStartIdx + treeNumPerThrdActual, 0) - treeNumPerThrdActual;
                    // DRAM -> PM
                    Record* outputDramAddr = readBuffer2 + posDramSTart;
                    Record* outputPmemAddr = pmemAddr_sorted + posPmemSTart;
                    uint32_t cpySize;
                    while (treeSizeSum > 0) {
                        if (treeSizeSum < BLOCK_SIZE) {
                            cpySize = treeSizeSum;
                            treeSizeSum = 0;
                        }
                        else {
                            cpySize = BLOCK_SIZE;
                            treeSizeSum -= BLOCK_SIZE;
                        }
#ifdef avx512
                        __memmove_chk_avx512_no_vzeroupper(outputPmemAddr, outputDramAddr, cpySize * sizeof(Record));
#else
                        memcpy(outputPmemAddr, outputDramAddr, cpySize * sizeof(Record));
#endif
                        outputDramAddr += cpySize;
                        outputPmemAddr += cpySize;
                    }
                })
            );
        }
        for (auto &&t: traversalWriteWait)
            t.wait();
    }

    Tend(endTimer);
    printf("Traversal ");
    singleLatency();
    
    // height info
    uint32_t totalHeight = 0;
	uint32_t maxHeight = 0;
    uint32_t posOffset = 0;
	for (uint32_t i = 0; i < treeNum; ++i) {
		uint32_t level = levelTraversal<TreeNode>(pmemAddr + posOffset);
		maxHeight = max(maxHeight, level);
		totalHeight += level;
        posOffset += maxTreeSize;
	}
	printf("avg height: %u, max height: %d\n", totalHeight / treeNum, maxHeight);

    // validateFile(pmemAddr_sorted, num);

    delete[] sampleArray;
    delete[] treeUpp;
    for (uint32_t i = 0; i < treeNum; ++i) {
        delete[] buffer[i].insertBuffer;
    }
    delete[] buffer;
    delete[] readBuffer2;
    unmmap_pmem_file<Record>(pmemAddr_sorted, num);
    unmmap_pmem_file<TreeNode>(pmemAddr, num + treeNum);
    unmmap_pmem_file<Record>(records, num);
}

