////////////////////////////////////////////
//
// Title: TSort
// Version: v5
// Details: (DHMA) Batching (double) + Multi-threading + hashing + asyn
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

void et_v5(uint32_t num, uint32_t treeNum, uint32_t batchSize, uint32_t threadNum1, 
            uint32_t threadNum2, uint32_t threadNum3, uint32_t threadNum4, 
            uint32_t threadNum5, uint32_t threadNum6, uint32_t threadNum7) {
    
    printf("ETSort v5...\n");
    resetTotalLatency();
    Tstart(startTimer);

    ThreadPool pool(std::thread::hardware_concurrency());
    Record* records = mmap_pmem_file<Record>(FILENAME, num);
    uint32_t maxVal = UINT32_MAX;   
    uint32_t bucketNum = treeNum * BUCKET_MULTIPLE;
    uint32_t bucketInterval = (maxVal - 1) / bucketNum + 1; 
    uint32_t* counterThrd = new uint32_t[threadNum2 * bucketNum]();
    uint32_t numPerThrd = num / threadNum2;
    uint32_t numPerThrdOnemore = num % threadNum2;
    vector<std::future<void>> bucketWait;
    for (uint32_t thrd = 0; thrd < threadNum2; ++thrd) {
        bucketWait.emplace_back(
            pool.enqueue([=, &counterThrd, &records]() {
                Record* thrd_pos = thrd < numPerThrdOnemore ?
                                records + thrd * (numPerThrd + 1) :
                                records + numPerThrdOnemore + thrd * numPerThrd;
                uint32_t* counter_pos = counterThrd + thrd * bucketNum;
                uint32_t numPerThrdActual = thrd < numPerThrdOnemore ? 
                                numPerThrd + 1 : numPerThrd;
                for (uint32_t i = 0; i < numPerThrdActual; ++i) {
                    uint32_t idx = hash_i((thrd_pos + i)->key, bucketInterval);
                    ++counter_pos[idx];
                }
            })
        );
    }
    for (auto &&b: bucketWait)
        b.wait();

    uint32_t* counter = new uint32_t[bucketNum]();                  
    vector<std::future<void>> countWait;
    numPerThrd = bucketNum / threadNum2;
    numPerThrdOnemore = bucketNum % threadNum2;
    for (uint32_t thrd = 0; thrd < threadNum2; ++thrd) {
        countWait.emplace_back(
            pool.enqueue([=, &counterThrd, &counter]() {
                uint32_t thrd_pos_off = thrd < numPerThrdOnemore ?
                                thrd * (numPerThrd + 1) :
                                numPerThrdOnemore + thrd * numPerThrd;
                uint32_t numPerThrdActual = thrd < numPerThrdOnemore ? 
                                numPerThrd + 1 : numPerThrd;
                for (uint32_t i = 0; i < numPerThrdActual; ++i) {
                    for (uint32_t k = 0; k < threadNum2; ++k) {
                        counter[thrd_pos_off + i] += counterThrd[k * bucketNum + thrd_pos_off + i];
                    }
                }
            })
        );
    }
    for (auto &&b: countWait)
        b.wait();

    uint32_t* bucketTreeMap = new uint32_t[bucketNum]();
    vector<uint32_t> treeRoot;
    vector<uint32_t> treeSize;
    uint32_t nodeEachTree = (num - 1) / treeNum + 1;
    uint32_t sum = 0;
    uint32_t treeIdx = 0;
    uint32_t rootVal;

    for (uint32_t i = 0; i < bucketNum; ++i) {
        sum += counter[i];
        if (sum > nodeEachTree && i > 0) {
            sum -= counter[i];
            uint32_t sum_cpy = sum;
            uint32_t idx = i;
            while (sum_cpy > sum / 2) {
                --idx;
                sum_cpy -= counter[idx];                
            }
            rootVal = (sum / 2 - sum_cpy) * bucketInterval / counter[idx] + bucketInterval * idx;
            treeRoot.emplace_back(rootVal);
            treeSize.emplace_back(sum);
            ++treeIdx; 
            sum = counter[i];
        }
        bucketTreeMap[i] = treeIdx;
    }
    uint32_t sum_cpy = sum;
    uint32_t idx = bucketNum;
    while (sum_cpy > sum / 2) {
        --idx;
        sum_cpy -= counter[idx];                
    }
    rootVal = (sum / 2 - sum_cpy) / counter[idx] * bucketInterval + bucketInterval * idx;
    treeRoot.emplace_back(rootVal);
    treeSize.emplace_back(sum);

    uint32_t treeNumActual = treeRoot.size();
    TreeNode* pmemAddr = mmap_pmem_file<TreeNode>(PARTITION_NAME, num + treeNumActual);
    Record* pmemAddr_sorted = mmap_pmem_file<Record>(PARTITION_SORTED, num);
    uint32_t treeNumPerThrd = treeNumActual / threadNum3;
    uint32_t treeNumPerThrdOnemore = treeNumActual % threadNum3;
    vector<std::future<void>> rootWait;
    for (uint32_t thrd = 0; thrd < threadNum3; ++thrd) {
        rootWait.emplace_back(
            pool.enqueue([=, &pmemAddr]() {
                uint32_t treeStartIdx = thrd < treeNumPerThrdOnemore ?
                                    thrd * (treeNumPerThrd + 1):
                                    thrd * treeNumPerThrd + treeNumPerThrdOnemore;
                uint32_t treeNumPerThrdActual = thrd < treeNumPerThrdOnemore ?
                                    treeNumPerThrd + 1 : treeNumPerThrd;
                uint32_t curTreeIdx = treeStartIdx;
                uint32_t posOffset = accumulate(treeSize.begin(), treeSize.begin() + curTreeIdx, curTreeIdx);
                for (uint32_t i = 0; i < treeNumPerThrdActual; ++i) {
                    // tree initialization
                    TreeNode* rootAddr = pmemAddr + posOffset;
                    buildTree<TreeNode, uint32_t>(rootAddr, curTreeIdx, treeRoot[curTreeIdx]); 
                    posOffset += (treeSize[curTreeIdx] + 1);
                    curTreeIdx += 1;
                }
            })
        );
    }
    for (auto &&r: rootWait)
        r.wait();
    uint32_t maxTreeNodeNum = *max_element(treeSize.begin(), treeSize.end()) + 1;
    for (uint32_t i = 1; i < treeSize.size(); ++i) {
        treeSize[i] += treeSize[i-1];
    }
    Tend(endTimer);
    printf("Initialization ");
    singleLatency();

    Tstart(startTimer);
    uint32_t bufferSize = batchSize * treeNum / treeNumActual;
    Buffer* buffer = new Buffer[treeNumActual];
    for (uint32_t i = 0; i < treeNumActual; ++i) {
        buffer[i].insertBuffer = new RecordPtr[bufferSize];
        buffer[i].treeBuffer = new RecordPtr[bufferSize];
    }
    numPerThrd = num / threadNum4;     // runSize = bufferSize
    numPerThrdOnemore = num % threadNum4;
    vector<std::future<void>> insertWait;
    for (uint32_t thrd = 0; thrd < threadNum4; ++thrd) {
        insertWait.emplace_back(
            pool.enqueue([=, &pmemAddr, &buffer, &records, &treeSize]() {
                Record* startThrd = thrd < numPerThrdOnemore ?
                                    records + thrd * (numPerThrd + 1) :
                                    records + numPerThrdOnemore + thrd * numPerThrd;
                uint32_t numPerThrdActual = thrd < numPerThrdOnemore ?
                                    numPerThrd + 1 : numPerThrd;
                RecordPtr kp;
                for (uint32_t i = 0; i < numPerThrdActual; ++i) {
                    kp.key = (startThrd + i)->key;
                    kp.ptr = startThrd + i;
                    uint32_t treeIdx = bucketTreeMap[hash_i(kp.key, bucketInterval)];
                    Buffer* targetBuffer = buffer + treeIdx;
                    TreeNode* root = treeIdx == 0 ? pmemAddr : pmemAddr + treeSize[treeIdx-1] + treeIdx;
                    insert(&kp, targetBuffer, bufferSize, root, treeIdx);
                }
            })
        );
    }
    for (auto &&i: insertWait)
        i.wait();
    
    numPerThrd = treeNumActual / threadNum4;     // runSize = bufferSize
    numPerThrdOnemore = treeNumActual % threadNum4;
    vector<std::future<void>> clearWait; 
    for (uint32_t thrd = 0; thrd < threadNum4; ++thrd) {
        clearWait.emplace_back(
             pool.enqueue([=, &pmemAddr, &buffer, &treeSize]() {
                uint32_t startThrd = thrd < numPerThrdOnemore ?
                                    thrd * (numPerThrd + 1) :
                                    numPerThrdOnemore + thrd * numPerThrd;
                uint32_t numPerThrdActual = thrd < numPerThrdOnemore ?
                                    numPerThrd + 1 : numPerThrd;
                for (uint32_t i = 0; i < numPerThrdActual; ++i) {
                    uint32_t idx = startThrd + i;
                    if (buffer[idx].insertSize > 0) {
                        TreeNode* root = idx == 0 ? pmemAddr : pmemAddr + treeSize[idx-1] + idx;
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
    uint32_t runTreeNum = bufferSize / maxTreeNodeNum;
    uint32_t runNum = (treeNumActual - 1) / runTreeNum + 1;
    treeNumPerThrd = runTreeNum / threadNum6;
    treeNumPerThrdOnemore = runTreeNum % threadNum6;
    uint32_t treeNumPerThrd_w = runTreeNum / threadNum7;
    uint32_t treeNumPerThrdOnemore_w = runTreeNum % threadNum7;
    for (uint32_t runIdx = 0; runIdx < runNum; ++runIdx) {
        uint32_t runTreeStart = runTreeNum * runIdx;
        if (runTreeStart + runTreeNum > treeNumActual) {
            runTreeNum = treeNumActual - runTreeStart;
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
                    uint32_t posSTart = treeStartIdx == 0 ? 0 : treeSize[treeStartIdx-1];  
                    if (runTreeStart > 0) posSTart -= treeSize[runTreeStart-1];
                    Record* bufferAddr = readBuffer2 + posSTart;
                    TreeNode* rootAddr = treeStartIdx == 0 ? pmemAddr : pmemAddr + treeSize[treeStartIdx-1] + treeStartIdx;
                    // PM -> DRAM
                    for (uint32_t i = 0; i < treeNumPerThrdActual; ++i) {
                        uint32_t idx = 0;
                        inOrderTraversal<TreeNode, Record>(rootAddr, bufferAddr, &idx);
                        rootAddr += treeSize[treeStartIdx + i] + 1;
                        bufferAddr += treeSize[treeStartIdx + i];
                        if (treeStartIdx + i > 0) {
                            rootAddr -= treeSize[treeStartIdx + i - 1];
                            bufferAddr -= treeSize[treeStartIdx + i - 1];
                        }
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
                    uint32_t posDramSTart = treeStartIdx == 0 ? 0 : treeSize[treeStartIdx-1];
                    if (runTreeStart > 0) posDramSTart -= treeSize[runTreeStart-1];
                    uint32_t posPmemSTart = runTreeStart == 0 ? posDramSTart : treeSize[runTreeStart-1] + posDramSTart;
                    uint32_t treeSizeSum = treeStartIdx + treeNumPerThrdActual == 0 ? 0 : treeSize[treeStartIdx + treeNumPerThrdActual - 1];
                    if (treeStartIdx > 0) treeSizeSum -= treeSize[treeStartIdx - 1];
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
	for (uint32_t i = 0; i < treeNumActual; ++i) {
		uint32_t level = levelTraversal<TreeNode>(pmemAddr + posOffset);
		maxHeight = max(maxHeight, level);
		totalHeight += level;
        posOffset = treeSize[i] + i + 1;
	}
	printf("avg height: %u, max height: %d\n", totalHeight / treeNumActual, maxHeight);

    // validateFile(pmemAddr_sorted, num);

    delete[] counter;
    delete[] bucketTreeMap;
    for (uint32_t i = 0; i < treeNumActual; ++i) {
        delete[] buffer[i].insertBuffer;
        delete[] buffer[i].treeBuffer;
    }
    delete[] buffer;
    delete[] readBuffer2;
    delete[] counterThrd;
    unmmap_pmem_file<Record>(pmemAddr_sorted, num);
    unmmap_pmem_file<TreeNode>(pmemAddr, num + treeNumActual);
    unmmap_pmem_file<Record>(records, num);
}

