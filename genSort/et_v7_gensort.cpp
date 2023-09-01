////////////////////////////////////////////
//
// Title: TSort
// Version: v7_gensort
// Details: this version is derived from the normal v7
// Author: Zheng Ying
// Comment:
//
////////////////////////////////////////////

#include <chrono>
#include <atomic>
#include <unistd.h>
#include <numeric>
#include <math.h>

#include "utils.h"

#define BLOCK_SIZE 1024 * 32  // 1024 * 1024 / RECORD_SIZE * 4

uint32_t findBucketIdx(Key key, uint32_t digitNum, uint32_t rest);
Key bucketIdx2Key(uint32_t idx, uint32_t digitNum, uint32_t rest, uint32_t ratioUp, uint32_t ratioDown);

void et_v7_gensort(const char* inputFile, uint32_t num, uint32_t treeNum, uint32_t batchSize, 
            uint32_t threadNum1, uint32_t threadNum2, uint32_t threadNum3, 
            uint32_t threadNum4, uint32_t threadNum5, uint32_t threadNum6) {
    
    printf("ETSort v7_gensort...\n");
    Tstart(startTimer);
    ThreadPool pool(std::thread::hardware_concurrency());
    Record_GenSort* records = mmap_pmem_file<Record_GenSort>(inputFile, num);

    uint32_t bucketNum = treeNum * BUCKET_MULTIPLE;
    uint32_t digitNum = 0;
    uint32_t rest = bucketNum;
    while (rest >= 95) {
        digitNum += 1;
        rest /= 95;
    }
    rest = bucketNum / (uint32_t)pow(95, digitNum);
    bucketNum = (uint32_t)pow(95, digitNum) * rest; 
    uint32_t* counterThrd = new uint32_t[threadNum1 * bucketNum]();
    uint32_t numPerThrd = num / threadNum1;
    uint32_t numPerThrdOnemore = num % threadNum1;
    vector<std::future<void>> bucketWait;
    for (uint32_t thrd = 0; thrd < threadNum1; ++thrd) {
        bucketWait.emplace_back(
            pool.enqueue([=, &counterThrd, &records]() {
                Record_GenSort* thrd_pos = thrd < numPerThrdOnemore ?
                                records + thrd * (numPerThrd + 1) :
                                records + numPerThrdOnemore + thrd * numPerThrd;
                uint32_t* counter_pos = counterThrd + thrd * bucketNum;
                uint32_t numPerThrdActual = thrd < numPerThrdOnemore ? 
                                numPerThrd + 1 : numPerThrd;
                for (uint32_t i = 0; i < numPerThrdActual; ++i) {
                    uint32_t idx = findBucketIdx((thrd_pos + i)->key, digitNum, rest);
                    ++counter_pos[idx];
                }
            })
        );
    }
    for (auto &&b: bucketWait)
        b.wait();

    uint32_t* counter = new uint32_t[bucketNum]();
    vector<std::future<void>> countWait;
    numPerThrd = bucketNum / threadNum1;
    numPerThrdOnemore = bucketNum % threadNum1;
    for (uint32_t thrd = 0; thrd < threadNum1; ++thrd) {
        countWait.emplace_back(
            pool.enqueue([=, &counterThrd, &counter]() {
                uint32_t thrd_pos_off = thrd < numPerThrdOnemore ?
                                thrd * (numPerThrd + 1) :
                                numPerThrdOnemore + thrd * numPerThrd;
                uint32_t numPerThrdActual = thrd < numPerThrdOnemore ? 
                                numPerThrd + 1 : numPerThrd;
                for (uint32_t i = 0; i < numPerThrdActual; ++i) {
                    for (uint32_t k = 0; k < threadNum1; ++k) {
                        counter[thrd_pos_off + i] += counterThrd[k * bucketNum + thrd_pos_off + i];
                    }
                }
            })
        );
    }
    for (auto &&b: countWait)
        b.wait();
    uint32_t* bucketTreeMap = new uint32_t[bucketNum]();
    vector<Key> treeRoot;
    vector<uint32_t> treeSize;
    uint32_t nodeEachTree = (num - 1) / treeNum + 1;
    uint32_t sum = 0;
    uint32_t treeIdx = 0;
    Key rootVal;
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
            rootVal = bucketIdx2Key(idx, digitNum, rest, sum / 2 - sum_cpy, counter[idx]);
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
    rootVal = bucketIdx2Key(idx, digitNum, rest, sum / 2 - sum_cpy, counter[idx]);
    treeRoot.emplace_back(rootVal);
    treeSize.emplace_back(sum);

    uint32_t treeNumActual = treeRoot.size();
    TreeNode_GenSort* pmemAddr = mmap_pmem_file<TreeNode_GenSort>(PARTITION_NAME, num + treeNumActual);
    Record_GenSort* pmemAddr_sorted = mmap_pmem_file<Record_GenSort>(PARTITION_SORTED, num);
    uint32_t treeNumPerThrd = treeNumActual / threadNum2;
    uint32_t treeNumPerThrdOnemore = treeNumActual % threadNum2;
    vector<std::future<void>> rootWait;
    for (uint32_t thrd = 0; thrd < threadNum2; ++thrd) {
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
                    TreeNode_GenSort* rootAddr = pmemAddr + posOffset;
                    buildTree<TreeNode_GenSort, Key>(rootAddr, curTreeIdx, treeRoot[curTreeIdx]); 
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
    uint32_t bufferSize = (batchSize * 2 * treeNum - (bucketNum * 4 + treeNumActual * (10 + 4)) / 24) / treeNumActual;  
    SingleBuffer_GenSort* buffer = new SingleBuffer_GenSort[treeNumActual];
    for (uint32_t i = 0; i < treeNumActual; ++i) {
        buffer[i].insertBuffer = new RecordPtr_GenSort[bufferSize];
    }
    numPerThrd = num / threadNum3;     // runSize = bufferSize
    numPerThrdOnemore = num % threadNum3;
    vector<std::future<void>> insertWait;

    for (uint32_t thrd = 0; thrd < threadNum3; ++thrd) {
        insertWait.emplace_back(
            pool.enqueue([=, &pmemAddr, &buffer, &records, &treeSize]() {
                Record_GenSort* startThrd = thrd < numPerThrdOnemore ?
                                    records + thrd * (numPerThrd + 1) :
                                    records + numPerThrdOnemore + thrd * numPerThrd;
                uint32_t numPerThrdActual = thrd < numPerThrdOnemore ?
                                    numPerThrd + 1 : numPerThrd;
                RecordPtr_GenSort kp;
                for (uint32_t i = 0; i < numPerThrdActual; ++i) {
                    kp.key = (startThrd + i)->key;
                    kp.ptr = startThrd + i;
                    uint32_t treeIdx = bucketTreeMap[findBucketIdx(kp.key, digitNum, rest)];
                    SingleBuffer_GenSort* targetBuffer = buffer + treeIdx;
                    TreeNode_GenSort* root = treeIdx == 0 ? pmemAddr : pmemAddr + treeSize[treeIdx-1] + treeIdx;
                    insert<RecordPtr_GenSort, SingleBuffer_GenSort, TreeNode_GenSort, Key>(&kp, targetBuffer, bufferSize, root, treeIdx);
                }
            })
        );
    }
    for (auto &&i: insertWait)
        i.wait();

    numPerThrd = treeNumActual / threadNum3;     // runSize = bufferSize
    numPerThrdOnemore = treeNumActual % threadNum3;
    vector<std::future<void>> clearWait; 
    for (uint32_t thrd = 0; thrd < threadNum3; ++thrd) {
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
                        TreeNode_GenSort* root = idx == 0 ? pmemAddr : pmemAddr + treeSize[idx-1] + idx;
                        ips4o::parallel::sort(buffer[idx].insertBuffer, buffer[idx].insertBuffer + buffer[idx].insertSize, std::less<>{});
                        batchInsert<TreeNode_GenSort, RecordPtr_GenSort, Key>(root, buffer[idx].insertBuffer, 0, buffer[idx].insertSize-1, root, idx);
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
    bufferSize = (batchSize * 2 * treeNum - treeNumActual / 6) * sizeof(RecordPtr_GenSort) / sizeof(Record_GenSort);   // 减去treeSize的大小
    // delete[] bucketTreeMap;
    // delete[] readBuffer;
    // vector<uint32_t>().swap(treeRoot);
    Record_GenSort* readBuffer2 = new Record_GenSort[bufferSize];
    uint32_t runTreeNum = bufferSize / maxTreeNodeNum;
    if (runTreeNum == 0) printf("Buffer is not large enough to hold all nodes of the maximum tree during traveresal. ");
    uint32_t runNum = (treeNumActual - 1) / runTreeNum + 1;
    treeNumPerThrd = runTreeNum / threadNum5;
    treeNumPerThrdOnemore = runTreeNum % threadNum5;
    uint32_t treeNumPerThrd_w = runTreeNum / threadNum6;
    uint32_t treeNumPerThrdOnemore_w = runTreeNum % threadNum6;
    for (uint32_t runIdx = 0; runIdx < runNum; ++runIdx) {
        uint32_t runTreeStart = runTreeNum * runIdx;
        if (runTreeStart + runTreeNum > treeNumActual) {
            runTreeNum = treeNumActual - runTreeStart;
            treeNumPerThrd = runTreeNum / threadNum5;
            treeNumPerThrdOnemore = runTreeNum % threadNum5;
            treeNumPerThrd_w = runTreeNum / threadNum6;
            treeNumPerThrdOnemore_w = runTreeNum % threadNum6;
        }
        vector<std::future<void>> traversalReadWait;
        for (uint32_t thrd = 0; thrd < threadNum5; ++thrd) {
            traversalReadWait.emplace_back(
                pool.enqueue([=, &pmemAddr, &readBuffer2]() {
                    uint32_t treeOffset = thrd < treeNumPerThrdOnemore ?
                                    thrd * (treeNumPerThrd + 1) :
                                    treeNumPerThrdOnemore + thrd * treeNumPerThrd;
                    uint32_t treeStartIdx = runTreeStart + treeOffset;
                    uint32_t treeNumPerThrdActual = thrd < treeNumPerThrdOnemore ?
                                    treeNumPerThrd + 1 : treeNumPerThrd;
                    uint32_t posSTart = treeStartIdx == 0 ? 0 : treeSize[treeStartIdx-1];    // exclude treeStartIdx 
                    if (runTreeStart > 0) posSTart -= treeSize[runTreeStart-1];
                    Record_GenSort* bufferAddr = readBuffer2 + posSTart;
                    TreeNode_GenSort* rootAddr = treeStartIdx == 0 ? pmemAddr : pmemAddr + treeSize[treeStartIdx-1] + treeStartIdx;
                    // PM -> DRAM
                    for (uint32_t i = 0; i < treeNumPerThrdActual; ++i) {
                        uint32_t idx = 0;
                        inOrderTraversal<TreeNode_GenSort, Record_GenSort>(rootAddr, bufferAddr, &idx);
                        rootAddr += (treeSize[treeStartIdx + i] + 1);
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
        for (uint32_t thrd = 0; thrd < threadNum6; ++thrd) {
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
                    Record_GenSort* outputDramAddr = readBuffer2 + posDramSTart;
                    Record_GenSort* outputPmemAddr = pmemAddr_sorted + posPmemSTart;
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
                        memcpy(outputPmemAddr, outputDramAddr, cpySize * sizeof(Record_GenSort));
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
    
    // // height info
    // uint32_t totalHeight = 0;
	// uint32_t maxHeight = 0;
    // uint32_t posOffset = 0;
	// for (uint32_t i = 0; i < treeNumActual; ++i) {
	// 	uint32_t level = levelTraversal(pmemAddr + posOffset);
	// 	maxHeight = max(maxHeight, level);
	// 	totalHeight += level;
    //     posOffset += (treeSize[i] + 1);
	// }
	// printf("avg height: %u, max height: %d\n", totalHeight / treeNumActual, maxHeight);

    // validateFile(pmemAddr_sorted, num);

    delete[] counter;
    delete[] bucketTreeMap;
    for (uint32_t i = 0; i < treeNumActual; ++i) {
        delete[] buffer[i].insertBuffer;
    }
    delete[] buffer;
    delete[] readBuffer2;
    delete[] counterThrd;
    unmmap_pmem_file<Record_GenSort>(pmemAddr_sorted, num);
    unmmap_pmem_file<TreeNode_GenSort>(pmemAddr, num + treeNumActual);
    unmmap_pmem_file<Record_GenSort>(records, num);
}


/////////// utils ///////////

// return bucket idx (start from 0)
uint32_t findBucketIdx(Key key, uint32_t digitNum, uint32_t rest) {
    uint32_t idx = 0; 
    // handle the first 'digitNum' char
    for (uint32_t i = 0; i < digitNum; ++i) {
        idx += ((uint32_t)pow(95, digitNum - i - 1) * rest) * ((uint32_t)(key.key[i]) - 32);
    }
    // handle the 'digitNum + 1' char
    uint32_t charPerBucket = 95 / rest;
    uint32_t charPerBucketOnemore = 95 % rest;
    uint32_t c = (uint32_t)(key.key[digitNum]) - 32;
    // divide into 2 parts: "onemore" is the firstPart，the rest is the secondPart
    uint32_t firstPart = charPerBucketOnemore * (charPerBucket + 1);
    if (c < firstPart) {
        idx += c / (charPerBucket + 1);
    }
    else {
        idx += (c - firstPart) / charPerBucket + charPerBucketOnemore;
    }
    return idx;
}

Key bucketIdx2Key(uint32_t idx, uint32_t digitNum, uint32_t rest, uint32_t ratioUp, uint32_t ratioDown) {
    Key root;
    for (uint32_t i = 0; i < digitNum; ++i) {
        uint32_t c = idx / ((uint32_t)pow(95, digitNum - i - 1) * rest);
        root.key[i] = c+32;
        idx -= (uint32_t)pow(95, digitNum - i - 1) * rest * c;
    }
    uint32_t charPerBucket = 95 / rest;
    uint32_t charPerBucketOnemore = 95 % rest;
    uint32_t c = idx < charPerBucketOnemore ? idx * (charPerBucket + 1) : idx * charPerBucket + charPerBucketOnemore;
    uint32_t charPerBucketActual = idx < charPerBucketOnemore ? charPerBucket + 1 : charPerBucket;
    c += charPerBucketActual * ratioUp / ratioDown;
    root.key[digitNum] = c+32;
    memset(root.key + digitNum + 1, 0, KEY_SIZE_GENSORT - digitNum - 1);
    return root;
}

