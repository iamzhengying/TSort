////////////////////////////////////////////
//
// Title: b* Sort
// Version: 
// Details: multi-threading, generate key-value pairs
// Author: Zheng Ying
// Comment: 
//
////////////////////////////////////////////

#include <set>

#include "utils.h"

TunnelEntryLock* findTargetTunnelEntry(TunnelEntryLock* tunnelList, uint32_t pmemEntryCount, uint32_t key);
void bstarInsert(TreeNodeLock* root, TreeNodeLock* node, uint32_t* regLength, uint32_t* regCeiling, uint32_t* regFloor, TreeNodeLock* regRoot, uint32_t th);
void inOrderTraversal(TreeNodeLock* root, Record* pmemAddr_sorted, uint32_t* counter, Record* buffer, uint32_t* dramCounter, uint32_t thrd);
void validatePointerFile(Record* sortedFile, uint32_t num, uint32_t counter);
uint32_t levelTraversal(TreeNodeLock* root);

void bstar(uint32_t num, uint32_t bufferSize, uint32_t threadNum, uint32_t threshold) {
    Record* records = mmap_pmem_file<Record>(FILENAME, num);

    printf("B* Sort...\n");
    Tstart(startTimer);

    uint32_t listNum = num;
    uint32_t pmemNodeCount = 0;
    uint32_t pmemEntryCount = 0;
    std::mutex pmemNodeMutex;
    std::mutex pmemEntryMutex;
    TreeNodeLock* recordTrees = mmap_pmem_file<TreeNodeLock>(FILENAME_BSTAR_PTR, num);
    TunnelEntryLock* tunnelList = mmap_pmem_file<TunnelEntryLock>(FILENAME_BSTAR_LIST, listNum);

    // initialization
    // register uint32_t th = 2 * sqrt(num);
    // register uint32_t th = threshold;
    uint32_t th = threshold;
    recordTrees->key = records->key;
    recordTrees->val = records;
    ++pmemNodeCount;
    tunnelList->ceiling = UINT32_MAX;
    tunnelList->floor = 0;
    tunnelList->root = recordTrees;
    ++pmemEntryCount;
    
    // construction
    #pragma omp parallel for num_threads(threadNum)
    for (uint32_t i = 1; i < num; ++i) {
        pmemNodeMutex.lock();
        uint32_t nodeIdx = pmemNodeCount;
        ++pmemNodeCount;
        pmemNodeMutex.unlock();
        recordTrees[nodeIdx].key = (records + i)->key;
        recordTrees[nodeIdx].val = records + i;
        // pmemEntryMutex.lock();
        TunnelEntryLock* tunnelEntry = findTargetTunnelEntry(tunnelList, pmemEntryCount, (records + i)->key);
        // pmemEntryMutex.unlock();
        uint32_t regLength = 1;
        // register uint32_t regCeiling = tunnelEntry->ceiling;
        // register uint32_t regFloor = tunnelEntry->floor;
        // register TreeNodeLock* regRoot = tunnelEntry->root;
        uint32_t regCeiling = tunnelEntry->ceiling;
        uint32_t regFloor = tunnelEntry->floor;
        TreeNodeLock* regRoot = tunnelEntry->root;
        bstarInsert(regRoot, recordTrees + nodeIdx, &regLength, &regCeiling, &regFloor, regRoot, th);
        // create new entry
        if (regLength >= th) {
            // printf("Create sub-tree.\n");
            TunnelEntryLock newEntry = {regFloor, regCeiling, regRoot, tunnelEntry};
            pmemEntryMutex.lock();
            if (pmemEntryCount >= listNum) {
                printf("No enough space to store tunnel entry. \n");
            }
            memcpy(tunnelList + pmemEntryCount, &newEntry, sizeof(TunnelEntryLock));
            ++pmemEntryCount;
            pmemEntryMutex.unlock();
        }
    }
    Tend(endTimer);
    printf("Sorting ");
    singleLatency();

    // Reading Sorted Data
    Tstart(startTimer);
    Record* pmemAddr_sorted = mmap_pmem_file<Record>(FILENAME_BSTAR_SORTED, num);
    Record* buffer = new Record[bufferSize];
    uint32_t counter = 0;
    uint32_t dramCounter = 0;
    inOrderTraversal(recordTrees, pmemAddr_sorted, &counter, buffer, &dramCounter, bufferSize);
    if (counter < num) {
        if (counter + dramCounter == num) {
            memcpy(pmemAddr_sorted + counter, buffer, sizeof(Record) * dramCounter);
        }
        else {
            printf("ERROR! lost some elements during traversal.\n");
        }
    }
    Tend(endTimer);
    printf("Reading ");
    singleLatency();

    // Validation
    Tstart(startTimer);
    validatePointerFile(pmemAddr_sorted, num, counter);
    Tend(endTimer);
    printf("Validation ");
    singleLatency();

    // count level
    uint32_t level = levelTraversal(recordTrees);
    printf("height: %d\n", level);

    // count writes
    // checkWrites(recordTrees);

    printf("Create %u sub-trees.\n", pmemEntryCount);
    
    delete[] buffer;
    unmmap_pmem_file<Record>(pmemAddr_sorted, num);
    unmmap_pmem_file<TunnelEntryLock>(tunnelList, listNum);
    unmmap_pmem_file<TreeNodeLock>(recordTrees, num);
    unmmap_pmem_file<Record>(records, num);
}


TunnelEntryLock* findTargetTunnelEntry(TunnelEntryLock* tunnelList, uint32_t pmemEntryCount, uint32_t key) {
    for (uint32_t i = pmemEntryCount - 1; i >= 0; --i) {
        if (key < (tunnelList + i)->ceiling && key > (tunnelList + i)->floor) {
            return tunnelList + i;
        }
        if (i == 0) break;
    }
    return tunnelList;
}


void bstarInsert(TreeNodeLock* root, TreeNodeLock* node, uint32_t* regLength, uint32_t* regCeiling, uint32_t* regFloor, TreeNodeLock* regRoot, uint32_t th) {
    ++(*regLength);
    if (node->key <= root->key) {
        root->mtx.lock();
        if (root->left == nullptr) {
            root->left = node;
            root->mtx.unlock();
        }
        else {
            root->mtx.unlock();
            if (*regLength < th / 2 + 1) {
                *regCeiling = min(*regCeiling, root->key);
            }
            else if (*regLength == th / 2 + 1) {
                regRoot = root;
            }
            bstarInsert(root->left, node, regLength, regCeiling, regFloor, regRoot, th);
        }
    }
    else {
        root->mtx.lock();
        if (root->right == nullptr) {
            root->right = node;
            root->mtx.unlock();
        }
        else {
            root->mtx.unlock();
            if (*regLength < th / 2 + 1) {
                *regFloor = max(*regFloor, root->key);
            }
            else if (*regLength == th / 2 + 1) {
                regRoot = root;
            }
            bstarInsert(root->right, node, regLength, regCeiling, regFloor, regRoot, th);
        }
    }
}


void inOrderTraversal(TreeNodeLock* root, Record* pmemAddr_sorted, uint32_t* counter, Record* buffer, uint32_t* dramCounter, uint32_t thrd) {
	if (root == NULL) return;
	if (root->left != NULL) {
		inOrderTraversal(root->left, pmemAddr_sorted, counter, buffer, dramCounter, thrd);
	}
	if (root->val != nullptr) {
		memcpy(buffer + (*dramCounter), root->val, sizeof(Record));
		++(*dramCounter);
		if (*dramCounter >= thrd) {
			memcpy(pmemAddr_sorted + (*counter), buffer, sizeof(Record) * thrd);
			(*dramCounter) = 0;
			(*counter) += thrd;
		}
	}
	if (root->right != NULL) {
		inOrderTraversal(root->right, pmemAddr_sorted, counter, buffer, dramCounter, thrd);
	}
}


void validatePointerFile(Record* sortedFile, uint32_t num, uint32_t counter) {
	if (counter != num) {
		printf("Validation Failed. Lost some elements.\n");
		return;
	}
	uint32_t preKey = 0;
	uint32_t curKey;
	for (uint32_t i = 0; i < num; ++i) {
		curKey = (sortedFile + i)->key;
		if (curKey < preKey) {
			printf("Validation Failed. Wrong order happened in pos # %u: Before %u is %u. \n", i, curKey, preKey);
			return;
		}
		preKey = curKey;
	}
	printf("Validation Successful.\n");
}


uint32_t levelTraversal(TreeNodeLock* root) {
	queue<TreeNodeLock*> q;
	uint32_t level = 0;
	q.push(root);
	while (!q.empty()) {
		uint32_t num = q.size();
		++level;
		for(uint32_t i = 0; i < num; ++i) {
			TreeNodeLock* node = q.front();
			q.pop();
			if (node->left != NULL) {
				q.push(node->left);
			}
			if (node->right != NULL) {
				q.push(node->right);
			}
		}
	}
	return level;
}
