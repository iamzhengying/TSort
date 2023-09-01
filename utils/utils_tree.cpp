#include "utils.h"

#ifdef __cplusplus
#include <stdint.h>
extern "C"
{
    void* __memmove_chk_avx512_no_vzeroupper(void *dest, void *src, uint32_t s);
    void* __memset_chk_avx512_no_vzeroupper(void *dest, int c, uint32_t s);
}
#endif


#define MAX_TREE_SIZE 950000
uint32_t pmemCountArr[MAX_TREE_SIZE] = {0};
static std::mutex pmemCountMtx[MAX_TREE_SIZE];



///////////////////////////////////////////
///////////////////////////////////////////
// For Initialization
///////////////////////////////////////////

template<typename T1, typename T2>
void buildTree(T1* rootAddr, uint32_t idx, T2 key) {
	rootAddr->key = key;
	rootAddr->val = nullptr;
	rootAddr->left = nullptr;
	rootAddr->right = nullptr;
	++pmemCountArr[idx];;
}
template void buildTree<TreeNode, uint32_t>(TreeNode*, uint32_t, uint32_t);
template void buildTree<TreeNodeLock, uint32_t>(TreeNodeLock*, uint32_t, uint32_t);
template void buildTree<TreeNode_GenSort, Key>(TreeNode_GenSort*, uint32_t, Key);


///////////////////////////////////////////
///////////////////////////////////////////
// For Construction
///////////////////////////////////////////

template <typename T1, typename T2>		// T1:RecordPtr
uint32_t findMid(T2 key, T1* bufferStart, uint32_t start, uint32_t end) {
	if (start == end) {
		return end;
	}
	uint32_t midLeft = (start + end) >> 1;
	uint32_t midRight = midLeft + 1;
	if ((bufferStart[midLeft].key < key || bufferStart[midLeft].key == key) 
			&& bufferStart[midRight].key > key) {
		return midLeft;
	}
	else if (bufferStart[midLeft].key > key) {
		return findMid<T1, T2>(key, bufferStart, start, midLeft);
	}
	else {
		return findMid<T1, T2>(key, bufferStart, midRight, end);
	}
}
template uint32_t findMid<Record, uint32_t>(uint32_t, Record*, uint32_t, uint32_t);
template uint32_t findMid<Record_GenSort, Key>(Key, Record_GenSort*, uint32_t, uint32_t);


template <typename T1, typename T2>		// T1:TreeNode, T2:RecordPtr
T1* batchBuildTree(T2* bufferStart, uint32_t start, uint32_t end, T1* pmemAddr, uint32_t idx) {
	if (start > end) return NULL;
	
	T1* node = pmemAddr + pmemCountArr[idx];
	pmemCountArr[idx] += 1;

	uint32_t mid = (start + end) >> 1;
	node->key = bufferStart[mid].key;
	node->val = bufferStart[mid].ptr;
	if (mid > 0) {
		node->left = batchBuildTree<T1, T2>(bufferStart, start, mid-1, pmemAddr, idx);
	}
	node->right = batchBuildTree<T1, T2>(bufferStart, mid+1, end, pmemAddr, idx);
	return node;
}
template TreeNode* batchBuildTree<TreeNode, RecordPtr>(RecordPtr*, uint32_t, uint32_t, TreeNode*, uint32_t);
template TreeNode_GenSort* batchBuildTree<TreeNode_GenSort, RecordPtr_GenSort>(RecordPtr_GenSort*, uint32_t, uint32_t, TreeNode_GenSort*, uint32_t);


template <typename T1, typename T2, typename T3> 	// T1:TreeNode, T2:RecordPtr, T3:uint32_t/Key
void  batchInsert(T1* node, T2* bufferStart, uint32_t start, uint32_t end, T1* pmemAddr, uint32_t idx) {
	T3 key = node->key;
	if (bufferStart[end].key < key || bufferStart[end].key == key) {
		if (node->left == nullptr) {
			// insert batch[start, end]
			node->left = batchBuildTree<T1, T2>(bufferStart, start, end, pmemAddr, idx);
		}
		else {
			batchInsert<T1, T2, T3>(node->left, bufferStart, start, end, pmemAddr, idx);
		}
	}
	else if (bufferStart[start].key > key) {
		if (node->right == nullptr) {
			// insert batch[start, end]
			node->right = batchBuildTree<T1, T2>(bufferStart, start, end, pmemAddr, idx);
		}
		else {
			batchInsert<T1, T2, T3>(node->right, bufferStart, start, end, pmemAddr, idx);
		}
		
	}
	else {
		uint32_t mid = findMid<T2, T3>(key, bufferStart, start, end);
		if (node->left == nullptr) {
			// insert batch[start, mid]
			node->left = batchBuildTree<T1, T2>(bufferStart, start, mid, pmemAddr, idx);
		}
		else {
			batchInsert<T1, T2, T3>(node->left, bufferStart, start, mid, pmemAddr, idx);
		}
		if (node->right == nullptr) {
			// insert batch[mid+1, end]
			node->right = batchBuildTree<T1, T2>(bufferStart, mid+1, end, pmemAddr, idx);
		}
		else {
			batchInsert<T1, T2, T3>(node->right, bufferStart, mid+1, end, pmemAddr, idx);
		}
	}
}
template void  batchInsert<TreeNode, RecordPtr, uint32_t>(TreeNode*, RecordPtr*, uint32_t, uint32_t, TreeNode*, uint32_t);
template void  batchInsert<TreeNode_GenSort, RecordPtr_GenSort, Key>(TreeNode_GenSort*, RecordPtr_GenSort*, uint32_t, uint32_t, TreeNode_GenSort*, uint32_t);



////////////////////////////////////////
// for no batching + multi-thread, i.e., lock nodes
//////////////////////////////////////// 

TreeNodeLock* batchBuildTree(RecordPtr* bufferStart, uint32_t start, uint32_t end, TreeNodeLock* pmemAddr, uint32_t idx) {
	if (start > end) return NULL;
	
	pmemCountMtx[idx].lock();
	TreeNodeLock* node = pmemAddr + pmemCountArr[idx];
	pmemCountArr[idx] += 1;
	pmemCountMtx[idx].unlock();

	uint32_t mid = (start + end) >> 1;
	node->key = bufferStart[mid].key;
	node->val = bufferStart[mid].ptr;
	if (mid > 0) {
		node->left = batchBuildTree(bufferStart, start, mid-1, pmemAddr, idx);
	}
	node->right = batchBuildTree(bufferStart, mid+1, end, pmemAddr, idx);
	return node;
}


// assume there is only one ele in the buffer (start == end)
void batchInsert(TreeNodeLock* node, RecordPtr* bufferStart, uint32_t start, uint32_t end, TreeNodeLock* pmemAddr, uint32_t idx) {
	uint32_t key = node->key;
	if (bufferStart[end].key <= key) {
		if (node->left == nullptr) {
			// insert batch[start, end]
			node->left = batchBuildTree(bufferStart, start, end, pmemAddr, idx);
			node->mtx.unlock();
		}
		else {
			node->left->mtx.lock();
			node->mtx.unlock();
			batchInsert(node->left, bufferStart, start, end, pmemAddr, idx);
		}
	}
	else {
		if (node->right == nullptr) {
			// insert batch[start, end]
			node->right = batchBuildTree(bufferStart, start, end, pmemAddr, idx);
			node->mtx.unlock();
		}
		else {
			node->right->mtx.lock();
			node->mtx.unlock();
			batchInsert(node->right, bufferStart, start, end, pmemAddr, idx);
		}
		
	}
}


////////////////////////////////////////
// for binary search ->  get target treeIdx
//////////////////////////////////////// 

uint32_t findTree(uint32_t key, uint32_t* spliter, uint32_t start, uint32_t end) {
	if (start == end) return end;
	int32_t mid = (start + end) >> 1;
	if (key == spliter[mid]) return mid;
	else if (key > spliter[mid]) {
		return findTree(key, spliter, mid + 1, end);
	}
	else {
		return findTree(key, spliter, start, mid);
	}
}



////////////////////////////////////////
// for async
//////////////////////////////////////// 

void insert(RecordPtr* recordPtr, Buffer* buffer, uint32_t batchSize, TreeNode* root, uint32_t treeIdx) {
	buffer->insertMutex.lock();
	memcpy(buffer->insertBuffer + buffer->insertSize, recordPtr, sizeof(RecordPtr));
	++buffer->insertSize;
	bool insertFlag = false;
	if (buffer->insertSize == batchSize) {
		buffer->treeMutex.lock();
		RecordPtr* tmp = buffer->insertBuffer;
		buffer->insertBuffer = buffer->treeBuffer;
		buffer->treeBuffer = tmp;
		buffer->insertSize = 0;
		insertFlag = true;
	}
	buffer->insertMutex.unlock();
	if (insertFlag) {
		ips4o::parallel::sort(buffer->treeBuffer, buffer->treeBuffer + batchSize, std::less<>{});
		batchInsert<TreeNode, RecordPtr, uint32_t>(root, buffer->treeBuffer, 0, batchSize-1, root, treeIdx);
		buffer->treeMutex.unlock();
	}
}

template <typename T1, typename T2, typename T3, typename T4>		// T1:RecordPtr, T2:SingleBuffer, T3:TreeNode, T4: uint32_t/Key(for batchInsert's input)
void insert(T1* recordPtr, T2* buffer, uint32_t batchSize, T3* root, uint32_t treeIdx) {
	buffer->insertMutex.lock();
	memcpy(buffer->insertBuffer + buffer->insertSize, recordPtr, sizeof(T1));
	++buffer->insertSize;
	if (buffer->insertSize == batchSize) {
		ips4o::parallel::sort(buffer->insertBuffer, buffer->insertBuffer + batchSize, std::less<>{});
		batchInsert<T3, T1, T4>(root, buffer->insertBuffer, 0, batchSize-1, root, treeIdx);
		buffer->insertSize = 0;
	}
	buffer->insertMutex.unlock();
}
template void insert<RecordPtr, SingleBuffer, TreeNode, uint32_t>(RecordPtr* recordPtr, SingleBuffer* buffer, uint32_t batchSize, TreeNode* root, uint32_t);
template void insert<RecordPtr_GenSort, SingleBuffer_GenSort, TreeNode_GenSort, Key>(RecordPtr_GenSort* recordPtr, SingleBuffer_GenSort* buffer, uint32_t batchSize, TreeNode_GenSort* root, uint32_t);


///////////////////////////////////////////
///////////////////////////////////////////
// For Traversal
///////////////////////////////////////////

template<typename T1, typename T2>			// T1:TreeNode, T2:Record
void inOrderTraversal(T1* root, T2* pmem, uint32_t* idx) {
	if (root == NULL) return;
	if (root->left != NULL) {
		inOrderTraversal(root->left, pmem, idx);
	}
	if (root->val != nullptr) {
#ifdef avx512
		__memmove_chk_avx512_no_vzeroupper(pmem + (*idx), root->val, sizeof(Record));
#else
		memcpy(pmem + (*idx), root->val, sizeof(T2));
#endif
		++(*idx);
	}
	if (root->right != NULL) {
		inOrderTraversal(root->right, pmem, idx);
	}
}
template void inOrderTraversal<TreeNode, Record>(TreeNode*, Record*, uint32_t*);
template void inOrderTraversal<TreeNodeLock, Record>(TreeNodeLock*, Record*, uint32_t*);
template void inOrderTraversal<TreeNode_GenSort, Record_GenSort>(TreeNode_GenSort*, Record_GenSort*, uint32_t*);



////////////////////////////////////////
// for single tree traversal (treeSize > dramBuffer)
//////////////////////////////////////// 

template<typename T>
void inOrderTraversal(T* root, Record* pmemAddr_sorted, uint32_t* counter, Record* buffer, uint32_t* dramCounter, uint32_t thrd) {
	if (root == NULL) return;
	if (root->left != NULL) {
		inOrderTraversal(root->left, pmemAddr_sorted, counter, buffer, dramCounter, thrd);
	}
	if (root->val != nullptr) {
		memcpy(buffer + (*dramCounter), root->val, sizeof(Record));
		++(*dramCounter);
		if (*dramCounter >= thrd) {
#ifdef avx512
            __memmove_chk_avx512_no_vzeroupper(pmemAddr_sorted + (*counter), buffer, sizeof(Record) * thrd);
#else
            memcpy(pmemAddr_sorted + (*counter), buffer, sizeof(Record) * thrd);
#endif
			(*dramCounter) = 0;
			(*counter) += thrd;
		}
	}
	if (root->right != NULL) {
		inOrderTraversal(root->right, pmemAddr_sorted, counter, buffer, dramCounter, thrd);
	}
}
template void inOrderTraversal<TreeNode>(TreeNode*, Record*, uint32_t*, Record*, uint32_t*, uint32_t);



///////////////////////////////////////////
///////////////////////////////////////////
// For Validation
///////////////////////////////////////////

template<typename T1, typename T2>
void validateFile(Record* sortedFile, uint32_t num) {
	uint32_t preKey = 0;
	for (uint32_t i = 0; i < num; ++i) {
		if (!(sortedFile + i)) {
			printf("Validation Failed. The error happens in position %u. Records missed", i);
			return;
		}
		if ((sortedFile + i)->key < preKey) {
			printf("Validation Failed. The error happens in position %u. Before %u is %u.\n", i, (sortedFile + i)->key, preKey);
			return;
		}
		preKey = (sortedFile + i)->key;
    }
	printf("Validation Successful.\n");
	printf("First element is %u, last element is %u.\n", sortedFile->key, (sortedFile + num - 1)->key );
}


void validateFile(Record_GenSort* sortedFile, uint32_t num) {
	Key preKey;
    memset(preKey.key, 0, KEY_SIZE_GENSORT);
	for (int64_t i = 0; i < num; ++i) {
		if (!(sortedFile + i)) {
			cout << "Validation Failed. The error happens in position " << i << ". Records missed" << endl;
			return;
		}
		if ((sortedFile + i)->key < preKey) {
			cout << "Validation Failed. The error happens in position " << i << ". Before '";
			for (int i = 0; i < KEY_SIZE_GENSORT; ++i) cout << (sortedFile + i)->key.key[i];
			cout << "' is '";
			for (int i = 0; i < KEY_SIZE_GENSORT; ++i) cout << preKey.key[i];
			cout << "'." << endl;
			return;
		}
		preKey = (sortedFile + i)->key;
    }
	cout << "Validation Successful. \n" << "First element is '";
	for (int i = 0; i < KEY_SIZE_GENSORT; ++i) cout << sortedFile->key.key[i];
	cout << "', last element is '";
	for (int i = 0; i < KEY_SIZE_GENSORT; ++i) cout << (sortedFile + num - 1)->key.key[i];
	cout << "'." << endl;
}



///////////////////////////////////////////
///////////////////////////////////////////
// For Tree Height
///////////////////////////////////////////

template<typename T>
uint32_t levelTraversal(T* root) {
	queue<T*> q;
	uint32_t level = 0;
	q.push(root);
	while (!q.empty()) {
		uint32_t num = q.size();
		++level;
		for(uint32_t i = 0; i < num; ++i) {
			T* node = q.front();
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
template uint32_t levelTraversal<TreeNode>(TreeNode* root);
template uint32_t levelTraversal<TreeNodeLock>(TreeNodeLock* root);
template uint32_t levelTraversal<TreeNode_GenSort>(TreeNode_GenSort* root);
