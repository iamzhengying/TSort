#pragma once

typedef struct TreeNode {
    uint32_t key;
    Record* val;
    TreeNode* left = NULL;
    TreeNode* right = NULL;
} TreeNode;         // size: 32

typedef struct TreeNodeLock {
    uint32_t key;
    Record* val;
    TreeNodeLock* left = NULL;
    TreeNodeLock* right = NULL;
    std::mutex mtx;
} TreeNodeLock;         // size: 32
