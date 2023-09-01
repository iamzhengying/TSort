#pragma once

typedef struct TreeNode_GenSort {
    Key key;
    Record_GenSort* val;
    TreeNode_GenSort* left = nullptr;
    TreeNode_GenSort* right = nullptr;
} TreeNode_GenSort;


typedef struct TreeNode_Dummy_GenSort {
    uint32_t key;
    TreeNode_GenSort* left = nullptr;
    TreeNode_GenSort* right = nullptr;
} TreeNode_Dummy_GenSort;
