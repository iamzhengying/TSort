#pragma once

typedef struct Buffer {
    std::mutex insertMutex;
    std::mutex treeMutex;
    uint32_t insertSize = 0;
    RecordPtr* insertBuffer;
    RecordPtr* treeBuffer;
} Buffer;

typedef struct SingleBuffer {
    std::mutex insertMutex;
    uint32_t insertSize = 0;
    RecordPtr* insertBuffer;
} SingleBuffer;
