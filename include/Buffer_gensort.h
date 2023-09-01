#pragma once

// #include "Record_gensort.h"

typedef struct SingleBuffer_GenSort {
    std::mutex insertMutex;
    uint32_t insertSize = 0;
    RecordPtr_GenSort* insertBuffer;
} SingleBuffer_GenSort;
