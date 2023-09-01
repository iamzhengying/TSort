#pragma once

#define RECORD_VALUESIZE_DEFAULT 124

typedef struct Record {
    uint32_t key;
    char value[RECORD_VALUESIZE_DEFAULT];
} Record;       // 256

typedef struct RecordPtr {
    uint32_t key;
    Record* ptr;

    bool operator<(const RecordPtr &r) const
    {
        return key < r.key;
    }

    bool operator>(const RecordPtr &r) const
    {
        return key > r.key;
    }
} RecordPtr;    // 16

// for external sort (heap)
typedef struct RecordIdx {
    uint32_t key;
    uint32_t idx;
} RecordIdx;