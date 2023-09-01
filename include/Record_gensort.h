#pragma once

#define KEY_SIZE_GENSORT 10
#define VALUE_SIZE_GENSORT 90

typedef struct Key {
    char key[KEY_SIZE_GENSORT];

    // void operator=(const Key &r)
    // {
    //     memcpy(&key, &r.key, KEY_SIZE_GENSORT);
    // }

    bool operator<(const Key &r) const
    {
        int res = memcmp(key, r.key, KEY_SIZE_GENSORT);
        return res <= 0 ? true : false;
    }
    bool operator>(const Key &r) const
    {
        int res = memcmp(key, r.key, KEY_SIZE_GENSORT);
        return res >= 0 ? true : false;
    }
    bool operator==(const Key &r)
    {
        return memcmp(key, &r, KEY_SIZE_GENSORT) == 0;
    }
} Key; 


typedef struct Record_GenSort {
    Key key;
    char value[VALUE_SIZE_GENSORT];

    bool operator<(const Record_GenSort &r) const
    {
        return key < r.key;
    }
    // void operator=(const Record_GenSort &r)
    // {
    //     key = r.key;
    //     memcpy(value, r.value, VALUE_SIZE_GENSORT);
    // }
    // Record_GenSort(Key _key, char* _value) {
    //     key = _key;
    //     memcpy(value, _value, VALUE_SIZE_GENSORT);
    // }
} Record_GenSort;   // 100


typedef struct RecordPtr_GenSort {
    Key key;
    Record_GenSort* ptr;

    bool operator<(const RecordPtr_GenSort &r) const
    {
        return key < r.key;
    }
    // void operator=(const RecordPtr_GenSort &r)
    // {
    //     key = r.key;
    //     ptr = r.ptr;
    // }
    // RecordPtr_GenSort(Key _key, Record_GenSort* _ptr) {
    //     key = _key;
    //     ptr = _ptr;
    // }
} RecordPtr_GenSort;    // 24