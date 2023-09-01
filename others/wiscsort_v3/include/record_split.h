#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include "config.h"
#include <stdint.h>

// struct k_t
// {
//     char key[KEY_SIZE];

//     void operator=(const k_t &rhs)
//     {
//         memcpy(&key, &rhs.key, KEY_SIZE);
//     }
//     // Initilizes key with a constant char
//     void operator=(const int rhs)
//     {
//         for (int i = 0; i < KEY_SIZE; ++i)
//             key[i] = rhs;
//     }
//     bool operator==(const k_t &rhs)
//     {
//         return memcmp(key, &rhs, KEY_SIZE) == 0;
//     }
//     bool operator<(const k_t &rhs) const
//     {
//         int n;
//         n = memcmp(key, rhs.key, KEY_SIZE);
//         if (n > 0)
//             return false;
//         else
//             return true;
//     }
//     bool operator>(const k_t &rhs) const
//     {
//         int n;
//         n = memcmp(key, rhs.key, KEY_SIZE);
//         if (n > 0)
//             return true;
//         else
//             return false;
//     }
// };

// typedef struct value
// {
//     char value[VALUE_SIZE];
// } value_t;

typedef struct record
{
    uint32_t k;
    char v[VALUE_SIZE];

    bool operator<(const record &rhs) const
    {
        return k < rhs.k;
    }

    bool operator>(const record &rhs) const
    {
        return k > rhs.k;
    }

    // record(k_t _k, value_t _v)
    // {
    //     k = _k;
    //     v = _v;
    // }
    // record() {}
} record_t;

// // This is record offset not value offset
// struct roffset
// {
//     uint8_t val[INDEX_SIZE];
//     void operator=(const size_t &rhs)
//     {
//         // convert given uuint32_t to a hex byte array
//         for (int i = 0; i < INDEX_SIZE; ++i)
//         {
//             val[i] = (rhs >> i * 8) & 0xFF;
//         }
//     }
//     void operator=(const roffset &rhs)
//     {
//         memcpy(&val, &rhs.val, INDEX_SIZE);
//     }
//     roffset() {}
// };

// typedef struct record_ot
// {
//     k_t k;
//     roffset roff;
//     record_ot(k_t _k, roffset _roff)
//     {
//         k = _k;
//         roff = _roff;
//     }
//     record_ot() {}
// } record_ot;

typedef struct in_record
{
    uint32_t k;
    record_t* r_off;

    bool operator<(const in_record &rhs) const
    {
        return k < rhs.k;
    }

    bool operator>(const in_record &rhs) const
    {
        return k > rhs.k;
    }

    void operator=(const in_record &rhs)
    {
        k = rhs.k;
        r_off = rhs.r_off;
    }

    in_record(uint32_t _k, record_t* _off)
    {
        k = _k;
        r_off = _off;
    }

    in_record() {}
} in_record_t;

// A struct that maintains input file offset to read from
// and the offset at which the write buffer must be filled.
typedef struct read_offs
{
    record_t* input_off;
    size_t output_buff_off;
} read_offs;