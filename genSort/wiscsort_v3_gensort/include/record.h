#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include "config.h"

typedef struct k_t
{
    char key[KEY_SIZE];

    void operator=(const k_t &rhs)
    {
        memcpy(&key, &rhs.key, KEY_SIZE);
    }

    bool operator<(const k_t &rhs) const
    {
        int n;
        n = memcmp(key, rhs.key, KEY_SIZE);
        if (n > 0)
            return false;
        else
            return true;
    }
    bool operator>(const k_t &rhs) const
    {
        int n;
        n = memcmp(key, rhs.key, KEY_SIZE);
        if (n > 0)
            return true;
        else
            return false;
    }
} k_t;

typedef struct value
{
    char value[VALUE_SIZE];
} value_t;

typedef struct record
{
    k_t k;
    value_t v;
    record(k_t _k, value_t _v)
    {
        k = _k;
        v = _v;
    }
    record() {}
} record_t;

typedef struct in_record
{
    k_t k;
    size_t v_index;

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
        v_index = rhs.v_index;
    }

    in_record(k_t _k, size_t _v_index)
    {
        k = _k;
        v_index = _v_index;
    }

    in_record() {}
} in_record_t;