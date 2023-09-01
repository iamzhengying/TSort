#pragma once

typedef struct WriteCount {
    int32_t count = 0;
    std::mutex mutex;
} WriteCount;