#include "utils.h"

#define RECORD_SIZE (sizeof(Record))
#define MAX_RECORDNUM_PERROUND 200000000

void generateRandomKey(uint32_t* array, uint32_t range, uint32_t num) {
    srand(104);
    uint32_t randomNum;
    uint32_t randomFactor;
    for (uint32_t i = 0; i < num; ++i) {
        if (range > INT32_MAX) {
            randomFactor = rand() % 2;
            randomNum = rand() * (randomFactor + 1);
        }
        else {
            randomNum = rand() % range;
        }
        *(array + i) = randomNum;
    }
}


void randomShuffle(uint32_t* array, uint32_t start, uint32_t end) {
    srand(66);
    
    uint32_t tmp;
    uint32_t randomNum;

    for (uint32_t i = start; i < end; ++i) {
        randomNum = rand() % (end - start + 1);
        tmp = *(array + i);
        *(array + i) = *(array + start + randomNum);
        *(array + start + randomNum) = tmp;
    }
}


uint32_t generateData(uint32_t range, uint32_t num, uint32_t size, uint32_t skew) {
    printf("Generating Data in PM... \n");
    printf("    - Number of records: %u \n", num);
    printf("    - Record Size: %luB (%luB key + %uB value) \n", size + sizeof(range), sizeof(range), size);

    uint32_t* keys = new uint32_t[num];

    if (skew > 100) {
        uint32_t outlier = num/400000;
        generateRandomKey(keys, range/10, num-outlier);
        generateRandomKey(keys+num-outlier, range, outlier);
        skew = 0;
    }
    else {
        generateRandomKey(keys, range, num);
    }

    if (skew != 0) {
        ips4o::parallel::sort(keys, keys+num, std::less<>{});
        // ordered
        if (skew == 1) ;
        // interleaved ordered
        else if (skew == 2) {
            uint32_t half = num >> 1;
            uint32_t tmp;
            for (uint32_t i = 0; i < half; ++i) {
                tmp = *(keys + i);
                *(keys + i) = *(keys + half + i);
                *(keys + half + i) = tmp;
            }
            srand(0);
            uint32_t shift_l = random() % half;  // 把前面的shift_l个放到后面
            uint32_t shift_r = random() % half;
            uint32_t* tmpArray = new uint32_t[half];
            // left
            memcpy(tmpArray, keys, sizeof(uint32_t) * half);
            memcpy(keys, tmpArray + shift_l, sizeof(uint32_t) * (half - shift_l));
            memcpy(keys + half - shift_l, tmpArray, sizeof(uint32_t) * shift_l);
            // right
            memcpy(tmpArray, keys + half, sizeof(uint32_t) * half);
            memcpy(keys + half, tmpArray + shift_r, sizeof(uint32_t) * (half - shift_r));
            memcpy(keys + 2 * half - shift_r, tmpArray, sizeof(uint32_t) * shift_r);
        }
        // reversed ordered
        else if (skew == 3) {
            uint32_t half = num >> 1;
            uint32_t tmp;
            for (uint32_t i = 0; i < half; ++i) {
                tmp = *(keys + i);
                *(keys + i) = *(keys + num - 1 - i);
                *(keys + num - 1 - i) = tmp;
            }
        }
        // partial ordered (skew%)
        else if (skew >= 10) {
            uint32_t start = num / 100 * skew;
            randomShuffle(keys, start, num-1);
        }
        // error
        else {
            printf("ERROR! Generate data fail. Wrong skew schema.\n");
            return 0;
        }
    }

    Record* pmemAddr = mmap_pmem_file<Record>(FILENAME, num);
    // when input file is super large, our DRAM cannot handle it with only one round
    uint32_t roundNum = num / MAX_RECORDNUM_PERROUND;
    uint32_t rest = num % MAX_RECORDNUM_PERROUND;
    
    Record* records = new Record[MAX_RECORDNUM_PERROUND];
    for (uint32_t roundIdx = 0; roundIdx < roundNum; ++roundIdx) {
        uint32_t startIdx = roundIdx * MAX_RECORDNUM_PERROUND;
        for (uint32_t i = 0; i < MAX_RECORDNUM_PERROUND; ++i) {
            (records + i)->key = *(keys + startIdx + i);
            memset((records + i)->value, 'o', RECORD_VALUESIZE_DEFAULT);
        }
        pmem_memcpy_persist(pmemAddr + startIdx, records, RECORD_SIZE * MAX_RECORDNUM_PERROUND);
    }
    uint32_t startIdx = roundNum * MAX_RECORDNUM_PERROUND;
    for (uint32_t i = 0; i < rest; ++i) {
        (records + i)->key = *(keys + startIdx + i);
        memset((records + i)->value, 'o', RECORD_VALUESIZE_DEFAULT);
    }
    pmem_memcpy_persist(pmemAddr + startIdx, records, RECORD_SIZE * rest);


    for(uint32_t i = 0; i < 6; ++i) {
        printf("%u ", pmemAddr[i].key);
    }
    printf("\n");
    
    delete[] records;
    delete[] keys;
    unmmap_pmem_file<Record>(pmemAddr, num);

    printf("Generate data successfully.\n");
    return 1;
}


