#include "utils.h"

int32_t main(int32_t argc, char *argv[]) {

    uint32_t num = atol(argv[1]);
    uint32_t partitionNum = atoi(argv[2]);
    uint32_t skew = atoi(argv[3]);
    uint32_t batchSize = atoi(argv[4]);
    uint32_t readTh = atoi(argv[5]);
    uint32_t writeThUpper = atoi(argv[6]);
    uint32_t writeThLower = atoi(argv[7]);
    uint32_t version = atoi(argv[8]);

    batchSize = num / 4000 * batchSize / partitionNum;
    uint32_t bufferSize = batchSize * 2 * partitionNum * sizeof(RecordPtr) / sizeof(Record);  // for Record

    uint32_t initialTh = writeThUpper;
    uint32_t insertTh = readTh;
    uint32_t traversalTh = writeThLower;

    std::cout << "Record num: " << num << ", Tree num: " << partitionNum << ", Batch size: " << batchSize <<
            ", Skew type: " << skew << ", Read thread: " << readTh << ", Initial thread: " << initialTh << 
            ", Insert thread: " << insertTh << ", Traversal thread: " << traversalTh <<
            ", Version: " << version << std::endl;

    if (version == 0) generateData(UINT32_MAX, num, RECORD_VALUESIZE_DEFAULT, skew);
    if (version == 1) et_v1(num, partitionNum, batchSize, readTh, readTh, initialTh , readTh, insertTh, readTh, traversalTh);
    if (version == 2) et_v2(num, partitionNum, batchSize, readTh, readTh, initialTh , readTh, insertTh, readTh, traversalTh);
    if (version == 3) et_v3(num, partitionNum, batchSize, readTh, readTh, initialTh , readTh, insertTh, readTh, traversalTh);
    if (version == 4) et_v4(num, partitionNum, batchSize, readTh, readTh, initialTh , readTh, insertTh, readTh, traversalTh);
    if (version == 5) et_v5(num, partitionNum, batchSize, readTh, readTh, initialTh , readTh, insertTh, readTh, traversalTh);
    if (version == 6) et_v6(num, partitionNum, batchSize, readTh, readTh, initialTh , readTh, insertTh, readTh, traversalTh);
    if (version == 7) et_v7(num, partitionNum, batchSize, readTh, readTh, initialTh , readTh, insertTh, readTh, traversalTh);
    if (version == 8) et_v8(num, partitionNum, batchSize, readTh, readTh, initialTh , readTh, insertTh, readTh, traversalTh);
    if (version == 9) et_v9(num, partitionNum, batchSize, readTh, readTh, initialTh , readTh, insertTh, readTh, traversalTh);
    if (version == 10) et_v10(num, partitionNum, batchSize, readTh, readTh, initialTh , readTh, insertTh, readTh, traversalTh);
    if (version == 11) et_v11(num, partitionNum, batchSize, readTh, readTh, initialTh , readTh, insertTh, readTh, traversalTh);

    if (version == 101) bstar(num, bufferSize, readTh, 2*sqrt(num));
    if (version == 102) quick(num, batchSize, readTh, writeThLower);
    if (version == 103) insertion(num, batchSize, readTh, writeThLower);
    if (version == 104) ips4oKV(num, batchSize, readTh, writeThLower);
    if (version == 105) external(num, batchSize, readTh);
    if (version == 106) quick_write(num, batchSize, readTh, writeThLower);
    if (version == 107) selection(num, batchSize, readTh, writeThLower);
    if (version == 108) ips4o_onlyBAS(num, readTh);
    if (version == 109) ips4o_inPlace(num);
    if (version == 110) introsort(num, batchSize, readTh, writeThLower);
    
    printf("\n");

    return 0;
}
