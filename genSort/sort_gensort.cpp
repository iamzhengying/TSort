#include "utils.h"

int32_t main(int32_t argc, char *argv[]) {

    string inputFile = argv[1];
    uint32_t num = atol(argv[2]);
    uint32_t partitionNum = atoi(argv[3]);
    uint32_t skew = atoi(argv[4]);
    uint32_t batchSize = atoi(argv[5]);
    uint32_t readTh = atoi(argv[6]);
    uint32_t writeThUpper = atoi(argv[7]);
    uint32_t writeThLower = atoi(argv[8]);
    uint32_t version = atoi(argv[9]);

    uint32_t initialTh = writeThUpper;
    uint32_t insertTh = readTh;
    uint32_t traversalTh = writeThLower;

    std::cout << "Record num: " << num << ", Tree num: " << partitionNum << ", Batch size: " << batchSize <<
            ", Skew type: " << skew << ", Read thread: " << readTh << ", Write thread lower: " << writeThLower << 
            ", Write thread upper: " << writeThUpper << ", Insert thread: " << insertTh << 
            ", Version: " << version << std::endl;

    if (version == 1) et_v7_gensort(inputFile.c_str(), num, partitionNum, batchSize, readTh, initialTh, readTh, insertTh, readTh, traversalTh);
    if (version == 2) ips4o_gensort(inputFile.c_str(), num, batchSize, readTh, traversalTh);

    printf("\n");

    return 0;
}
