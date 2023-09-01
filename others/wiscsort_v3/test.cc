#include <string>
#include "config.h"
#include "ext_sort.h"
#include <getopt.h>
#include <iostream>

void parseCmdline(int, char **);

struct Config conf;

int main(int argc, char **argv)
{
    parseCmdline(argc, argv);

    ExtSort sorter(conf.output_dir);
    sorter.Sort(conf.files);
}

void print_help_message(const char *progname)
{
    /* take only the last portion of the path */
    const char *basename = strrchr(progname, '/');
    basename = basename ? basename + 1 : progname;

    printf("WiscSort usage: %s [OPTION]\n", basename);
    printf("\t -h, --help\n"
           "\t Print this help and exit.\n"
           "\t Change the RECORD size in config.h\n"
           "\t If you need help with workload generation check Gensort.\n");
    printf("\t -i, --input[=FILENAMES]\n"
           "\t List of unsorted files.\n");
    printf("\t -o, --output_dir[=OUTPUTDIR]\n"
           "\t Output dir (can be different mount points)\n"
           "\t The output file is saved as 'sorted'.\n");
    printf("\t -b, --block\n"
           "\t Read/write block size used for the device.\n");
    printf("\t -m, --mode\n"
           "\t The concurrency modes:\n"
           "\t\t 0 -> Staged\n"
           "\t\t 1 -> RSW\n"
           "\t\t 2 -> Piped\n");
    printf("\t -p, --read-buffer\n"
           "\t Read buffer size.\n");
    printf("\t -q, --write-buffer\n"
           "\t Write buffer size.\n");
    printf("\t -l, --merge-read-buffer\n"
           "\t Write buffer size.\n");
    printf("\t -r, --read-threads\n"
           "\t Number of reader threads. It must evenly divide read buffer.\n"
           "\t Per thread chunk size should evenly divide the block size specified.\n");
    printf("\t -s, --sort-threads\n"
           "\t Number of sorter threads.\n");
    printf("\t -w, --write-threads\n"
           "\t Number of writer threads. It must evenly divide write buffer.\n");
}

// Parse cmdline and init the config
void parseCmdline(int argc, char **argv)
{

    static struct option long_options[] = {
        {"input", required_argument, 0, 'i'},
        {"output_dir", required_argument, 0, 'o'},
        {"block", required_argument, 0, 'b'},
        {"mode", required_argument, 0, 'm'},
        {"read-buffer", required_argument, 0, 'p'},
        {"merge-read-buffer", required_argument, 0, 'l'},
        {"write-buffer", required_argument, 0, 'q'},
        {"read-threads", required_argument, 0, 'r'},
        {"sort-threads", required_argument, 0, 's'},
        {"write-threads", required_argument, 0, 'w'},
        {"help", no_argument, 0, 'h'},
        {0, 0, 0, 0}};
    // read the args
    int c, option_index = 0;
    while (1)
    {
        c = getopt_long(argc, argv, "i:o:b:m:p:l:q:r:s:w:h",
                        long_options, &option_index);

        // is end of the option
        if (c == -1)
            break;

        switch (c)
        {
        case 0:
            break;
        case 'i':
            conf.files.push_back(optarg);
            break;
        case 'o':
            conf.output_dir = optarg;
            break;
        // case 'k':
        //     conf.key_size = atoi(optarg);
        //     break;
        // case 'v':
        //     conf.value_size = atoi(optarg);
        //     break;
        case 'b':
            conf.block_size = atoi(optarg);
            break;
        case 'm':
            conf.mode = atoi(optarg);
            break;
        case 'p':
            conf.read_buffer_size = (size_t)MB((atoi(optarg)));
            // conf.read_buffer_size = 4080000000;

            // To sort 159999840000 gb (160GB)
            // /home/kanwu/vinay/pm-models/gensort-1.5/gensort 1599998400 /mnt/pmem/160gbUnsorted
            // conf.read_buffer_size = 11999988000;
            // To sort 200 gb (199999800000)
            // conf.read_buffer_size = 14999985000;
            break;
        case 'l':
            conf.merge_read_buffer_size = (size_t)MB((atoi(optarg)));
            // conf.merge_read_buffer_size = 10200000000;

            // To sort 159999840000 gb (160GB)
            // conf.merge_read_buffer_size = 11999988000;
            // To sort 200 gb (199999800000)
            // conf.merge_read_buffer_size = 14999985000;
            break;
        case 'q':
            conf.write_buffer_size = (size_t)MB((atoi(optarg)));
            // conf.write_buffer_size = 4096000000;
            // conf.write_buffer_size = 10240000000;

            // To sort 159999840000 gb (160GB) or 199999800000 gb (200 GB)
            // conf.write_buffer_size = 4999995000;

            break;
        case 'r':
            conf.read_thrds = atoi(optarg);
            break;
        case 's':
            conf.sort_thrds = atoi(optarg);
            break;
        case 'w':
            conf.write_thrds = atoi(optarg);
            break;
        case 'h':
            print_help_message(argv[0]);
            _exit(0);
        default:
            break;
        }
    }
    // fill the rest of the struct before returning
    conf.record_size = RECORD_SIZE;
    conf.idx_record_size = IDX_RECORD_SIZE;
    conf.block_size_2 = BLOCK_SIZE_MERGEWRITE;
    conf.recs_per_blk = conf.block_size_2 / conf.record_size;
    conf.idx_recs_per_blk = conf.block_size / conf.idx_record_size;
    conf.read_buff_blk_count = conf.read_buffer_size / conf.block_size;
    conf.write_buff_blk_count = conf.write_buffer_size / conf.block_size;
    conf.read_buff_rec_count = (conf.read_buff_blk_count * conf.block_size) / conf.record_size;
    conf.write_buff_rec_count = (conf.write_buff_blk_count * conf.block_size) / conf.record_size;

    std::cout << "record size: " << conf.record_size <<
            "B; recordPtr size: " << conf.idx_record_size << 
            "B; block size 1: " << conf.block_size << 
            "B; block size 2: " << conf.block_size_2 << 
            "B; run buffer size: " << conf.read_buffer_size / 1000 / 1000 <<
            "mB; merge read buffer size: " << conf.merge_read_buffer_size / 1000 / 1000 << 
            "mB; merge write buffer size: " << conf.write_buffer_size / 1000 / 1000 <<
            "mB; thread num (read/sort/write): " << conf.read_thrds << "/" << 
            conf.sort_thrds << "/" << conf.write_thrds << std::endl;
}