#!/bin/bash

ulimit -s unlimited

OMP_STACKSIZE=65536
export OMP_STACKSIZE

num=400000000
parNum=100000

batchList=(80 250 500 1000)

# 100B (24B k-p)
dramList2=(96 300 600 1200)     # divided by 4, for 400m

# datasize
sizeList='400m'
filename="stat/20230101_gensort"
date="20230101"
typeList="uniform"

rm /dcpmm/zhengy/pmem_unsorted_gen
numactl --physcpubind=16-31,48-63 --membind=1 ./gensort-1.5/gensort -a $num /dcpmm/zhengy/pmem_unsorted_gen

for batch in ${batchList[@]}
do
    for((i=0;i<10;i+=1))
    do
        numactl --physcpubind=16-31,48-63 --membind=1 ./genSort/sort_gensort /dcpmm/zhengy/pmem_unsorted_gen $num $parNum 0 $batch 32 16 4 1 >> "${filename}/${sizeList}_et_v7_gensort_${typeList}_${date}.out"
        rm /dcpmm/zhengy/pmem_partition*
    done
    sleep 30

    for((i=0;i<10;i+=1))
    do
        numactl --physcpubind=16-31,48-63 --membind=1 ./genSort/sort_gensort /dcpmm/zhengy/pmem_unsorted_gen $num $parNum 0 $batch 32 16 4 2 >> "${filename}/${sizeList}_ips4o_gensort_2_${typeList}_${date}.out"
        rm /dcpmm/zhengy/pmem_i*
    done
    sleep 30
done

for batch in ${dramList2[@]}
do
    for((i=0;i<10;i+=1))
    do
        numactl --physcpubind=16-31,48-63 --membind=1 ./genSort/wiscsort_v3_gensort/release  -i /dcpmm/zhengy/pmem_unsorted_gen -o /dcpmm/zhengy/ -b 3840 -m 0 -p $(($batch*4)) -l $(($batch*3)) -q $(($batch*1)) -r 20 -s 20 -w 5 >> "${filename}/${sizeList}_wisc_r3w1_gensort_${typeList}_${date}.out"
        rm -rf /dcpmm/zhengy/1*
    done
    sleep 30
done
