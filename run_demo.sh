#!/bin/bash

ulimit -s unlimited

OMP_STACKSIZE=65536
export OMP_STACKSIZE

num=400000000
parNum=100000

batchList=(80 200 500 1000)

rm -rf /dcpmm/zhengy/*
numactl --physcpubind=16-31,48-63 --membind=1 ./sort $num $parNum 0 1000 32 16 4 0

for batch in ${batchList[@]}
do
    numactl --physcpubind=16-31,48-63 --membind=1 ./sort $num $parNum 0 $batch 32 16 4 7
    rm /dcpmm/zhengy/pmem_partition*
done

