#!/bin/bash

ulimit -s unlimited

OMP_STACKSIZE=65536
export OMP_STACKSIZE

num=400000000
parNum=100000

batch=500
dram=400

filename="stat/bandwidth"
date="20230101"
type="uniform"
size='400m'


rm -rf /dcpmm/zhengy/pmem_unsorted
numactl --physcpubind=16-31,48-63 --membind=1 ./sort $num $parNum 0 1000 32 16 4 0 >> "${filename}/${size}_${type}_${date}.out"
sleep 30

numactl --physcpubind=16-31,48-63 --membind=1 ./sort $num $parNum 0 $batch 32 16 4 7 >> "${filename}/${size}_et_v7_${type}_${date}.out"
rm /dcpmm/zhengy/pmem_partition*
sleep 30

numactl --physcpubind=16-31,48-63 --membind=1 ./others/wiscsort_v3/release  -i /dcpmm/zhengy/pmem_unsorted -o /dcpmm/zhengy/ -b 4000 -m 0 -p $(($dram*4)) -l $(($dram*3)) -q $(($dram*1)) -r 20 -s 20 -w 5 >> "${filename}/${size}_wisc_r3w1_${type}_${date}.out"
rm -rf /dcpmm/zhengy/1*
 