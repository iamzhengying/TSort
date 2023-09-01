#!/bin/bash

ulimit -s unlimited

OMP_STACKSIZE=65536
export OMP_STACKSIZE

num=40000000    # 40m
parNum=10000

batch=500
# 16B k-p
dram=40     # 40m
skewList=(0 2 50)

filename="stat/latency/20230101_pre"
date="20230101"
size='40m'


for((idx=0;idx<3;idx+=1))
do
    rm -rf /dcpmm/zhengy/pmem_unsorted
    numactl --physcpubind=16-31,48-63 --membind=1 ./sort $num $parNum ${skewList[$idx]} 1000 32 16 4 0 >> "${filename}/${size}_${date}.out"
    sleep 30

    for((i=0;i<10;i+=1))
    do
        numactl --physcpubind=16-31,48-63 --membind=1 ./sort $num $parNum ${skewList[$idx]} $$batch 32 16 4 7 >> "${filename}/${size}_et_v7_${date}.out"
        rm /dcpmm/zhengy/pmem_partition*
    done
    sleep 30

    for((i=0;i<10;i+=1))
    do
        numactl --physcpubind=16-31,48-63 --membind=1 ./sort $num $parNum ${skewList[$idx]} $batch 32 16 4 104 >> "${filename}/${size}_ips4o_${date}.out"
        rm /dcpmm/zhengy/pmem_i*
    done
    sleep 30

    for((i=0;i<10;i+=1))
    do
        numactl --physcpubind=16-31,48-63 --membind=1 ./sort $num $parNum ${skewList[$idx]} $batch 32 16 4 105 >> "${filename}/${size}_external_${date}.out"
        rm /dcpmm/zhengy/pmem_e*
    done
    sleep 30

    for((i=0;i<10;i+=1))
    do
        numactl --physcpubind=16-31,48-63 --membind=1 ./others/wiscsort_v3/release  -i /dcpmm/zhengy/pmem_unsorted -o /dcpmm/zhengy/ -b 4000 -m 0 -p $(($dram*4)) -l $(($dram*3)) -q $(($dram*1)) -r 20 -s 20 -w 5 >> "${filename}/${size}_wisc_r3w1_${date}.out"
        rm -rf /dcpmm/zhengy/1*
    done
    sleep 30
done



idx=0
rm -rf /dcpmm/zhengy/pmem_unsorted
numactl --physcpubind=16-31,48-63 --membind=1 ./sort $num $parNum ${skewList[$idx]} 1000 32 16 4 0 >> "${filename}/${size}_${date}.out"
sleep 30

for((i=0;i<10;i+=1))
do
    numactl --physcpubind=16-31,48-63 --membind=1 ./sort $num $parNum ${skewList[$idx]} $batch 32 16 4 102 >> "${filename}/${size}_quick_${date}.out"
    rm /dcpmm/zhengy/pmem_q*
done
sleep 30




