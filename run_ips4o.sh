#!/bin/bash

ulimit -s unlimited

OMP_STACKSIZE=65536
export OMP_STACKSIZE

num=200000000
parNum=50000

batchList=(500)

filename="stat/20230101_ips4o"
date="20230101"
datasize="400m"
skewList=(0)
typeList=("uniform")
nameList=("ips4o_hybrid" "ips4o_onlyBAS" "ips4o_inPlace")
codeList=(104 108 109)

rm -rf /dcpmm/zhengy/pmem_unsorted
numactl --physcpubind=16-31,48-63 --membind=1 ./sort $(($num*2)) $(($parNum*2)) ${skewList[0]} 1000 32 16 4 0 >> "${filename}/${datasize}_${typeList[0]}_${date}.out"
sleep 30

for((idx=2;idx<3;idx+=1))
do
    for batch in ${batchList[@]}
    do
        for((i=0;i<10;i+=1))
        do
            if [ "$idx" -eq "2" ];then
                rm -rf /dcpmm/zhengy/pmem_unsorted
                numactl --physcpubind=16-31,48-63 --membind=1 ./sort $(($num*2)) $(($parNum*2)) ${skewList[0]} 1000 32 16 4 0 >> "${filename}/${datasize}_${typeList[0]}_${date}.out"
            fi
            numactl --physcpubind=16-31,48-63 --membind=1 ./sort $(($num*2)) $(($parNum*2)) ${skewList[0]} $batch 32 16 4 ${codeList[$(($idx))]} >> "${filename}/${datasize}_${nameList[$idx]}_${typeList[0]}_${date}.out"
            rm /dcpmm/zhengy/pmem_ips4o*
        done
        sleep 30
    done
done
