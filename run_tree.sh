#!/bin/bash

ulimit -s unlimited

OMP_STACKSIZE=65536
export OMP_STACKSIZE

num=200000000
parNum=50000

batchList=(80 500)

filename="stat/20230101_tree"
date="20230101"
datasize="400m"
skewList=(0 2 50)
typeList=("uniform" "skew_2" "skew_50")
nameList_1=("v1_BHMS" "v2_BBMS" "v3_NHMA" "v4_DHMS" "v5_DHMA" "v6_BGMS" "v7_BHMA" "v8_DBMS" "v9_DBMA" "v10_BBMA" "v7_BHSA")
nameList_2=("v1_BHmS" "v2_BBmS" "v4_DHmS" "v5_DHmA" "v7_BHmA" "v8_DBmS" "v9_DBmA" "v10_BBmA")
codeList=(1 2 4 5 7 8 9 10)

for((round=0;round<3;round+=1))
do
    rm -rf /dcpmm/zhengy/pmem_unsorted
    numactl --physcpubind=16-31,48-63 --membind=1 ./sort $(($num*2)) $(($parNum*2)) ${skewList[$(($round))]} 1000 32 16 4 0 >> "${filename}/${datasize}_${typeList[$(($round))]}_${date}.out"
    sleep 30

    for((idx=0;idx<10;idx+=1))
    do
        if [ "$idx" -ne "5" ];then
            for batch in ${batchList[@]}
            do
                for((i=0;i<10;i+=1))
                do
                    numactl --physcpubind=16-31,48-63 --membind=1 ./sort $(($num*2)) $(($parNum*2)) ${skewList[$(($round))]} $batch 32 16 4 $(($idx+1)) >> "${filename}/${datasize}_${nameList_1[$idx]}_${typeList[$(($round))]}_${date}.out"
                    rm /dcpmm/zhengy/pmem_partition*
                done
                sleep 30
            done
        fi
    done

    for batch in ${batchList[@]}
    do
        for((i=0;i<10;i+=1))
        do
            numactl --physcpubind=16-31,48-63 --membind=1 ./sort $(($num*2)) $(($parNum*2)) ${skewList[$(($round))]} $batch 1 1 1 7 >> "${filename}/${datasize}_${nameList_1[10]}_${typeList[$(($round))]}_${date}.out"
            rm /dcpmm/zhengy/pmem_partition*
        done
        sleep 30
    done

    for((idx=0;idx<8;idx+=1))
    do
        for batch in ${batchList[@]}
        do
            for((i=0;i<10;i+=1))
            do
                numactl --physcpubind=16-31,48-63 --membind=1 ./sort $(($num*2)) $(($parNum*2)) ${skewList[$(($round))]} $batch 32 32 32 ${codeList[$(($idx))]} >> "${filename}/${datasize}_${nameList_2[$idx]}_${typeList[$(($round))]}_${date}.out"
                rm /dcpmm/zhengy/pmem_partition*
            done
            sleep 30
        done
    done
done

