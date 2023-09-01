#!/bin/bash

ulimit -s unlimited

OMP_STACKSIZE=65536
export OMP_STACKSIZE

num=200000000
parNum=50000

batchList=(80 250 500 1000)
# 16B k-p
dramList0=(32 100 200 400) # 已经除以4, 200m
dramList1=(64 200 400 800)
dramList2=(128 400 800 1600)
dramList3=(192 600 1200 2400)
dramList4=(256 800 1600 3200)
dramList5=(320 1000 2000 4000)
# datasize
multipleList=(1 2 4 6 8 10)
sizeList=('200m' '400m' '800m' '1200m' '1600m' '2000m')
skewList=(1 2 3 25 50 200)

filename="stat/20230101_final"
date="20230101"
typeList=("uniform" "skew_1" "skew_2" "skew_3" "skew_25" "skew_50" "skew_200")


# uniform
for((round=0;round<6;round+=1))
do
    rm -rf /dcpmm/zhengy/pmem_unsorted
    numactl --physcpubind=16-31,48-63 --membind=1 ./sort $(($num*${multipleList[$round]})) $(($parNum*${multipleList[$round]})) 0 1000 32 16 4 0 >> "${filename}/${sizeList[$round]}_${typeList[0]}_${date}.out"
    sleep 30

    for batch in ${batchList[@]}
    do
        for((i=0;i<10;i+=1))
        do
            numactl --physcpubind=16-31,48-63 --membind=1 ./sort $(($num*${multipleList[$round]})) $(($parNum*${multipleList[$round]})) 0 $batch 32 16 4 7 >> "${filename}/${sizeList[$round]}_et_v7_${typeList[0]}_${date}.out"
            rm /dcpmm/zhengy/pmem_partition*
        done
        sleep 30
    done

    dramList="dramList$round[@]"
    for batch in "${!dramList}"
    do
        for((i=0;i<10;i+=1))
        do
            numactl --physcpubind=16-31,48-63 --membind=1 ./others/wiscsort_v3/release  -i /dcpmm/zhengy/pmem_unsorted -o /dcpmm/zhengy/ -b 4000 -m 0 -p $(($batch*4)) -l $(($batch*3)) -q $(($batch*1)) -r 20 -s 20 -w 5 >> "${filename}/${sizeList[$round]}_wisc_r3w1_${typeList[0]}_${date}.out"
            rm -rf /dcpmm/zhengy/1*
        done
        sleep 30
    done

    for batch in ${batchList[@]}
    do
        for((i=0;i<10;i+=1))
        do
            numactl --physcpubind=16-31,48-63 --membind=1 ./sort $(($num*${multipleList[$round]})) $(($parNum*${multipleList[$round]})) 0 $batch 32 16 4 104 >> "${filename}/${sizeList[$round]}_ips4o_${typeList[0]}_${date}.out"
            rm /dcpmm/zhengy/pmem_ip*
        done
        sleep 30
    done
done


# skew (400m)
for((round=0;round<6;round+=1))
do
    rm -rf /dcpmm/zhengy/pmem_unsorted
    numactl --physcpubind=16-31,48-63 --membind=1 ./sort $(($num*2)) $(($parNum*2)) ${skewList[$round]} 1000 32 16 4 0 >> "${filename}/${sizeList[1]}_${typeList[$(($round+1))]}_${date}.out"
    sleep 30

    for batch in ${batchList[@]}
    do
        for((i=0;i<10;i+=1))
        do
            numactl --physcpubind=16-31,48-63 --membind=1 ./sort $(($num*2)) $(($parNum*2)) ${skewList[$round]} $batch 32 16 4 7 >> "${filename}/${sizeList[1]}_et_v7_${typeList[$(($round+1))]}_${date}.out"
            rm /dcpmm/zhengy/pmem_partition*
        done
        sleep 30
    done

    for batch in ${dramList1[@]}
    do
        for((i=0;i<10;i+=1))
        do
            numactl --physcpubind=16-31,48-63 --membind=1 ./others/wiscsort_v3/release  -i /dcpmm/zhengy/pmem_unsorted -o /dcpmm/zhengy/ -b 4000 -m 0 -p $(($batch*4)) -l $(($batch*3)) -q $(($batch*1)) -r 20 -s 20 -w 5 >> "${filename}/${sizeList[1]}_wisc_r3w1_${typeList[$(($round+1))]}_${date}.out"
            rm -rf /dcpmm/zhengy/1*
        done
        sleep 30
    done

    for batch in ${batchList[@]}
    do
        for((i=0;i<10;i+=1))
        do
            numactl --physcpubind=16-31,48-63 --membind=1 ./sort $(($num*2)) $(($parNum*2)) ${skewList[$round]} $batch 32 16 4 104 >> "${filename}/${sizeList[1]}_ips4o_${typeList[$(($round+1))]}_${date}.out"
            rm /dcpmm/zhengy/pmem_ip*
        done
        sleep 30
    done
done


