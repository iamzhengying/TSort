# Sorting on Byte-Addressable Storage: The Resurgence of Tree Structure

### 1. Requirements
#### Hardware:
- A two-socket machine with 3rd generation (or above) scalable processors
- Intel Optane DIMMs >= 600GB
- DRAM >= 16GB

#### Software:
- Linux Kernel >= 5.4.0-163
- OS version >= Ubuntu 20.04.5
- CMake >= 3.16.3
- gcc >= 10.5.0

### 2. Quick Start

#### 2.1 Clone the project
```
    $ git clone https://github.com/iamzhengying/sort.git
```

#### 2.2 Install dependencies
```
    $ bash install.sh
```

#### 2.3 Compile
```
    $ make
```

#### 2.4 Run demo code
```
    $ bash run_demo.sh
```

#### 2.5 Rerun experiments
Note 1: You may want to update the paths of BAS mounting points in `utils.h` and scripts before running experiemnts.

Note 2: Remember to (1) uncomment `#define bandwidth true` in files `./src/et_v7.cpp`, `./others/wiscsort_v3/data_manager.cc`, and `./others/wiscsort_v3/ext_sort.cc` and (2) recompile files using `./make` and `./others/wiscsort_v3/make release` before rerun the bandwidth experiment.
```
    Experiments in Section 2:
    $ bash run_ips4o.sh

    Experiments in Section 4:
    $ bash run_tree.sh

    Experiments in Section 5:
    $ bash run_pre.sh
    $ bash run_final.sh
    $ bash run_bandwidth.sh
    $ bash run_gensort.sh
```

#### 2.6 Reproduce figures
Please check all `.ipynb` files under `./stat/`

