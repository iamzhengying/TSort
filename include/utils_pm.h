#pragma once

#include <libpmem.h>
#include <stdlib.h>

template<typename T>
T* mmap_pmem_file(const char* path, size_t num_items) {
  size_t buffer_size = sizeof(T) * num_items;

  char* pmemaddr;
  int32_t is_pmem;
  size_t mapped_len;
  if (fopen(path, "r") != NULL) {
    pmemaddr = (char*)pmem_map_file(path, buffer_size,
        PMEM_FILE_CREATE, 0666, &mapped_len, &is_pmem);
  } else {
    pmemaddr = (char*)pmem_map_file(path, buffer_size,
        PMEM_FILE_CREATE|PMEM_FILE_EXCL, 0666, &mapped_len, &is_pmem);
  }

  if (pmemaddr == NULL) {
    perror("Map memory failed\n");
    exit(1);
  }

  if (is_pmem) {
    // printf("PM successfully allocated\n");

  } else {
    printf("error, not pmem\n");
    exit(1);
  }

  T* buf = reinterpret_cast<T*>(pmemaddr);
  return buf;
}


template<typename T>
void unmmap_pmem_file(T*& buf, size_t num_items) {
  pmem_unmap(buf, sizeof(T) * num_items);
}
