#pragma once

#include <atomic>

#define PAGE_SIZE (4ull * 1024ull)

typedef std::atomic_uint64_t PageState;
#define PAGE_STATE_MASK 255ull
#define PAGE_DIRTY_BIT 0b100000000ull
#define PAGE_MODIFIED_BIT 0b1000000000ull
#define PAGE_MODIFIED(state) ((state & PAGE_MODIFIED_BIT) != 0)
#define PAGE_STATE(state) (state & PAGE_STATE_MASK)
#define PAGE_VERSION_OFFSET 10
#define PAGE_VERSION(state) (state >> PAGE_VERSION_OFFSET)

#define PAGE_STATE_UNLOCKED 0
#define PAGE_STATE_LOCKED_SHARED_MIN 1
#define PAGE_STATE_LOCKED_SHARED_MAX 251
#define PAGE_STATE_FAULTED 252 // this is for temporary pages that are currently unused but have not been returned to the OS yet using 'madvise(MADV_DONTNEED)'
#define PAGE_STATE_LOCKED 253
#define PAGE_STATE_MARKED 254
#define PAGE_STATE_EVICTED 255