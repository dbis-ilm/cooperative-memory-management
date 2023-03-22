#pragma once

#include <atomic>
#include <cassert>
#include <cstddef>
#include <iostream>
#include <memory>
#include <mutex>
#include <stdint.h>
#include <sys/mman.h>
#include <unistd.h>

#include "../core/units.hpp"
#include "../utils/errno.hpp"
#include "policy/partitioning_strategy.hpp"
#include "page.hpp"

class alignas(64) VMCache {
public:
    VMCache(uint64_t max_size, uint64_t virtual_pages, int fd, std::unique_ptr<PartitioningStrategy>&& partitioning_strategy, bool stats_on_shutdown, const size_t num_workers);
    ~VMCache();

    VMCache(const VMCache& other) = delete;
    VMCache(VMCache&& other) = delete;
    VMCache& operator=(const VMCache& other) = delete;
    VMCache& operator=(VMCache&& other) = delete;

    PageId allocatePage();
    char* allocateTemporaryPage(uint32_t worker_id); // allocates a page for temporary use and latches it exclusively
    char* allocateTemporaryHugePage(const size_t num_pages, uint32_t worker_id);
    void dropTemporaryPage(char* page, uint32_t worker_id);
    void dropTemporaryHugePage(char* page, const size_t num_pages, uint32_t worker_id);

    size_t getMaxPhysicalPages() const { return max_physical_pages; }
    const PartitioningStrategy& getPartitions() const { return *partitioning_strategy; }
    size_t getNumLatchedDataPages() const;
    size_t getNumTemporaryPagesInUse() const { return num_temporary_pages_in_use.load(); }

    // for testing
    const PageState& getPageState(const PageId pid) const {
        return page_states[pid];
    }

    inline char* fixExclusive(PageId pid, uint32_t worker_id) {
        checkPid(pid);
        const uint64_t offset = pid * PAGE_SIZE;
        uint64_t s = page_states[pid].load();
        while (true) {
            const uint64_t state = PAGE_STATE(s);
            const uint64_t new_s = (s & ~PAGE_STATE_MASK) | PAGE_STATE_LOCKED;
            if (state == PAGE_STATE_EVICTED) {
                if (page_states[pid].compare_exchange_weak(s, new_s)) {
                    fault(pid, false, worker_id); // we assume that a scan will never acquire an exclusive latch
                    return memory + offset;
                }
            } else if (state == PAGE_STATE_MARKED || state == PAGE_STATE_UNLOCKED) {
                if (page_states[pid].compare_exchange_weak(s, new_s)) {
                    if (state == PAGE_STATE_MARKED)
                        ref(pid, false, worker_id); // we assume that a scan will never acquire an exclusive latch
                    return memory + offset;
                }
            } else {
                s = page_states[pid].load();
            }
        }
    }

    inline void unfixExclusive(PageId pid) {
        checkPid(pid);
        page_states[pid].store(((page_states[pid].load() & ~PAGE_STATE_MASK) + (1ull << PAGE_VERSION_OFFSET)) | PAGE_STATE_UNLOCKED | PAGE_DIRTY_BIT, std::memory_order_relaxed);
    }

    inline char* fixShared(PageId pid, uint32_t worker_id, bool scan = false) {
        checkPid(pid);
        const uint64_t offset = pid * PAGE_SIZE;
        uint64_t s = page_states[pid].load();
        while (true) {
            const uint64_t state = PAGE_STATE(s);
            if (state == PAGE_STATE_EVICTED) {
                // latch the page in exclusive mode first for reading the data from disk
                const uint64_t new_s = (s & ~PAGE_STATE_MASK) | PAGE_STATE_LOCKED;
                if (page_states[pid].compare_exchange_weak(s, new_s)) {
                    fault(pid, scan, worker_id);
                    // downgrade to shared latch
                    page_states[pid].store((new_s & ~PAGE_STATE_MASK) | PAGE_STATE_LOCKED_SHARED_MIN);
                    return memory + offset;
                }
            } else if (state == PAGE_STATE_MARKED || state == PAGE_STATE_UNLOCKED) {
                const uint64_t new_s = (s & ~PAGE_STATE_MASK) | PAGE_STATE_LOCKED_SHARED_MIN;
                if (page_states[pid].compare_exchange_weak(s, new_s)) {
                    if (state == PAGE_STATE_MARKED)
                        ref(pid, scan, worker_id);
                    return memory + offset;
                }
            } else if (state >= PAGE_STATE_LOCKED_SHARED_MIN && state < PAGE_STATE_LOCKED_SHARED_MAX) {
                const uint64_t new_s = (s & ~PAGE_STATE_MASK) | (state + 1);
                if (page_states[pid].compare_exchange_weak(s, new_s)) {
                    if (state == PAGE_STATE_MARKED)
                        ref(pid, scan, worker_id);
                    return memory + offset;
                }
            } else {
                s = page_states[pid].load();
            }
        }
    }

    inline void unfixShared(PageId pid) {
        checkPid(pid);
        uint64_t s = page_states[pid].load();
        while (true) {
            assert(PAGE_STATE(s) >= PAGE_STATE_LOCKED_SHARED_MIN && PAGE_STATE(s) <= PAGE_STATE_LOCKED_SHARED_MAX);
            if (page_states[pid].compare_exchange_weak(s, s - 1)) {
                return;
            }
        }
    }

    void printMemoryUsage() const;
    void evictAll(bool check_residency, uint32_t worker_id); // evicts all pages that are not currently locked

    size_t getTotalFaultedPageCount() const { return total_faulted_pages.load(); }

private:
#ifdef DEBUG
    inline void checkPid(const PageId pid) {
        if (pid > num_allocated_pages)
            throw std::runtime_error("Invalid PID");
    }
#else
    inline void checkPid(const PageId) { }
#endif

    inline void addToTemporaryPagesInUse(size_t num_pages) {
        int64_t n = num_pages;
        const int64_t temp_in_use = num_temporary_pages_in_use.fetch_add(n) + n;
        int64_t peak_temp_in_use = peak_num_temporary_pages_in_use.load();
        while (peak_temp_in_use < temp_in_use && !peak_num_temporary_pages_in_use.compare_exchange_weak(peak_temp_in_use, temp_in_use, std::memory_order_relaxed)) { }
    }

    inline void fault(const PageId pid, bool scan, uint32_t worker_id) {
        partitioning_strategy->preFault(pid, scan, worker_id);
        // read the page from disk
        const size_t offset = pid * PAGE_SIZE;
        if (lseek(fd, 0, SEEK_END) >= static_cast<off_t>(offset + PAGE_SIZE)) {
            if (pread(fd, memory + offset, PAGE_SIZE, offset) != PAGE_SIZE) {
                if (errno != 0) { // if errno == 0, the page simply did not exist in the database file yet, this is fine
                    std::cout << "[vmcache] " << "Error: Failed to read page (errno " << errno << ", " << errnoStr() << ")" << std::endl;
                    errno = 0;
                    // TODO: proper error handling
                }
            } else {
                total_faulted_pages++;
            }
        }
    }

    inline void ref(const PageId pid, bool scan, uint32_t worker_id) {
        partitioning_strategy->ref(pid, scan, worker_id);
    }

    void flushDirtyPage(const PageId pid);

    const int fd;
    const bool stats_on_shutdown;
    const uint64_t max_size;
    const uint64_t virtual_pages;
    const uint64_t max_physical_pages;
    uint64_t p0[4]; // padding
    // for page allocation
    std::atomic_uint64_t num_allocated_pages;
    // for eviction and statistics
    uint64_t p1[7]; // padding
    std::unique_ptr<PartitioningStrategy> partitioning_strategy;
    std::atomic_uint64_t total_faulted_pages;
    std::atomic_int64_t num_temporary_pages_in_use;
    std::atomic_int64_t peak_num_temporary_pages_in_use;
    // anonymous memory mapping used for the buffer pool
    uint64_t p2[4]; // padding
    char* const memory;
    PageState* const page_states;

    friend class VMCacheAlignmentChecker;
    template <class T> friend class CachePartition;
};

class VMCacheAlignmentChecker {
    static_assert(offsetof(VMCache, num_allocated_pages) == 64);
    static_assert(offsetof(VMCache, partitioning_strategy) == 128);
    static_assert(offsetof(VMCache, memory) == 192);
};