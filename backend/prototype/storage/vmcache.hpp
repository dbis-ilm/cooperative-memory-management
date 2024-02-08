#pragma once

#include <atomic>
#include <cassert>
#include <cstddef>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <stdint.h>
#include <sys/mman.h>
#include <unistd.h>
#include <vector>

#include "../core/units.hpp"
#include "../utils/errno.hpp"
#include "policy/partitioning_strategy.hpp"
#include "page.hpp"
#include "linux/exmap.h"

// threshold for considering temporary allocations as "large" (and thereby use the eviction target mechanism if enabled)
//  (in pages)
#define LARGE_ALLOCATION_THRESHOLD (4ul * 1024ul * 1024ul / PAGE_SIZE)

struct alignas(64) VMCacheStats {
    std::atomic_uint64_t total_accessed_pages;
    std::atomic_uint64_t total_faulted_pages;
    uint64_t pad[4];
};

int exmapAction(int exmapfd, exmap_opcode op, uint16_t len, uint32_t worker_id);

class alignas(64) VMCache {
    template<class T>
    friend struct OptimisticGuard;

public:
    VMCache(uint64_t max_size, uint64_t virtual_pages, const std::string& path, bool sandbox, bool no_dirty_writeback, bool flush_asynchronously, bool use_eviction_target, std::unique_ptr<PartitioningStrategy>&& partitioning_strategy, bool use_exmap, bool stats_on_shutdown, const size_t num_workers);
    ~VMCache();

    VMCache(const VMCache& other) = delete;
    VMCache(VMCache&& other) = delete;
    VMCache& operator=(const VMCache& other) = delete;
    VMCache& operator=(VMCache&& other) = delete;

    PageId allocatePage();
    inline bool isEmpty() const { return num_allocated_pages == 0; }
    char* allocateTemporaryPage(uint32_t worker_id); // allocates a page for temporary use and latches it exclusively
    char* allocateTemporaryHugePage(const size_t num_pages, uint32_t worker_id);
    void dropTemporaryPage(char* page, uint32_t worker_id);
    void dropTemporaryHugePage(char* page, const size_t num_pages, uint32_t worker_id);

    size_t getMaxPhysicalPages() const { return max_physical_pages; }
    const PartitioningStrategy& getPartitions() const { return *partitioning_strategy; }
    size_t getNumLatchedDataPages() const;
    size_t getNumTemporaryPagesInUse() const { return num_temporary_pages_in_use.load(); }

    void setAllocationLatencyLogCallback(std::shared_ptr<std::function<void(size_t)>> callback) { log_allocation_latency = callback; }

    PageState& getPageState(const PageId pid) {
        return page_states[pid];
    }

    inline char* toPointer(PageId pid) const {
        return memory + pid * PAGE_SIZE;
    }

    inline char* fixExclusive(PageId pid, uint32_t worker_id) {
        stats[worker_id].total_accessed_pages++;
        checkPid(pid);
        uint64_t s = page_states[pid].load();
        while (true) {
            const uint64_t state = PAGE_STATE(s);
            const uint64_t new_s = (s & ~PAGE_STATE_MASK) | PAGE_STATE_LOCKED;
            if (state == PAGE_STATE_EVICTED) {
                if (page_states[pid].compare_exchange_weak(s, new_s)) {
                    fault(pid, PAGE_MODIFIED(s), false, worker_id); // we assume that a scan will never acquire an exclusive latch
                    return toPointer(pid);
                }
            } else if (state == PAGE_STATE_MARKED || state == PAGE_STATE_UNLOCKED) {
                if (page_states[pid].compare_exchange_weak(s, new_s)) {
                    if (state == PAGE_STATE_MARKED)
                        ref(pid, false, worker_id); // we assume that a scan will never acquire an exclusive latch
                    return toPointer(pid);
                }
            } else {
                s = page_states[pid].load();
            }
        }
    }

    inline void unfixExclusive(PageId pid) {
        checkPid(pid);
        const uint64_t s = page_states[pid].load();
        const uint64_t dirty_bit = dirty_writeback ? PAGE_DIRTY_BIT : PAGE_MODIFIED_BIT;
        page_states[pid].store(((s & ~PAGE_STATE_MASK) + (1ull << PAGE_VERSION_OFFSET)) | PAGE_STATE_UNLOCKED | dirty_bit, std::memory_order_relaxed);
        // update statistics
        if (dirty_writeback && (s & PAGE_DIRTY_BIT) == 0) // page was not already dirty
            num_dirty_pages++;
    }

    inline char* fixShared(PageId pid, uint32_t worker_id, bool scan = false) {
        stats[worker_id].total_accessed_pages++;
        checkPid(pid);
        uint64_t s = page_states[pid].load();
        while (true) {
            const uint64_t state = PAGE_STATE(s);
            if (state == PAGE_STATE_EVICTED) {
                // latch the page in exclusive mode first for reading the data from disk
                const uint64_t new_s = (s & ~PAGE_STATE_MASK) | PAGE_STATE_LOCKED;
                if (page_states[pid].compare_exchange_weak(s, new_s)) {
                    fault(pid, PAGE_MODIFIED(s), scan, worker_id);
                    // downgrade to shared latch
                    page_states[pid].store((new_s & ~PAGE_STATE_MASK) | PAGE_STATE_LOCKED_SHARED_MIN);
                    return toPointer(pid);
                }
            } else if (state == PAGE_STATE_MARKED || state == PAGE_STATE_UNLOCKED) {
                const uint64_t new_s = (s & ~PAGE_STATE_MASK) | PAGE_STATE_LOCKED_SHARED_MIN;
                if (page_states[pid].compare_exchange_weak(s, new_s)) {
                    if (state == PAGE_STATE_MARKED)
                        ref(pid, scan, worker_id);
                    return toPointer(pid);
                }
            } else if (state >= PAGE_STATE_LOCKED_SHARED_MIN && state < PAGE_STATE_LOCKED_SHARED_MAX) {
                const uint64_t new_s = (s & ~PAGE_STATE_MASK) | (state + 1);
                if (page_states[pid].compare_exchange_weak(s, new_s)) {
                    if (state == PAGE_STATE_MARKED)
                        ref(pid, scan, worker_id);
                    return toPointer(pid);
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

    size_t getTotalAccessedPageCount() const {
        size_t result = 0;
        for (size_t i = 0; i < num_workers; ++i)
            result += stats[i].total_accessed_pages.load();
        return result;
    }
    size_t getTotalFaultedPageCount() const {
        size_t result = 0;
        for (size_t i = 0; i < num_workers; ++i)
            result += stats[i].total_faulted_pages.load();
        return result;
    }
    size_t getTotalEvictedPageCount() const { return partitioning_strategy->getTotalEvictedPageCount(); }
    size_t getTotalDirtyWritePageCount() const { return partitioning_strategy->getTotalDirtyWritePageCount(); }
    size_t getDirtyPageCount() const { return static_cast<size_t>(std::max(0l, num_dirty_pages.load())); }


    bool performIdleMaintenance(uint32_t worker_id) {
        return partitioning_strategy->performIdleMaintenance(worker_id);
    }
    bool isUsingAsyncFlushing() const { return flush_asynchronously; }
    bool isUsingEvictionTarget() const { return use_eviction_target; }

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

    inline void fault(const PageId pid, bool is_modified, bool scan, uint32_t worker_id) {
        partitioning_strategy->preFault(pid, scan, worker_id);
        // exmap allocation
        if (use_exmap && (dirty_writeback || !is_modified)) {
            exmap_interface[worker_id]->iov[0].page = pid;
            exmap_interface[worker_id]->iov[0].len = 1;
            while (exmapAction(exmap_fd, EXMAP_OP_ALLOC, 1, worker_id) < 0) {
                std::cerr << "fault errno: " << errno << " pid: " << pid << " worker_id: " << worker_id << std::endl;
            }
        }
        // read the page from disk
        if (!dirty_writeback && is_modified) {
            // if we are not writing back dirty pages (but keeping them in memory) and the page was already faulted previously, do a dummy read here to simulate the latency of a real page fault
            char DUMMY_READ_DEST[PAGE_SIZE];
            stats[worker_id].total_faulted_pages++;
            pread(fd, DUMMY_READ_DEST, PAGE_SIZE, 0);
            return;
        }
        const size_t offset = pid * PAGE_SIZE;
        int read_fd = is_modified ? shadow_fd : fd;
        if (lseek(read_fd, 0, SEEK_END) >= static_cast<off_t>(offset + PAGE_SIZE)) {
            if (pread(read_fd, toPointer(pid), PAGE_SIZE, offset) != PAGE_SIZE) {
                if (errno != 0) { // if errno == 0, the page simply did not exist in the database file yet, this is fine
                    std::cout << "[vmcache] " << "Error: Failed to read page (errno " << errno << ", " << errnoStr() << ")" << std::endl;
                    errno = 0;
                    // TODO: proper error handling
                }
            } else {
                stats[worker_id].total_faulted_pages++;
            }
        }
    }

    inline void ref(const PageId pid, bool scan, uint32_t worker_id) {
        partitioning_strategy->ref(pid, scan, worker_id);
    }

    void flushDirtyPage(const PageId pid);

    int fd;
    int exmap_fd;
    const bool use_exmap;
    const bool stats_on_shutdown;
    const uint64_t max_size;
    const uint64_t virtual_pages;
    const uint64_t max_physical_pages;
    std::vector<struct exmap_user_interface*> exmap_interface;
    // for page allocation
    std::atomic_uint64_t num_allocated_pages;
    // for eviction and statistics
    uint64_t p1[7]; // padding
    std::unique_ptr<PartitioningStrategy> partitioning_strategy;
    std::atomic_int64_t num_temporary_pages_in_use;
    std::atomic_int64_t peak_num_temporary_pages_in_use;
    std::atomic_int64_t num_dirty_pages;
    // anonymous memory mapping used for the buffer pool
    uint64_t p2[4]; // padding
    char* memory;
    PageState* const page_states;
    uint64_t p3[6]; // padding
    int shadow_fd;
    const bool sandbox;
    const bool dirty_writeback;
    const bool flush_asynchronously;
    const bool use_eviction_target;
    const std::string db_path;
    // stats per worker
    VMCacheStats* stats;
    const size_t num_workers;
    std::shared_ptr<std::function<void(size_t)>> log_allocation_latency; // note: using a shared_ptr here since using std::function directly makes VMCache a "non-standard-layout" class, which breaks the alignment checks below

    friend class VMCacheAlignmentChecker;
    template <class T> friend class CachePartition;
};

class VMCacheAlignmentChecker {
    static_assert(offsetof(VMCache, num_allocated_pages) == 64);
    static_assert(offsetof(VMCache, partitioning_strategy) == 128);
    static_assert(offsetof(VMCache, memory) == 192);
    static_assert(offsetof(VMCache, shadow_fd) == 256);
    static_assert(sizeof(VMCacheStats) == 64);
};