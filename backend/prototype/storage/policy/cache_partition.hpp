#pragma once

#include <atomic>
#include <iomanip>
#include <random>

#include "../../core/units.hpp"
#include "../../utils/crtp.hpp"
#include "../../utils/hashset.hpp"
#include "../vmcache.hpp"
#include "cache_trace.hpp"

/*
Base class for cache partitions. Derived classes provide different "partition-local" eviction policies and
are required to implement the following members:
    void fault(const PageId pid) - called when a given page has been faulted into memory
    void ref(const PageId pid, uint32_t worker_id) - called when a memory-resident marked page is being latched again
    void evict(uint32_t worker_id) - called when pages need to be evicted, should update 'physical_pages' and the respective stats variables appropriately; eviction policies may evict pages in batches, but this is not required
    void notifyDroppedImpl(const PageId pid) - called when VMCache itself has dropped a page and it does not reside in memory anymore
    size_t getNumLatchedPages(PageId max_pid) const - currently just used for statistics collection purposes
    static size_t getPerPageMemoryCost()
    static size_t getConstantMemoryCost(const size_t num_workers)
*/
template <class T>
class CachePartition : public CRTP<T, CachePartition> {
public:
    inline void prepareTempAllocation(size_t num_pages, uint32_t worker_id) {
        physical_temp_pages += num_pages;
        physical_pages += num_pages; // we are allocating new physical pages
        while (physical_pages > max_physical_pages) {
            this->actual().evict(worker_id);
        }
    }

    inline void handleFault(const PageId pid, bool scan, uint32_t worker_id) {
        physical_data_pages++;
        physical_pages++; // we are allocating a new physical page
        this->actual().fault(pid, scan);
        while (physical_pages > max_physical_pages) {
            this->actual().evict(worker_id);
        }
#ifdef COLLECT_CACHE_TRACES
        tracer.trace(CacheAction::Fault, pid, worker_id);
#endif
    }

    inline void notifyDropped(const PageId pid, __attribute__((unused)) uint32_t worker_id) {
        physical_data_pages--;
        this->actual().notifyDroppedImpl(pid);
        physical_pages--;
#ifdef COLLECT_CACHE_TRACES
        tracer.trace(CacheAction::Evict, pid, worker_id);
#endif
    }

    inline void notifyTempDropped(size_t num_pages) {
        physical_temp_pages -= num_pages;
        physical_pages -= num_pages;
    }

    void printMemoryUsage() const {
        std::cout << physical_pages.load() * PAGE_SIZE / 1024 / 1024 << " MiB / " << max_physical_pages * PAGE_SIZE / 1024 / 1024 << " MiB" << std::endl;
    }

    void printEvictionStats() const {
        std::cout << total_evicted_pages << " (" << std::setiosflags(std::ios::fixed) << std::setprecision(2) << total_evicted_pages * PAGE_SIZE / 1024.0 / 1024.0 / 1024.0 << " GiB)" << std::endl;
    }

    void printDirtyWriteStats() const {
        std::cout << total_dirty_pages_written << " (" << std::setiosflags(std::ios::fixed) << std::setprecision(2) << total_dirty_pages_written * PAGE_SIZE / 1024.0 / 1024.0 / 1024.0 << " GiB)" << std::endl;
    }

protected:
    VMCache& vmcache;
    const size_t max_physical_pages;
    const size_t num_workers;
    std::atomic_uint64_t physical_pages;
    // stats
    std::atomic_uint64_t total_evicted_pages;
    std::atomic_uint64_t total_dirty_pages_written;
    std::atomic_int64_t& physical_data_pages;
    std::atomic_int64_t& physical_temp_pages;
#ifdef COLLECT_CACHE_TRACES
    CacheTracer tracer;
#endif

    CachePartition(VMCache& vmcache, const size_t max_physical_pages, std::atomic_int64_t& physical_data_pages, std::atomic_int64_t& physical_temp_pages, const size_t num_workers)
    : vmcache(vmcache)
    , max_physical_pages(max_physical_pages)
    , num_workers(num_workers)
    , physical_pages(0)
    , total_evicted_pages(0)
    , total_dirty_pages_written(0)
    , physical_data_pages(physical_data_pages)
    , physical_temp_pages(physical_temp_pages)
#ifdef COLLECT_CACHE_TRACES
    , tracer(num_workers)
#endif
    { }

    inline bool tryMark(const PageId pid, uint64_t& s) {
        return vmcache.page_states[pid].compare_exchange_strong(s, (s & ~PAGE_STATE_MASK) | PAGE_STATE_MARKED);
    }

    inline void flushDirty(const PageId pid) {
        vmcache.flushDirtyPage(pid);
    }

    inline void pageOut(const PageId pid) {
        madvise(vmcache.memory + pid * PAGE_SIZE, PAGE_SIZE, MADV_DONTNEED);
    }

    inline void markEvicted(const PageId pid, __attribute__((unused)) uint32_t worker_id) {
        vmcache.page_states[pid].store(((vmcache.page_states[pid].load() & ~PAGE_STATE_MASK) + (1ull << PAGE_VERSION_OFFSET)) | PAGE_STATE_EVICTED, std::memory_order_relaxed);
#ifdef COLLECT_CACHE_TRACES
        tracer.trace(CacheAction::Evict, pid, worker_id);
#endif
    }

    inline uint64_t loadState(const PageId pid) const {
        return vmcache.page_states[pid].load();
    }

    inline bool tryCAS(const PageId pid, uint64_t& s, uint64_t new_s) {
        return vmcache.page_states[pid].compare_exchange_strong(s, new_s);
    }
};

/*
Base class for cache partitions implementing eviction policies that use a hash set to track faulted pages
*/
template <class T>
class HashSetCachePartition : public CachePartition<T> {
public:
    HashSetCachePartition(VMCache& vmcache, const size_t max_physical_pages, std::atomic_int64_t& physical_data_pages, std::atomic_int64_t& physical_temp_pages, const size_t num_workers)
    : CachePartition<T>(vmcache, max_physical_pages, physical_data_pages, physical_temp_pages, num_workers)
    , cached_pages(max_physical_pages * 3 / 2) { }

    inline void fault(const PageId pid, bool) {
        cached_pages.insert(pid);
    }

    inline void ref(const PageId, bool, uint32_t) { }

    inline void evict(uint32_t worker_id) {
        // evict some pages
        const size_t EVICTION_BATCH_SIZE = 64; // NOTE: Do not increase this above 64! dirty_pages & locked_pages will break otherwise!
        PageId eviction_candidates[EVICTION_BATCH_SIZE];
        uint64_t dirty_pages = 0;
        // get eviction candidates
        size_t num_eviction_candidates = this->actual().getEvictionCandidates(EVICTION_BATCH_SIZE, eviction_candidates, dirty_pages, worker_id);
        if (num_eviction_candidates == 0)
            return;
        // write out dirty pages
        for (size_t i = 0; i < num_eviction_candidates; i++) {
            PageId pid = eviction_candidates[i];
            if ((dirty_pages >> i) & 1ull) {
                this->flushDirty(pid);
                this->total_dirty_pages_written++;
            }
        }
        // obtain exclusive locks
        uint64_t locked_pages = 0;
        for (size_t i = 0; i < num_eviction_candidates; i++) {
            PageId pid = eviction_candidates[i];
            uint64_t s = this->loadState(pid);
            uint64_t new_s = (s & ~PAGE_STATE_MASK) | PAGE_STATE_LOCKED;
            if ((dirty_pages >> i) & 1ull) { // lock upgrade from shared to exclusive
                if (PAGE_STATE(s) == PAGE_STATE_LOCKED_SHARED_MIN && this->tryCAS(pid, s, new_s)) {
                    locked_pages |= 1ull << i;
                } else {
                    this->vmcache.unfixShared(pid);
                }
            } else { // lock exclusively
                if ((PAGE_STATE(s) == PAGE_STATE_MARKED || PAGE_STATE(s) == PAGE_STATE_UNLOCKED || PAGE_STATE(s) == PAGE_STATE_FAULTED) && this->tryCAS(pid, s, new_s)) {
                    locked_pages |= 1ull << i;
                }
            }
        }
        // remove locked pages from page table & remove locked pages from eviction hash table, unlock
        size_t evicted_data_pages = 0;
        for (size_t i = 0; i < num_eviction_candidates; i++) {
            if ((locked_pages >> i) & 1ull) {
                const PageId pid = eviction_candidates[i];
                this->pageOut(pid);
                cached_pages.erase(pid);
                this->markEvicted(pid, worker_id);
                evicted_data_pages++;
            }
        }
        this->physical_pages -= evicted_data_pages;
        this->total_evicted_pages += evicted_data_pages;
        this->physical_data_pages -= evicted_data_pages;
    }

    inline void notifyDroppedImpl(const PageId pid) {
        cached_pages.erase(pid);
    }

    size_t getNumLatchedPages(PageId max_pid) const {
        size_t result = 0;
        for (size_t i = 0; i < cached_pages.bucketCount(); i++) {
            PageId pid = cached_pages.getBucket(i);
            if (pid == HashSet<PageId>::tombstone_bucket || pid == HashSet<PageId>::empty_bucket || pid > max_pid)
                continue;
            const uint8_t state = PAGE_STATE(this->loadState(pid));
            if ((state >= PAGE_STATE_LOCKED_SHARED_MIN && state <= PAGE_STATE_LOCKED_SHARED_MAX) || state == PAGE_STATE_LOCKED)
                result++;
        }
        return result;
    }

    static size_t getPerPageMemoryCost() {
        return sizeof(PageId) * 3 / 2;
    }

protected:
    HashSet<PageId> cached_pages;
};

/*
This implements the batched clock eviction policy described in "Virtual-Memory Assisted Buffer Management" (Leis et al.)
*/
class alignas(64) ClockEvictionCachePartition : public HashSetCachePartition<ClockEvictionCachePartition> {
public:
    ClockEvictionCachePartition(VMCache& vmcache, const size_t max_physical_pages, std::atomic_int64_t& physical_data_pages, std::atomic_int64_t& physical_temp_pages, const size_t num_workers)
    : HashSetCachePartition<ClockEvictionCachePartition>(vmcache, max_physical_pages, physical_data_pages, physical_temp_pages, num_workers)
    , clock(0) { }

    inline size_t getEvictionCandidates(const size_t batch_size, PageId* eviction_candidates, uint64_t& dirty_pages, uint32_t) {
        size_t num_eviction_candidates = 0;
        const size_t begin_clock = clock.load();
        while (num_eviction_candidates < batch_size) {
            const size_t clock_step = batch_size - num_eviction_candidates;
            size_t current_clock = clock.load();
            const size_t next_clock = (current_clock + clock_step) % cached_pages.bucketCount();
            if (!clock.compare_exchange_weak(current_clock, next_clock))
                continue;
            for (size_t i = current_clock; i != next_clock; i = (i + 1) % cached_pages.bucketCount()) {
                if (i == (begin_clock == 0 ? cached_pages.bucketCount() - 1 : begin_clock - 1))
                    return num_eviction_candidates;
                PageId pid = cached_pages.getBucket(i);
                if (pid == HashSet<PageId>::tombstone_bucket || pid == HashSet<PageId>::empty_bucket)
                    continue;

                uint64_t s = loadState(pid);
                if (num_eviction_candidates < batch_size && (PAGE_STATE(s) == PAGE_STATE_MARKED || PAGE_STATE(s) == PAGE_STATE_FAULTED)) {
                    if ((s & PAGE_DIRTY_MASK) > 0) {
                        if (tryCAS(pid, s, (s & ~PAGE_STATE_MASK) | PAGE_STATE_LOCKED_SHARED_MIN)) {
                            dirty_pages |= 1ull << num_eviction_candidates;
                            eviction_candidates[num_eviction_candidates++] = pid;
                        }
                    } else {
                        eviction_candidates[num_eviction_candidates++] = pid;
                    }
                }
                // mark pages that are currently unlocked for eviction in the next round
                if (PAGE_STATE(s) == PAGE_STATE_UNLOCKED)
                    tryMark(pid, s);
            }
        }
        return num_eviction_candidates;
    }

    static size_t getConstantMemoryCost(const size_t) {
        return sizeof(ClockEvictionCachePartition);
    }

private:
    std::atomic_uint64_t clock;
};

/*
This implements an eviction policy that selects pages for eviction randomly
*/
class alignas(64) RandomEvictionCachePartition : public HashSetCachePartition<RandomEvictionCachePartition> {
public:
    RandomEvictionCachePartition(VMCache& vmcache, const size_t max_physical_pages, std::atomic_int64_t& physical_data_pages, std::atomic_int64_t& physical_temp_pages, const size_t num_workers)
    : HashSetCachePartition<RandomEvictionCachePartition>(vmcache, max_physical_pages, physical_data_pages, physical_temp_pages, num_workers) {
        generators.reserve(num_workers);
        distributions.reserve(num_workers);
        for (size_t i = 0; i < num_workers; i++) {
            generators.emplace_back(i);
            distributions.emplace_back(0, cached_pages.bucketCount() - 1);
        }
    }

    inline size_t getEvictionCandidates(const size_t batch_size, PageId* eviction_candidates, uint64_t& dirty_pages, uint32_t worker_id) {
        size_t num_eviction_candidates = 0;
        while (num_eviction_candidates < batch_size) {
            size_t i = distributions[worker_id](generators[worker_id]); // select a random bucket from 'cached_pages'
            PageId pid = cached_pages.getBucket(i);
            if (pid == decltype(cached_pages)::tombstone_bucket || pid == decltype(cached_pages)::empty_bucket)
                continue;

            uint64_t s = loadState(pid);
            if (PAGE_STATE(s) == PAGE_STATE_MARKED || PAGE_STATE(s) == PAGE_STATE_FAULTED || PAGE_STATE(s) == PAGE_STATE_UNLOCKED) {
                if ((s & PAGE_DIRTY_MASK) > 0) {
                    if (tryCAS(pid, s, (s & ~PAGE_STATE_MASK) | PAGE_STATE_LOCKED_SHARED_MIN)) {
                        dirty_pages |= 1ull << num_eviction_candidates;
                        eviction_candidates[num_eviction_candidates++] = pid;
                    }
                } else {
                    eviction_candidates[num_eviction_candidates++] = pid;
                }
            }
        }
        return num_eviction_candidates;
    }

    static size_t getConstantMemoryCost(const size_t num_workers) {
        return sizeof(RandomEvictionCachePartition) + num_workers * (sizeof(std::mt19937) + sizeof(std::uniform_int_distribution<size_t>));
    }

private:
    std::vector<std::mt19937> generators;
    std::vector<std::uniform_int_distribution<size_t>> distributions;
};

/*
This implements an MRU eviction policy
*/
class alignas(64) MRUEvictionCachePartition : public HashSetCachePartition<MRUEvictionCachePartition> {
public:
    MRUEvictionCachePartition(VMCache& vmcache, const size_t max_physical_pages, std::atomic_int64_t& physical_data_pages, std::atomic_int64_t& physical_temp_pages, const size_t num_workers)
    : HashSetCachePartition<MRUEvictionCachePartition>(vmcache, max_physical_pages, physical_data_pages, physical_temp_pages, num_workers)
    , clock(0)
    , mru_size(getMRUSize(num_workers))
    , mru_tail(0)
    , mru_head(0) {
        mru = new PageId[mru_size];
    }

    ~MRUEvictionCachePartition() {
        delete[] mru;
    }

    MRUEvictionCachePartition(const MRUEvictionCachePartition& other) = delete;
    MRUEvictionCachePartition& operator=(const MRUEvictionCachePartition& other) = delete;

    inline void fault(const PageId pid, bool scan) {
        HashSetCachePartition<MRUEvictionCachePartition>::fault(pid, scan);
        if (scan)
            appendMRU(pid);
    }

    inline void ref(const PageId pid, bool scan, uint32_t) {
        if (scan)
            appendMRU(pid);
    }

    inline size_t getEvictionCandidates(const size_t batch_size, PageId* eviction_candidates, uint64_t& dirty_pages, uint32_t) {
        if (!mru_mutex.try_lock())
            return 0;
        size_t num_eviction_candidates = 0;
        while (num_eviction_candidates != batch_size && mru_head != mru_tail) {
            PageId pid = mru[mru_head];
            if (mru_head == 0)
                mru_head = mru_size - 1;
            else
                mru_head--;
            uint64_t s = loadState(pid);
            if (PAGE_STATE(s) == PAGE_STATE_MARKED || PAGE_STATE(s) == PAGE_STATE_FAULTED || PAGE_STATE(s) == PAGE_STATE_UNLOCKED) {
                for (size_t j = 0; j < num_eviction_candidates; j++) {
                    if (eviction_candidates[j] == pid)
                        continue;
                }
                if ((s & PAGE_DIRTY_MASK) > 0) {
                    if (tryCAS(pid, s, (s & ~PAGE_STATE_MASK) | PAGE_STATE_LOCKED_SHARED_MIN)) {
                        dirty_pages |= 1ull << num_eviction_candidates;
                        eviction_candidates[num_eviction_candidates++] = pid;
                    }
                } else {
                    eviction_candidates[num_eviction_candidates++] = pid;
                }
            }
        }
        mru_mutex.unlock();
        const size_t begin_clock = clock.load();
        while (num_eviction_candidates != batch_size) {
            const size_t clock_step = batch_size - num_eviction_candidates;
            // if the MRU list is empty, fall back to clock eviction
            size_t current_clock = clock.load();
            const size_t next_clock = (current_clock + clock_step) % cached_pages.bucketCount();
            if (!clock.compare_exchange_weak(current_clock, next_clock))
                continue;
            for (size_t i = current_clock; i != next_clock; i = (i + 1) % cached_pages.bucketCount()) {
                if (i == (begin_clock == 0 ? cached_pages.bucketCount() - 1 : begin_clock - 1)) {
                    return num_eviction_candidates;
                }
                PageId pid = cached_pages.getBucket(i);
                if (pid == HashSet<PageId>::tombstone_bucket || pid == HashSet<PageId>::empty_bucket)
                    continue;

                uint64_t s = loadState(pid);
                if (PAGE_STATE(s) == PAGE_STATE_MARKED || PAGE_STATE(s) == PAGE_STATE_FAULTED) {
                    if ((s & PAGE_DIRTY_MASK) > 0) {
                        if (tryCAS(pid, s, (s & ~PAGE_STATE_MASK) | PAGE_STATE_LOCKED_SHARED_MIN)) {
                            dirty_pages |= 1ull << num_eviction_candidates;
                            eviction_candidates[num_eviction_candidates++] = pid;
                        }
                    } else {
                        eviction_candidates[num_eviction_candidates++] = pid;
                    }
                }
                // mark pages that are currently unlocked for eviction in the next round
                if (PAGE_STATE(s) == PAGE_STATE_UNLOCKED)
                    tryMark(pid, s);
            }
        }
        return num_eviction_candidates;
    }

    static size_t getConstantMemoryCost(const size_t num_workers) {
        return sizeof(MRUEvictionCachePartition) + sizeof(PageId) * getMRUSize(num_workers);
    }

private:
    inline void appendMRU(PageId pid) {
        std::lock_guard<std::mutex> guard(mru_mutex);
        if (mru_tail == mru_head + 1) {
            mru_tail = (mru_tail + 1) % mru_size;
        }
        mru[mru_head] = pid;
        mru_head = (mru_head + 1) % mru_size;
    }

    static size_t getMRUSize(const size_t num_workers) { return 128 * num_workers; }

    std::atomic_uint64_t clock;
    const size_t mru_size;
    PageId* mru;
    uint32_t mru_tail;
    uint32_t mru_head;
    std::mutex mru_mutex;
};


template <template <class T> class Strategy, typename... Arguments>
std::unique_ptr<PartitioningStrategy> createPartitioningStrategy(const std::string& eviction_policy, Arguments... args) {
    if (eviction_policy == "clock") {
        return std::make_unique<Strategy<ClockEvictionCachePartition>>(args...);
    } else if (eviction_policy == "random") {
        return std::make_unique<Strategy<RandomEvictionCachePartition>>(args...);
    } else if (eviction_policy == "mru") {
        return std::make_unique<Strategy<MRUEvictionCachePartition>>(args...);
    }
    return nullptr;
}

#define INSTANTIATE_PARTITIONING_STRATEGY(strategy) \
template class strategy<ClockEvictionCachePartition>; \
template class strategy<RandomEvictionCachePartition>; \
template class strategy<MRUEvictionCachePartition>;
