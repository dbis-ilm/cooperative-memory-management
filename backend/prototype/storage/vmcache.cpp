#include "vmcache.hpp"

#include <iomanip>
#include <iostream>
#include <stdexcept>
#include <sys/mman.h>

VMCache::VMCache(uint64_t max_size, uint64_t virtual_pages, int fd, std::unique_ptr<PartitioningStrategy>&& partitioning_strategy, bool stats_on_shutdown, const size_t num_workers)
    : fd(fd)
    , stats_on_shutdown(stats_on_shutdown)
    , max_size(max_size)
    , virtual_pages(virtual_pages)
    , max_physical_pages((max_size - sizeof(VMCache) - partitioning_strategy->getConstantMemoryCost(num_workers) - virtual_pages * sizeof(PageState)) / (PAGE_SIZE + partitioning_strategy->getPerPageMemoryCost())) // memory cost per physical page is the page size itself + eviction policy overhead
    , num_allocated_pages(0)
    , partitioning_strategy(std::move(partitioning_strategy))
    , total_faulted_pages(0)
    , num_temporary_pages_in_use(0)
    , peak_num_temporary_pages_in_use(0)
    , memory(reinterpret_cast<char*>(mmap(0, virtual_pages * PAGE_SIZE, PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_PRIVATE | MAP_NORESERVE, -1, 0)))
    , page_states(new PageState[virtual_pages])
{
    const size_t MB = 1000 * 1000;
    std::cout << "[vmcache] " << "Memory limit: " << max_size / MB << " MB" << std::endl;
    std::cout << "[vmcache] " << "Effective capacity: " << max_physical_pages * PAGE_SIZE / MB << " MB (" << max_physical_pages << " pages)" << std::endl;
    std::cout << "[vmcache] " << "Page state array uses " << virtual_pages * sizeof(PageState) / MB << " MB (" << virtual_pages << " entries)" << std::endl;
    const size_t ps_constant_cost = this->partitioning_strategy->getConstantMemoryCost(num_workers);
    const size_t ps_pp_cost = this->partitioning_strategy->getPerPageMemoryCost();
    std::cout << "[vmcache] " << "Partitioning strategy uses a constant " << ps_constant_cost / MB << " MB and " << ps_pp_cost << " B per page (" << (ps_constant_cost + ps_pp_cost * max_physical_pages) / MB << " MB total)" << std::endl;

    for (size_t i = 0; i < virtual_pages; i++)
        page_states[i] = PAGE_STATE_EVICTED;

    size_t file_size = lseek(fd, 0, SEEK_END);
    num_allocated_pages = file_size / PAGE_SIZE;
    madvise(memory, virtual_pages * PAGE_SIZE, MADV_DONTNEED | MADV_NOHUGEPAGE);
    this->partitioning_strategy->setVMCache(this, num_workers);
}

VMCache::~VMCache() {
    int64_t warnings_to_show = 5;
    size_t pages_written = 0;
    for (PageId pid = 0; pid < virtual_pages; pid++) {
        uint64_t s = page_states[pid].load();
        uint64_t state = PAGE_STATE(s);
        if (state != PAGE_STATE_UNLOCKED && state != PAGE_STATE_MARKED && state != PAGE_STATE_EVICTED && state != PAGE_STATE_FAULTED) {
            if (warnings_to_show > 0)
                std::cout << "[vmcache] " << "Warning: Detected latched buffer frame on shutdown (0x" << std::hex <<    static_cast<uint16_t>(state) << std::dec << ", PID " << pid << ")" << std::endl;
            warnings_to_show--;
        }
        if ((s & PAGE_DIRTY_MASK) > 0) {
            // write out the page
            flushDirtyPage(pid);
            pages_written++;
        }
    }
    if (warnings_to_show < 0)
        std::cout << "[vmcache] " << -warnings_to_show << " warnings not shown" << std::endl;
    if (stats_on_shutdown) {
        std::cout << "[vmcache] " << "Wrote " << pages_written << " of " << num_allocated_pages << " pages to disk on shutdown" << std::endl;
        std::cout << "[vmcache] " << "At peak, " << peak_num_temporary_pages_in_use << " pages (" << std::setprecision(2) << peak_num_temporary_pages_in_use * PAGE_SIZE / 1024.0 / 1024.0 / 1024.0 << " GiB) were used for temporary data" << std::endl;
        // print general stats
        partitioning_strategy->printStats();
        std::cout << "[vmcache] " << "Total faulted: " << total_faulted_pages;
        std::cout << " (" << std::setiosflags(std::ios::fixed) << std::setprecision(2) << total_faulted_pages * PAGE_SIZE / 1024.0 / 1024.0 / 1024.0 << " GiB)" << std::endl;
    }
    delete[] page_states;
}

PageId VMCache::allocatePage() {
    if (num_allocated_pages > virtual_pages)
        throw std::runtime_error("Page limit reached");
    return num_allocated_pages++; // TODO: consider free pages here
}

char* VMCache::allocateTemporaryPage(uint32_t worker_id) {
    partitioning_strategy->prepareTempAllocation(1, worker_id);
    addToTemporaryPagesInUse(1);
    return reinterpret_cast<char*>(malloc(PAGE_SIZE));
}

char* VMCache::allocateTemporaryHugePage(const size_t num_pages, uint32_t worker_id) {
    partitioning_strategy->prepareTempAllocation(num_pages, worker_id);
    addToTemporaryPagesInUse(num_pages);
    return reinterpret_cast<char*>(malloc(num_pages * PAGE_SIZE));
}

void VMCache::dropTemporaryPage(char* page, __attribute__((unused)) uint32_t worker_id) {
    partitioning_strategy->notifyTempDropped(1);
    free(page);
    num_temporary_pages_in_use--;
}

void VMCache::dropTemporaryHugePage(char* page, const size_t num_pages, __attribute__((unused)) uint32_t worker_id) {
    partitioning_strategy->notifyTempDropped(num_pages);
    free(page);
    num_temporary_pages_in_use -= static_cast<int64_t>(num_pages);
}

// TODO: this should probably be part of the 'PartitioningStrategy'?
void VMCache::evictAll(bool check_residency, uint32_t worker_id) {
    size_t evicted_pages = 0;
    for (PageId pid = 0; pid < virtual_pages; pid++) {
        uint64_t s = page_states[pid].load();
        if (PAGE_STATE(s) == PAGE_STATE_MARKED || PAGE_STATE(s) == PAGE_STATE_FAULTED || PAGE_STATE(s) == PAGE_STATE_UNLOCKED) {
            if (page_states[pid].compare_exchange_strong(s, (s & ~PAGE_STATE_MASK) | PAGE_STATE_LOCKED)) {
                if ((s & PAGE_DIRTY_MASK) > 0) {
                    flushDirtyPage(pid);
                }
                uint64_t offset = pid * PAGE_SIZE;
                madvise(memory + offset, PAGE_SIZE, MADV_DONTNEED);
                partitioning_strategy->notifyDropped(pid, worker_id);
                page_states[pid].store(((page_states[pid].load() & ~PAGE_STATE_MASK) + (1ull << PAGE_VERSION_OFFSET)) | PAGE_STATE_EVICTED, std::memory_order_relaxed);
                evicted_pages++;
            }
        }
    }

    if (check_residency) {
        unsigned char* vec = reinterpret_cast<unsigned char*>(malloc(virtual_pages));
        mincore(memory, virtual_pages * PAGE_SIZE, vec);
        size_t still_resident_count = 0;
        size_t expected_resident_count = 0;
        for (size_t i = 0; i < virtual_pages; i++) {
            if ((vec[i] & 0x1) == 0x1) {
                still_resident_count++;
                std::cout << "[vmcache] " << "Page " << i << " is resident in memory (" << std::hex << page_states[i].load() << std::dec << ")" << std::endl;
            }
            if (PAGE_STATE(page_states[i].load()) != PAGE_STATE_EVICTED) {
                expected_resident_count++;
            }
        }
        std::cout << "[vmcache] " << still_resident_count << " pages are still resident in memory, expected " << expected_resident_count << "!" << std::endl;
        free(vec);
    }
}

void VMCache::printMemoryUsage() const {
    partitioning_strategy->printMemoryUsage();
}

void VMCache::flushDirtyPage(const PageId pid) {
    uint64_t offset = PAGE_SIZE * pid;
    auto written = pwrite(fd, memory + offset, PAGE_SIZE, offset);
    if (written != PAGE_SIZE) {
        std::cout << "[vmcache] " << "Error: Failed to write page (errno " << errno << ", " << errnoStr() << ")" << std::endl;
        errno = 0;
        // TODO: proper error handling (but what should we even do if this write fails???)
    }
    // clear dirty bit
    uint64_t s = page_states[pid].load();
    uint64_t new_s;
    do {
        new_s = s & ~PAGE_DIRTY_MASK;
    } while(!page_states[pid].compare_exchange_weak(s, new_s));
}

size_t VMCache::getNumLatchedDataPages() const {
    return partitioning_strategy->getNumLatchedPages(num_allocated_pages - 1);
}