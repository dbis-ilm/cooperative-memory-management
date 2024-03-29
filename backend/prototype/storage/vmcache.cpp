#include "vmcache.hpp"

#include <fcntl.h>
#include <iomanip>
#include <iostream>
#include <stdexcept>
#include <sys/mman.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/types.h>


// exmap helper function
int exmapAction(int exmapfd, exmap_opcode op, uint16_t len, uint32_t worker_id) {
   struct exmap_action_params params_free = { .interface = static_cast<uint16_t>(worker_id), .iov_len = len, .opcode = (uint16_t)op, .flags = 0 };
   return ioctl(exmapfd, EXMAP_IOCTL_ACTION, &params_free);
}

VMCache::VMCache(uint64_t max_size, uint64_t virtual_pages, const std::string& path, bool sandbox, bool no_dirty_writeback, bool flush_asynchronously, bool use_eviction_target, std::unique_ptr<PartitioningStrategy>&& partitioning_strategy, bool use_exmap, bool stats_on_shutdown, const size_t num_workers)
    : use_exmap(use_exmap)
    , stats_on_shutdown(stats_on_shutdown)
    , max_size(max_size)
    , virtual_pages(virtual_pages)
    , max_physical_pages((max_size - sizeof(VMCache) - sizeof(VMCacheStats) * num_workers - partitioning_strategy->getConstantMemoryCost(num_workers) - virtual_pages * sizeof(PageState)) / (PAGE_SIZE + partitioning_strategy->getPerPageMemoryCost())) // memory cost per physical page is the page size itself + eviction policy overhead
    , num_allocated_pages(0)
    , partitioning_strategy(std::move(partitioning_strategy))
    , num_temporary_pages_in_use(0)
    , peak_num_temporary_pages_in_use(0)
    , num_dirty_pages(0)
    , page_states(new PageState[virtual_pages])
    , sandbox(sandbox)
    , dirty_writeback(!no_dirty_writeback)
    , flush_asynchronously(flush_asynchronously)
    , use_eviction_target(use_eviction_target)
    , db_path(path)
    , num_workers(num_workers)
{
    int flags = O_RDWR | O_DIRECT;
    struct stat st;
    if (stat(path.c_str(), &st) != 0) {
        flags |= O_CREAT;
    }
    fd = open(path.c_str(), flags, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if (fd == -1) {
        std::cout << "Error: Failed to open database file (errno " << errno << ", " << errnoStr() << ")" << std::endl;
        errno = 0;
        throw std::runtime_error("Failed to open database file");
    }
    shadow_fd = open((path + ".shadow").c_str(), O_RDWR | O_DIRECT | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
    if (shadow_fd == -1) {
        std::cout << "Error: Failed to create shadow file (errno " << errno << ", " << errnoStr() << ")" << std::endl;
        errno = 0;
        throw std::runtime_error("Failed to create shadow file");
    }

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

    if (use_exmap) {
        exmap_fd = open("/dev/exmap", O_RDWR);
        if (exmap_fd < 0)
            throw std::runtime_error("Unable to open exmap, did you load the kernel module?");

        struct exmap_ioctl_setup buffer;
        buffer.fd = -1;
        buffer.max_interfaces = num_workers;
        buffer.buffer_size = max_physical_pages;
        buffer.flags = 0;
        if (ioctl(exmap_fd, EXMAP_IOCTL_SETUP, &buffer) < 0)
            throw std::runtime_error("EXMAP_SETUP failed");

        for (size_t i = 0; i < num_workers; i++) {
            exmap_interface.push_back(reinterpret_cast<struct exmap_user_interface*>(mmap(NULL, PAGE_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, exmap_fd, EXMAP_OFF_INTERFACE(i))));
            if (exmap_interface.back() == MAP_FAILED)
                throw std::runtime_error("exmap interface setup failed");
        }

        memory = reinterpret_cast<char*>(mmap(0, virtual_pages * PAGE_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, exmap_fd, 0));
    } else {
        memory = reinterpret_cast<char*>(mmap(0, virtual_pages * PAGE_SIZE, PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_PRIVATE | MAP_NORESERVE, -1, 0));
        if (memory == MAP_FAILED)
            throw std::runtime_error("Failed to create anonymous memory mapping for vmcache");
        madvise(memory, virtual_pages * PAGE_SIZE, MADV_DONTNEED | MADV_NOHUGEPAGE);
    }
    this->partitioning_strategy->setVMCache(this, num_workers);

    // initialize stat counters
    stats = new VMCacheStats[num_workers];
    for (size_t i = 0; i < num_workers; ++i) {
        stats[i].total_accessed_pages = 0;
        stats[i].total_faulted_pages = 0;
    }
}

VMCache::~VMCache() {
    // write out dirty pages from memory
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
        if ((s & PAGE_DIRTY_BIT) > 0 && !(sandbox && (s & PAGE_MODIFIED_BIT) > 0)) {
            // write out the page
            flushDirtyPage(pid);
            pages_written++;
        }
    }
    if (warnings_to_show < 0)
        std::cout << "[vmcache] " << -warnings_to_show << " warnings not shown" << std::endl;

    // copy shadow pages from the shadow file to the database file
    if (!sandbox) {
        size_t shadow_pages_copied = 0;
        warnings_to_show = 4;
        for (PageId pid = 0; pid < virtual_pages; pid++) {
            if (PAGE_MODIFIED(page_states[pid].load())) {
                // copy the page from the shadow file to the database file
                // note: we just reuse the first page of 'memory' here as a buffer for copying
                //  this is safe as all changes were previously flushed to the shadow file
                const size_t offset = pid * PAGE_SIZE;
                if (pread(shadow_fd, memory, PAGE_SIZE, offset) != PAGE_SIZE) {
                    if (warnings_to_show > 0)
                        std::cout << "[vmcache] Warning: Failed to read (errno " << errno << ", " << errnoStr() << ") from shadow file on shutdown" << std::endl;
                    warnings_to_show--;
                }
                if (pwrite(fd, memory, PAGE_SIZE, offset) != PAGE_SIZE) {
                    if (warnings_to_show > 0)
                        std::cout << "[vmcache] Warning: Failed to copy (errno " << errno << ", " << errnoStr() << ") from shadow file to database file on shutdown" << std::endl;
                    warnings_to_show--;
                }
                shadow_pages_copied++;
            }
        }
        std::cout << "[vmcache] " << "Copied " << shadow_pages_copied << " shadow pages to the database file on shutdown" << std::endl;
    }
    if (warnings_to_show < 0)
        std::cout << "[vmcache] " << -warnings_to_show << " warnings not shown" << std::endl;

    close(shadow_fd);
    if (unlink((db_path + ".shadow").c_str()) != 0)
        std::cerr << "Warning: Failed to delete database shadow file" << std::endl;
    close(fd);
    munmap(memory, virtual_pages * PAGE_SIZE);
    if (stats_on_shutdown) {
        std::cout << "[vmcache] " << "Wrote " << pages_written << " of " << num_allocated_pages << " pages to disk on shutdown" << std::endl;
        std::cout << "[vmcache] " << "At peak, " << peak_num_temporary_pages_in_use << " pages (" << std::setprecision(2) << peak_num_temporary_pages_in_use * PAGE_SIZE / 1024.0 / 1024.0 / 1024.0 << " GiB) were used for temporary data" << std::endl;
        // print general stats
        partitioning_strategy->printStats();
        std::cout << "[vmcache] " << "Total faulted: " << getTotalFaultedPageCount();
        std::cout << " (" << std::setiosflags(std::ios::fixed) << std::setprecision(2) << getTotalFaultedPageCount() * PAGE_SIZE / 1024.0 / 1024.0 / 1024.0 << " GiB)" << std::endl;
    }
    delete[] page_states;
    delete[] stats;
}

PageId VMCache::allocatePage() {
    if (num_allocated_pages > virtual_pages)
        throw std::runtime_error("Page limit reached");
    return num_allocated_pages++; // TODO: consider free pages here - could, e.g., make free pages form a linked list
}

char* VMCache::allocateTemporaryPage(uint32_t worker_id) {
    partitioning_strategy->prepareTempAllocation(1, worker_id);
    addToTemporaryPagesInUse(1);
    return reinterpret_cast<char*>(malloc(PAGE_SIZE));
}

char* VMCache::allocateTemporaryHugePage(const size_t num_pages, uint32_t worker_id) {
    char* result = nullptr;
    if (num_pages > LARGE_ALLOCATION_THRESHOLD && log_allocation_latency && *log_allocation_latency) {
        // log latency of "large" allocations
        auto begin = std::chrono::steady_clock::now();
        partitioning_strategy->prepareTempAllocation(num_pages, worker_id);
        addToTemporaryPagesInUse(num_pages);
        result = reinterpret_cast<char*>(malloc(num_pages * PAGE_SIZE));
        (*log_allocation_latency)(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - begin).count());
    } else {
        partitioning_strategy->prepareTempAllocation(num_pages, worker_id);
        addToTemporaryPagesInUse(num_pages);
        result = reinterpret_cast<char*>(malloc(num_pages * PAGE_SIZE));
    }
    return result;
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
                if ((s & PAGE_DIRTY_BIT) > 0) {
                    flushDirtyPage(pid);
                }
                if (use_exmap) {
                    exmap_interface[worker_id]->iov[0].page = pid;
                    exmap_interface[worker_id]->iov[0].len = 1;
                    if (exmapAction(exmap_fd, EXMAP_OP_FREE, 1, worker_id) < 0)
                        throw std::runtime_error("ioctl: EXMAP_OP_FREE");
                } else {
                    madvise(toPointer(pid), PAGE_SIZE, MADV_DONTNEED);
                }
                partitioning_strategy->notifyDropped(pid, worker_id);
                page_states[pid].store(((page_states[pid].load() & ~PAGE_STATE_MASK) + (1ull << PAGE_VERSION_OFFSET)) | PAGE_STATE_EVICTED, std::memory_order_release);
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
    // note: we write all dirty pages to the shadow file first, modified pages are only copied to the database file on shutdown if we are not in sandbox mode
    auto written = pwrite(shadow_fd, toPointer(pid), PAGE_SIZE, offset);
    if (written != PAGE_SIZE) {
        std::cout << "[vmcache] " << "Error: Failed to write page (errno " << errno << ", " << errnoStr() << ")" << std::endl;
        errno = 0;
        // TODO: proper error handling (but what should we even do if this write fails???)
    }
    // clear dirty bit, set modified bit
    uint64_t s = page_states[pid].load();
    uint64_t new_s;
    do {
        new_s = (s & ~PAGE_DIRTY_BIT) | PAGE_MODIFIED_BIT;
    } while(!page_states[pid].compare_exchange_weak(s, new_s));
    // update statistics
    num_dirty_pages--;
}

size_t VMCache::getNumLatchedDataPages() const {
    return partitioning_strategy->getNumLatchedPages(num_allocated_pages - 1);
}