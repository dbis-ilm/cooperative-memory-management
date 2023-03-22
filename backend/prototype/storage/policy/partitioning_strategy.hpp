#pragma once

#include <atomic>
#include <cstddef>

#include "../../core/units.hpp"

class VMCache;

class PartitioningStrategy {
public:
    PartitioningStrategy()
    : physical_data_pages(0)
    , physical_temp_pages(0) { }
    virtual ~PartitioningStrategy() { };

    virtual void prepareTempAllocation(size_t num_pages, uint32_t worker_id) = 0;
    virtual void preFault(const PageId pid, bool scan, uint32_t worker_id) = 0;
    virtual void ref(const PageId pid, bool scan, uint32_t worker_id) = 0;
    virtual void notifyDropped(const PageId pid, uint32_t worker_id) = 0;
    virtual void notifyTempDropped(size_t num_pages) = 0;
    virtual size_t getPerPageMemoryCost() const = 0;
    virtual size_t getConstantMemoryCost(const size_t num_workers) const = 0;
    virtual void setVMCache(VMCache* vmcache, const size_t) { this->vmcache = vmcache; }
    virtual void printMemoryUsage() const = 0;
    virtual void printStats() const = 0;

    int64_t getCurrentPhysicalDataPageCount() const { return physical_data_pages.load(); }
    int64_t getCurrentPhysicalTempPageCount() const { return physical_temp_pages.load(); }
    virtual size_t getNumLatchedPages(PageId max_pid) const = 0;

protected:
    VMCache* vmcache;
    std::atomic_int64_t physical_data_pages;
    std::atomic_int64_t physical_temp_pages;
};