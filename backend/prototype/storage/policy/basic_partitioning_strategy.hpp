#pragma once

#include <atomic>
#include <memory>

#include "partitioning_strategy.hpp"

template <class PartitionType>
class BasicPartitioningStrategy : public PartitioningStrategy {
public:
    BasicPartitioningStrategy();

    void setVMCache(VMCache* vmcache, const size_t num_workers) override;
    void prepareTempAllocation(size_t num_pages, uint32_t worker_id) override;
    void preFault(const PageId pid, bool scan, uint32_t worker_id) override;
    void ref(const PageId pid, bool scan, uint32_t worker_id) override;
    void notifyDropped(const PageId pid, uint32_t worker_id) override;
    void notifyTempDropped(size_t num_pages) override;
    bool performIdleMaintenance(uint32_t worker_id) override;
    size_t getPerPageMemoryCost() const override;
    size_t getConstantMemoryCost(const size_t num_workers) const override;
    size_t getNumLatchedPages(PageId max_pid) const override;
    void printMemoryUsage() const override;
    void printStats() const override;
    size_t getTotalEvictedPageCount() const override;
    size_t getTotalDirtyWritePageCount() const override;

private:
    std::unique_ptr<PartitionType> partition;
};