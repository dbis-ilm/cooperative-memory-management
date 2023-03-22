#pragma once

#include <iomanip>
#include <iostream>
#include <memory>

#include "../vmcache.hpp"

template <class PartitionType>
class DataTempPartitioningStrategy : public PartitioningStrategy {
public:
    DataTempPartitioningStrategy(const size_t temporary_page_reservation);

    void setVMCache(VMCache* vmcache, const size_t num_workers) override;
    void prepareTempAllocation(size_t num_pages, uint32_t worker_id);
    void preFault(const PageId pid, bool scan, uint32_t worker_id) override;
    void ref(const PageId pid, bool scan, uint32_t worker_id) override;
    void notifyDropped(const PageId pid, uint32_t worker_id) override;
    void notifyTempDropped(size_t num_pages) override;
    size_t getPerPageMemoryCost() const override;
    size_t getConstantMemoryCost(const size_t num_workers) const override;
    size_t getNumLatchedPages(PageId max_pid) const override;
    void printMemoryUsage() const override;
    void printStats() const override;

private:
    std::unique_ptr<PartitionType> partitions[2];
    const size_t max_temp_physical_pages;
};