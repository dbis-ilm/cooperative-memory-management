#include "basic_partitioning_strategy.hpp"

#include <iomanip>
#include <iostream>

#include "../vmcache.hpp"
#include "cache_partition.hpp"

template <class PartitionType>
BasicPartitioningStrategy<PartitionType>::BasicPartitioningStrategy() {}

template <class PartitionType>
void BasicPartitioningStrategy<PartitionType>::setVMCache(VMCache* vmcache, const size_t num_workers) {
    PartitioningStrategy::setVMCache(vmcache, num_workers);
    // ~66% load factor at peak for 'cached_pages'
    partition = std::make_unique<PartitionType>(*vmcache, vmcache->getMaxPhysicalPages(), physical_data_pages, physical_temp_pages, num_workers);
}

template <class PartitionType>
void BasicPartitioningStrategy<PartitionType>::prepareTempAllocation(size_t num_pages, uint32_t worker_id) {
    partition->prepareTempAllocation(num_pages, worker_id);
}

template <class PartitionType>
void BasicPartitioningStrategy<PartitionType>::preFault(const PageId pid, bool scan, uint32_t worker_id) {
    partition->handleFault(pid, scan, worker_id);
}

template <class PartitionType>
void BasicPartitioningStrategy<PartitionType>::ref(const PageId pid, bool scan, uint32_t worker_id) {
    partition->ref(pid, scan, worker_id);
}

template <class PartitionType>
void BasicPartitioningStrategy<PartitionType>::notifyDropped(const PageId pid, uint32_t worker_id) {
    partition->notifyDropped(pid, worker_id);
}

template <class PartitionType>
void BasicPartitioningStrategy<PartitionType>::notifyTempDropped(size_t num_pages) {
    partition->notifyTempDropped(num_pages);
}

template <class PartitionType>
size_t BasicPartitioningStrategy<PartitionType>::getPerPageMemoryCost() const {
    // space required in the cached_pages hash table (assumed to be sizeof(PageId) * 3 / 2 to get a load factor of ~66%)
    return PartitionType::getPerPageMemoryCost();
}

template <class PartitionType>
size_t BasicPartitioningStrategy<PartitionType>::getConstantMemoryCost(const size_t num_workers) const {
    return sizeof(BasicPartitioningStrategy<PartitionType>) + PartitionType::getConstantMemoryCost(num_workers);
}

template <class PartitionType>
size_t BasicPartitioningStrategy<PartitionType>::getNumLatchedPages(PageId max_pid) const {
    return partition->getNumLatchedPages(max_pid);
}

template <class PartitionType>
void BasicPartitioningStrategy<PartitionType>::printMemoryUsage() const {
    partition->printMemoryUsage();
}

template <class PartitionType>
void BasicPartitioningStrategy<PartitionType>::printStats() const {
    partition->printMemoryUsage();
    std::cout << "[vmcache] " << "Total evicted: ";
    partition->printEvictionStats();
    std::cout << "[vmcache] " << "Total dirty w: ";
    partition->printDirtyWriteStats();
}

INSTANTIATE_PARTITIONING_STRATEGY(BasicPartitioningStrategy)