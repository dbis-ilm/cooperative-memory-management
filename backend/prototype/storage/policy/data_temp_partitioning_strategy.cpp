#include "data_temp_partitioning_strategy.hpp"

#include <iomanip>
#include <iostream>

#include "cache_partition.hpp"

template <class PartitionType>
DataTempPartitioningStrategy<PartitionType>::DataTempPartitioningStrategy(const size_t temporary_page_reservation)
    : max_temp_physical_pages(temporary_page_reservation) {}

template <class PartitionType>
void DataTempPartitioningStrategy<PartitionType>::setVMCache(VMCache* vmcache, const size_t num_workers) {
    PartitioningStrategy::setVMCache(vmcache, num_workers);
    if (vmcache->getMaxPhysicalPages() <= max_temp_physical_pages)
        throw std::runtime_error("Invalid partitioned eviction policy configuration: Temporary page reservation exceeds VMCache's maximum physical page count!");
    // ~66% load factor at peak for both hash sets
    partitions[0] = std::make_unique<PartitionType>(*vmcache, vmcache->getMaxPhysicalPages() - max_temp_physical_pages, physical_data_pages, physical_temp_pages, num_workers);
    partitions[1] = std::make_unique<PartitionType>(*vmcache, max_temp_physical_pages, physical_data_pages, physical_temp_pages, num_workers);
}

template <class PartitionType>
void DataTempPartitioningStrategy<PartitionType>::prepareTempAllocation(size_t num_pages, uint32_t worker_id) {
    partitions[1]->prepareTempAllocation(num_pages, worker_id);
}

template <class PartitionType>
void DataTempPartitioningStrategy<PartitionType>::preFault(const PageId pid, bool scan, uint32_t worker_id) {
    partitions[0]->handleFault(pid, scan, worker_id);
}

template <class PartitionType>
void DataTempPartitioningStrategy<PartitionType>::ref(const PageId pid, bool scan, uint32_t worker_id) {
    partitions[0]->ref(pid, scan, worker_id);
}

template <class PartitionType>
void DataTempPartitioningStrategy<PartitionType>::notifyDropped(const PageId pid, uint32_t worker_id) {
    partitions[0]->notifyDropped(pid, worker_id);
}

template <class PartitionType>
void DataTempPartitioningStrategy<PartitionType>::notifyTempDropped(size_t num_pages) {
    partitions[1]->notifyTempDropped(num_pages);
}

template <class PartitionType>
size_t DataTempPartitioningStrategy<PartitionType>::getPerPageMemoryCost() const {
    return PartitionType::getPerPageMemoryCost();
}

template <class PartitionType>
size_t DataTempPartitioningStrategy<PartitionType>::getConstantMemoryCost(const size_t num_workers) const {
    return sizeof(DataTempPartitioningStrategy) + 2 * PartitionType::getConstantMemoryCost(num_workers);
}

template <class PartitionType>
size_t DataTempPartitioningStrategy<PartitionType>::getNumLatchedPages(PageId max_pid) const {
    return partitions[0]->getNumLatchedPages(max_pid) + partitions[1]->getNumLatchedPages(max_pid);
}

template <class PartitionType>
bool DataTempPartitioningStrategy<PartitionType>::performIdleMaintenance(uint32_t worker_id) {
    return partitions[0]->performIdleMaintenance(worker_id);
}

template <class PartitionType>
void DataTempPartitioningStrategy<PartitionType>::printMemoryUsage() const {
    std::cout << "[vmcache] " << "Data: ";
    partitions[0]->printMemoryUsage();
    std::cout << "[vmcache] " << "Temp: ";
    partitions[1]->printMemoryUsage();
}

template <class PartitionType>
void DataTempPartitioningStrategy<PartitionType>::printStats() const {
    printMemoryUsage();
    std::cout << "[vmcache] " << "Data evicted: ";
    partitions[0]->printEvictionStats();
    std::cout << "[vmcache] " << "Data dirty w: ";
    partitions[0]->printDirtyWriteStats();
    std::cout << "[vmcache] " << "Temp evicted: ";
    partitions[1]->printEvictionStats();
    std::cout << "[vmcache] " << "Temp dirty w: ";
    partitions[1]->printDirtyWriteStats();
}

template <class PartitionType>
size_t DataTempPartitioningStrategy<PartitionType>::getTotalEvictedPageCount() const {
    return partitions[0]->getTotalEvictedPageCount() + partitions[1]->getTotalEvictedPageCount();
}

template <class PartitionType>
size_t DataTempPartitioningStrategy<PartitionType>::getTotalDirtyWritePageCount() const {
    return partitions[0]->getTotalDirtyWritePageCount() + partitions[1]->getTotalDirtyWritePageCount();
}


INSTANTIATE_PARTITIONING_STRATEGY(DataTempPartitioningStrategy)