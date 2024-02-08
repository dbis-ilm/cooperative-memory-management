#include "aggregation.hpp"

#include "../utils/MurmurHash3.hpp"
#include "../utils/memcpy.hpp"

const size_t LOCAL_HT_SIZE = PAGE_SIZE;
#define LOCAL_HT_NUM_PAGES (PAGE_SIZE / LOCAL_HT_SIZE)
#define LOCAL_HT_BITSET_SIZE(capacity) ((((capacity) + 7) / 8 + 7) / 8 * 8)
#define LOCAL_HT_SIZE_SIZE sizeof(uint64_t)

#define BITSET_BLOCK_SIZE (sizeof(uint64_t) * 8)
#define BIT_SET(bitset, slot) ((((bitset)[(slot) / BITSET_BLOCK_SIZE] >> ((slot) % BITSET_BLOCK_SIZE)) & 0x1) != 0)
#define SET_BIT(bitset, slot) { (bitset)[(slot) / BITSET_BLOCK_SIZE] |= 0x1ull << ((slot) % BITSET_BLOCK_SIZE); }

AggregationBreaker::AggregationBreaker(VMCache& vmcache, BatchDescription& batch_description, const size_t key_size, size_t num_workers)
: DefaultBreaker(batch_description, num_workers)
, vmcache(vmcache)
, key_size(key_size)
, hts(num_workers, nullptr)
, flushed_tuples(num_workers, std::vector<std::shared_ptr<Batch>>()) {
    ht_capacity = (LOCAL_HT_SIZE - LOCAL_HT_SIZE_SIZE) * 8 / (key_size * 8 + 1);
    while (LOCAL_HT_SIZE < LOCAL_HT_SIZE_SIZE + ht_capacity * key_size + LOCAL_HT_BITSET_SIZE(ht_capacity)) {
        ht_capacity--;
    }
    ht_data_offset = LOCAL_HT_SIZE_SIZE + LOCAL_HT_BITSET_SIZE(ht_capacity);
    std::cout << ht_data_offset << std::endl;
}

void AggregationBreaker::push(std::shared_ptr<Batch> batch, uint32_t worker_id) {
    // allocate local HT if not allocated yet
    if (hts[worker_id] == nullptr) {
        hts[worker_id] = vmcache.allocateTemporaryHugePage(LOCAL_HT_NUM_PAGES, worker_id);
        memset(hts[worker_id], 0, ht_data_offset);
    }
    // insert keys into local HT
    for (uint32_t row_id = 0; row_id < batch->getCurrentSize(); row_id++) {
        if (!batch->isRowValid(row_id))
            continue;
        const void* row = batch->getRow(row_id);
        const char* key = reinterpret_cast<const char*>(row);
        uint32_t hash;
        MurmurHash3_x86_32(key, key_size, 1, &hash);
        uint64_t& ht_size = *reinterpret_cast<uint64_t*>(hts[worker_id]);
        uint64_t* bitset = reinterpret_cast<uint64_t*>(hts[worker_id] + LOCAL_HT_SIZE_SIZE);
        uint32_t slot = hash % ht_capacity;
        while (true) {
            if (BIT_SET(bitset, slot)) {
                // slot is already occupied, check if it contains 'key' already
                if (memcmp(hts[worker_id] + ht_data_offset + key_size * slot, key, key_size) == 0) {
                    // slot contains 'key' already, done
                    break;
                }
                // slot contains different key, try next slot
                slot++;
            } else {
                // slot is empty, insert key
                fast_memcpy(hts[worker_id] + ht_data_offset + key_size * slot, key, key_size);
                SET_BIT(bitset, slot);
                ht_size++;
                break;
            }
        }

        if (ht_size > ht_capacity * 0.7) {
            flush(worker_id, false, worker_id);
        }
    }
}

void AggregationBreaker::flush(uint32_t ht_id, bool deallocate, uint32_t worker_id) {
    uint64_t* bitset = reinterpret_cast<uint64_t*>(hts[ht_id] + LOCAL_HT_SIZE_SIZE);
    bool did_flush = false;
    for (size_t slot = 0; slot < ht_capacity; slot++) {
        if (BIT_SET(bitset, slot)) {
            char* loc = nullptr;
            uint32_t row_id;
            if (flushed_tuples[ht_id].empty()) {
                flushed_tuples[ht_id].push_back(std::make_shared<Batch>(vmcache, key_size, worker_id));
            }
            loc = reinterpret_cast<char*>(flushed_tuples[ht_id].back()->addRowIfPossible(row_id));
            if (loc == nullptr) {
                flushed_tuples[ht_id].push_back(std::make_shared<Batch>(vmcache, key_size, worker_id));
                loc = reinterpret_cast<char*>(flushed_tuples[ht_id].back()->addRowIfPossible(row_id));
            }
            fast_memcpy(loc, hts[worker_id] + ht_data_offset + key_size * slot, key_size);
            did_flush = true;
        }
    }

    if (deallocate)
        vmcache.dropTemporaryHugePage(hts[ht_id], LOCAL_HT_NUM_PAGES, worker_id);

    if (did_flush)
        flush_count.fetch_add(1, std::memory_order_relaxed);
}

void AggregationOperator::pipelinePreExecutionSteps(uint32_t worker_id) {
    for (uint32_t wid = 0; wid < breaker->hts.size(); wid++) {
        if (breaker->hts[wid] != nullptr)
            breaker->flush(wid, true, worker_id);
    }
}

void AggregationOperator::execute(size_t, size_t, uint32_t worker_id) {
    if (breaker->flush_count == 0)
        return;

    if (breaker->flush_count == 1) {
        for (auto& partition : breaker->flushed_tuples) {
            for (auto& batch : partition)
                next_operator->push(batch, worker_id);
        }
    } else {
        throw std::runtime_error("Global hash table for aggregation is not implemented yet!");
        // TODO: aggregate partition-wise, push to next
    }
}