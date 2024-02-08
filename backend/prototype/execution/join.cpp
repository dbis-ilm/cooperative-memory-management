#include "join.hpp"

#include "../utils/memcpy.hpp"
#include "../utils/MurmurHash3.hpp"

template <typename key_type>
void JoinBuild::joinBuildKernel(size_t from, size_t to) {
    for (size_t i = from; i < to; i++) {
        const auto batch = batches[i];
        for (uint32_t row_id = 0; row_id < batch->getCurrentSize(); row_id++) {
            if (!batch->isRowValid(row_id))
                continue;
            void* row = batch->getRow(row_id);
            // NOTE: by convention, the join key is always expected to be at the start of each row (on the build side after the pointer to the next row); this saves us from doing a bunch of pointer arithmetic
            const char* key = reinterpret_cast<char*>(row) + sizeof(void*);
            uint32_t hash;
            MurmurHash3_x86_32(key, sizeof(key_type), 1, &hash);
            const uint64_t new_tag = TAG_FROM_HASH(hash); // tag is the bit position resulting from the lowest log2(HASH_TAG_BITS) of the hash value
            size_t slot = (hash >> HASH_TAG_BITS_LOG2) & ((1ull << ht_bits) - 1ull);
            assert(slot < (1ull << ht_bits));
            void* old = ht[slot].load();
            void* new_val = 0;
            do {
                assert(old != row);
                reinterpret_cast<uint64_t*>(row)[0] = (uint64_t)old & ~HASH_TAG_MASK; // row->next = old
                new_val = (void*)((uint64_t)row | ((uint64_t)old & HASH_TAG_MASK) | new_tag);
            } while (!ht[slot].compare_exchange_weak(old, new_val, std::memory_order_relaxed));
        }
    }
}

void JoinBuild::generalJoinBuildKernel(size_t from, size_t to, size_t key_size) {
    for (size_t i = from; i < to; i++) {
        const auto batch = batches[i];
        for (uint32_t row_id = 0; row_id < batch->getCurrentSize(); row_id++) {
            if (!batch->isRowValid(row_id))
                continue;
            void* row = batch->getRow(row_id);
            // NOTE: by convention, the join key is always expected to be at the start of each row (on the build side after the pointer to the next row); this saves us from doing a bunch of pointer arithmetic
            const char* key = reinterpret_cast<char*>(row) + sizeof(void*);
            uint32_t hash;
            MurmurHash3_x86_32(key, key_size, 1, &hash);
            const uint64_t new_tag = TAG_FROM_HASH(hash); // tag is the bit position resulting from the lowest log2(HASH_TAG_BITS) of the hash value
            size_t slot = (hash >> HASH_TAG_BITS_LOG2) & ((1ull << ht_bits) - 1ull);
            assert(slot < (1ull << ht_bits));
            void* old = ht[slot].load();
            void* new_val = 0;
            do {
                assert(old != row);
                reinterpret_cast<uint64_t*>(row)[0] = (uint64_t)old & ~HASH_TAG_MASK; // row->next = old
                new_val = (void*)((uint64_t)row | ((uint64_t)old & HASH_TAG_MASK) | new_tag);
            } while (!ht[slot].compare_exchange_weak(old, new_val, std::memory_order_relaxed));
        }
    }
}

template void JoinBuild::joinBuildKernel<uint32_t>(size_t from, size_t to);
template void JoinBuild::joinBuildKernel<uint64_t>(size_t from, size_t to);


template <typename key_type>
void JoinProbe::joinProbeKernel(const std::shared_ptr<Batch>& batch, IntermediateHelper& intermediates) {
    for (uint32_t row_id = 0; row_id < batch->getCurrentSize(); row_id++) {
        if (!batch->isRowValid(row_id))
            continue;
        const void* row = batch->getRow(row_id);
        const char* key = reinterpret_cast<const char*>(row);
        uint32_t hash;
        MurmurHash3_x86_32(key, sizeof(key_type), 1, &hash);
        const size_t slot = (hash >> HASH_TAG_BITS_LOG2) & ((1ull << build->ht_bits) - 1ull);
        const uint64_t expected_tag = TAG_FROM_HASH(hash);
        uint64_t bucket_val = (uint64_t)(reinterpret_cast<void**>(build->ht)[slot]);
        const uint64_t tag = bucket_val & HASH_TAG_MASK;
        if ((tag & expected_tag) == 0)
            continue;
        const char* ptr = (char*)(bucket_val & ~HASH_TAG_MASK);
        const key_type key_val = *reinterpret_cast<const key_type*>(key);
        while (ptr != nullptr) {
            const void* build_row = ptr + sizeof(void*);
            ptr = reinterpret_cast<const char* const*>(ptr)[0];
            if (key_val == *reinterpret_cast<const key_type*>(build_row)) {
                char* loc = intermediates.addRow();
                for (auto& col : output_column_infos) {
                    const size_t sz = col.column.column->getValueTypeSize();
                    const char* val_ptr = reinterpret_cast<const char*>(col.from_probe ? row : build_row) + col.column.offset;
                    fast_memcpy(loc, val_ptr, sz);
                    loc += sz;
                }
            }
        }
    }
}

void JoinProbe::generalJoinProbeKernel(const std::shared_ptr<Batch>& batch, IntermediateHelper& intermediates, size_t key_size) {
    for (uint32_t row_id = 0; row_id < batch->getCurrentSize(); row_id++) {
        if (!batch->isRowValid(row_id))
            continue;
        const void* row = batch->getRow(row_id);
        const char* key = reinterpret_cast<const char*>(row);
        uint32_t hash;
        MurmurHash3_x86_32(key, key_size, 1, &hash);
        const size_t slot = (hash >> HASH_TAG_BITS_LOG2) & ((1ull << build->ht_bits) - 1ull);
        const uint64_t expected_tag = TAG_FROM_HASH(hash);
        uint64_t bucket_val = (uint64_t)(reinterpret_cast<void**>(build->ht)[slot]);
        const uint64_t tag = bucket_val & HASH_TAG_MASK;
        if ((tag & expected_tag) == 0)
            continue;
        const char* ptr = (char*)(bucket_val & ~HASH_TAG_MASK);
        while (ptr != nullptr) {
            const void* build_row = ptr + sizeof(void*);
            ptr = reinterpret_cast<const char* const*>(ptr)[0];
            if (memcmp(key, build_row, key_size) == 0) {
                char* loc = intermediates.addRow();
                for (auto& col : output_column_infos) {
                    const size_t sz = col.column.column->getValueTypeSize();
                    const char* val_ptr = reinterpret_cast<const char*>(col.from_probe ? row : build_row) + col.column.offset;
                    fast_memcpy(loc, val_ptr, sz);
                    loc += sz;
                }
            }
        }
    }
}

template void JoinProbe::joinProbeKernel<uint32_t>(const std::shared_ptr<Batch>& batch, IntermediateHelper& intermediates);
template void JoinProbe::joinProbeKernel<uint64_t>(const std::shared_ptr<Batch>& batch, IntermediateHelper& intermediates);

std::shared_ptr<JoinBuild> JoinFactory::createBuildPipelines(std::vector<std::unique_ptr<ExecutablePipeline>>& pipelines, VMCache& vmcache, const Pipeline& input, const size_t key_size) {
    auto breaker = std::dynamic_pointer_cast<JoinBreaker>(input.breaker);
    if (breaker == nullptr)
        throw std::runtime_error("Pipeline without join breaker supplied as input in createBuildPipelines()!");
    BatchDescription output_desc = BatchDescription(std::vector<NamedColumn>({}));
    auto join_build = std::make_shared<JoinBuild>(vmcache, output_desc, breaker, key_size);
    auto join_init = JoinHTInit::create(join_build);
    size_t init_pipeline_id = pipelines.size();
    pipelines.push_back(std::make_unique<ExecutablePipeline>(init_pipeline_id));
    pipelines.back()->addBreaker(join_init);
    pipelines.back()->addDependency(input.getId());
    pipelines.push_back(std::make_unique<ExecutablePipeline>(pipelines.size()));
    pipelines.back()->addBreaker(join_build);
    pipelines.back()->addDependency(init_pipeline_id);
    pipelines.back()->current_columns = BatchDescription(std::vector<NamedColumn>(breaker->batch_description.getColumns()));
    return join_build;
}