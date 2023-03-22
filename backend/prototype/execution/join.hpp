#pragma once

#include "pipeline_breaker.hpp"
#include "../storage/vmcache.hpp"

#define HASH_TAG_BITS 4
#define HASH_TAG_BITS_LOG2 2
#define HASH_TAG_MASK (((1ull << HASH_TAG_BITS) - 1ull) << (64 - HASH_TAG_BITS))
#define TAG_FROM_HASH(hash) (1ull << (((hash & (HASH_TAG_BITS - 1)) + 64 - HASH_TAG_BITS)))

class JoinBreaker : public PipelineBreakerBase {
public:
    JoinBreaker(VMCache& vmcache, BatchDescription& batch_description, size_t num_workers) : PipelineBreakerBase(batch_description), vmcache(vmcache), batches(num_workers), valid_row_count(0) { }

    void push(std::shared_ptr<Batch> batch, uint32_t worker_id) override {
        std::shared_ptr<Batch> current_batch;
        const uint32_t row_size = batch->getRowSize();
        if (row_size != batch_description.getRowSize() - sizeof(void*))
            throw std::runtime_error("JoinBreaker: Batch row size does not match batch_description");
        const uint32_t row_size_including_next_ptr = row_size + sizeof(void*);
        if (batches[worker_id].empty()) {
            batches[worker_id].push_back(std::make_shared<Batch>(vmcache, row_size_including_next_ptr, worker_id));
        }
        current_batch = batches[worker_id].back();

        // copy rows to new batches, leave space for pointers for chained addressing
        for (uint32_t i = 0; i < batch->getCurrentSize(); i++) {
            if (!batch->isRowValid(i))
                continue;
            uint32_t row_id;
            void* loc = current_batch->addRowIfPossible(row_id);
            if (loc == nullptr) {
                batches[worker_id].push_back(std::make_shared<Batch>(vmcache, row_size_including_next_ptr, worker_id));
                current_batch = batches[worker_id].back();
                loc = current_batch->addRowIfPossible(row_id);
            }
            *reinterpret_cast<void**>(loc) = nullptr;
            memcpy(reinterpret_cast<char*>(loc) + sizeof(void*), batch->getRow(i), row_size);
        }
        valid_row_count += batch->getValidRowCount();
    }

    void consumeBatches(std::vector<std::shared_ptr<Batch>>& target, uint32_t) override {
        if (!target.empty()) {
            throw std::runtime_error("Target not empty");
        }

        // TODO: ensure that NUMA-locality is maintained as much as possible
        size_t batch_count = 0;
        for (const std::vector<std::shared_ptr<Batch>>& worker_batches : batches) {
            batch_count += worker_batches.size();
        }
        target.reserve(batch_count);
        for (std::vector<std::shared_ptr<Batch>>& worker_batches : batches) {
            for (std::shared_ptr<Batch>& batch : worker_batches) {
                target.push_back(batch);
                batch = nullptr;
            }
        }
    }

    size_t getValidRowCount() const {
        return valid_row_count.load();
    }

private:
    VMCache& vmcache;
    std::vector<std::vector<std::shared_ptr<Batch>>> batches;
    std::atomic_size_t valid_row_count;
};

class JoinBuild : public PipelineStarterBreakerBase {
public:
    friend class JoinProbe;
    friend class JoinHTInit;

    JoinBuild(VMCache& vmcache, BatchDescription& batch_description, std::shared_ptr<JoinBreaker> input, size_t key_size) : PipelineStarterBreakerBase(batch_description), input(input), key_size(key_size), ht_bits(0), vmcache(vmcache), ht(nullptr) { }

    ~JoinBuild() {
        if (ht != nullptr)
            vmcache.dropTemporaryHugePage(reinterpret_cast<char*>(ht), std::max((1ull << ht_bits) * sizeof(void*), PAGE_SIZE) / PAGE_SIZE, worker_id);
    }

    void execute(size_t from, size_t to, uint32_t) override {
        switch (key_size) {
            case 4:
                joinBuildKernel<uint32_t>(from, to);
                break;
            case 8:
                joinBuildKernel<uint64_t>(from, to);
                break;
            default:
                generalJoinBuildKernel(from, to, key_size);
                break;
        }
    }

    // this operator does not produce any batches, instead it builds the hash table 'ht', which is used by the 'JoinProbe' operator for the probe operation
    void consumeBatches(std::vector<std::shared_ptr<Batch>>&, uint32_t) override { }

    size_t getInputSize() const override { return batches.size(); }
    size_t getMorselSizeHint() const override { return 1; } // morsel size = 1 batch

private:
    void allocateHT(uint32_t worker_id) {
        this->worker_id = worker_id; // TODO: this is not a good solution, as the worker that allocates the hash table will not necessarily be the one that frees it later. Find a proper solution
        // get input tuples
        input->consumeBatches(batches, worker_id);
        // allocate hash table
        const size_t min_ht_size = input->getValidRowCount() * 2;
        ht_bits = (64 - __builtin_clzl(min_ht_size - 1)); // use next power of 2 as actual hash table size
        const size_t ht_size = std::max((1ull << ht_bits) * sizeof(void*), PAGE_SIZE);
        ht = reinterpret_cast<std::atomic<void*>*>(vmcache.allocateTemporaryHugePage(ht_size / PAGE_SIZE, worker_id));
        //std::cout << "Building ht with size " << ht_size << " (" << ht_size / 1024 / 1024 << " MiB)" << " for " << input->getValidRowCount() << " build tuples" << std::endl;
    }

    template <typename key_type>
    void joinBuildKernel(size_t from, size_t to);
    void generalJoinBuildKernel(size_t from, size_t to, size_t key_size);

    std::shared_ptr<JoinBreaker> input;
    std::vector<std::shared_ptr<Batch>> batches;
    size_t key_size;
    size_t ht_bits;
    VMCache& vmcache;
    std::atomic<void*>* ht;
    uint32_t worker_id;
};

class JoinHTInit : public PipelineStarterBreakerBase {
public:
    JoinHTInit(BatchDescription& batch_description, std::shared_ptr<JoinBuild> output) : PipelineStarterBreakerBase(batch_description), output(output) { }
    static std::shared_ptr<JoinHTInit> create(std::shared_ptr<JoinBuild> output) {
        BatchDescription output_desc = BatchDescription(std::vector<NamedColumn>({}));
        return std::make_shared<JoinHTInit>(output_desc, output);
    }

    void pipelinePreExecutionSteps(uint32_t worker_id) override {
        output->allocateHT(worker_id);
    }

    void execute(size_t from, size_t to, uint32_t) override {
        memset(output->ht + from, 0, (to - from) * sizeof(std::atomic<void*>));
    }

    // this operator does not produce any batches
    void consumeBatches(std::vector<std::shared_ptr<Batch>>&, uint32_t) override { }

    size_t getInputSize() const override { return (1ull << output->ht_bits); }
    size_t getMorselSizeHint() const override { return 128ull * 1024ull; } // initialize 1 MiB blocks (as each ht bucket is 8B wide)

private:
    std::shared_ptr<JoinBuild> output;
};

class JoinProbe : public OperatorBase {
public:
    JoinProbe(VMCache& vmcache, std::shared_ptr<JoinBuild> build, BatchDescription& build_columns, BatchDescription& probe_columns, BatchDescription& output_columns)
    : vmcache(vmcache)
    , build(build)
    {
        this->build_columns.swap(build_columns);
        this->probe_columns.swap(probe_columns);
        this->output_columns.swap(output_columns);
        const auto& output_cols = this->output_columns.getColumns();
        output_column_infos.reserve(output_cols.size());
        for (auto& col : output_cols) {
            output_column_infos.push_back(JoinColumnInfo {});
            if (this->probe_columns.tryFind(col.name, output_column_infos.back().column)) {
                output_column_infos.back().from_probe = true;
            } else if (this->build_columns.tryFind(col.name, output_column_infos.back().column)) {
                output_column_infos.back().from_probe = false;
            } else {
                throw std::runtime_error("Join output column name '" + col.name + "' not found in build and probe inputs");
            }
        }
    }

    void push(std::shared_ptr<Batch> batch, uint32_t worker_id) override {
        const size_t tuple_size = output_columns.getRowSize();
        std::shared_ptr<Batch> results = std::make_shared<Batch>(vmcache, tuple_size, worker_id);
        switch (build->key_size) {
            case 4:
                joinProbeKernel<uint32_t>(batch, results, worker_id);
                break;
            case 8:
                joinProbeKernel<uint64_t>(batch, results, worker_id);
                break;
            default:
                generalJoinProbeKernel(batch, results, build->key_size, worker_id);
                break;
        }
        // push the last batch
        if (results->getCurrentSize() > 0)
            next_operator->push(results, worker_id);
    }

private:
    template <typename key_type>
    void joinProbeKernel(const std::shared_ptr<Batch>& batch, std::shared_ptr<Batch>& results, uint32_t worker_id);
    void generalJoinProbeKernel(const std::shared_ptr<Batch>& batch, std::shared_ptr<Batch>& results, size_t key_size, uint32_t worker_id);

    VMCache& vmcache;
    std::shared_ptr<JoinBuild> build;
    BatchDescription build_columns;
    BatchDescription probe_columns;
    BatchDescription output_columns;

    struct JoinColumnInfo {
        ColumnInfo column;
        bool from_probe;
    };
    std::vector<JoinColumnInfo> output_column_infos;
};

class JoinFactory {
public:
    static std::shared_ptr<JoinBuild> createBuildPipelines(std::vector<std::unique_ptr<ExecutablePipeline>>& pipelines, VMCache& vmcache, const Pipeline& input, const size_t key_size);
};