#pragma once

#include "pipeline_breaker.hpp"
#include "pipeline_starter.hpp"

// Aggregation operator implementation following Leis, Viktor, Peter A. Boncz, Alfons Kemper, and Thomas Neumann. “Morsel-Driven Parallelism: A NUMA-Aware Query Evaluation Framework for the Many-Core Age.” In Proceedings of the 2014 ACM SIGMOD International Conference on Management of Data, 743–54. SIGMOD ’14. New York, NY, USA: Association for Computing Machinery, 2014. https://doi.org/10.1145/2588555.2610507.

// Phase 1: thread-local pre-aggregation, spill into global partitions on overflow
class AggregationBreaker : public DefaultBreaker {
    friend class AggregationOperator;

public:
    AggregationBreaker(VMCache& vmcache, BatchDescription& batch_description, const size_t key_size, size_t num_workers); // TODO: add support for "payloads" (e.g., count/min/max/sum/etc of arbitrary input columns)

    void push(std::shared_ptr<Batch> batch, uint32_t worker_id) override;
    void flush(uint32_t ht_id, bool deallocate, uint32_t worker_id); // flushes local HT (if exists) to global partitions, optionally deallocates the HT

private:
    VMCache& vmcache;
    const size_t key_size;
    size_t ht_capacity;
    size_t ht_data_offset;
    std::vector<char*> hts;
    std::atomic_uint32_t flush_count;
    std::vector<std::vector<std::shared_ptr<Batch>>> flushed_tuples; // TODO: partition tuples right away when flushing
};

// Phase 2: aggregate per partition and push to next operators
class AggregationOperator : public PipelineStarterBase {
    friend class AggregationBreaker;
public:
    AggregationOperator(VMCache& vmcache, const std::shared_ptr<AggregationBreaker>& breaker)
    : vmcache(vmcache)
    , breaker(breaker) { }

    void execute(size_t, size_t, uint32_t worker_id) override;

    size_t getInputSize() const override { return 1; } // TODO: return partititon count???

    double getExpectedTimePerUnit() const override { return 0.001; }

    void pipelinePreExecutionSteps(uint32_t worker_id) override;

private:
    VMCache& vmcache;
    std::shared_ptr<AggregationBreaker> breaker;
};