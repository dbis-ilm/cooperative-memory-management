#pragma once

#include "pipeline_breaker.hpp"
#include "pipeline_starter.hpp"

enum class Order {
    Ascending,
    Descending
};

class SortBreaker : public DefaultBreaker {
    friend class SortOperator;

public:
    SortBreaker(BatchDescription& batch_description, const std::vector<NamedColumn>& sort_keys, const std::vector<Order>& sort_orders, size_t num_workers);

    void push(std::shared_ptr<Batch> batch, uint32_t worker_id) override;

private:
    std::vector<NamedColumn> sort_keys;
    std::vector<ColumnInfo> sort_key_infos;
    std::vector<Order> sort_orders;
    std::function<int(const Row&, const Row&)> comp;
};

class SortOperator : public PipelineStarterBase {
public:
    SortOperator(VMCache& vmcache, const std::shared_ptr<SortBreaker>& breaker)
    : vmcache(vmcache)
    , breaker(breaker) { }

    void execute(size_t, size_t, uint32_t worker_id) override;

    size_t getInputSize() const override { return 1; }

    size_t getMorselSizeHint() const override { return 1; }

    void pipelinePreExecutionSteps(uint32_t worker_id) override {
        breaker->consumeBatches(batches, worker_id);
    }

private:
    VMCache& vmcache;
    std::shared_ptr<SortBreaker> breaker;
    std::vector<std::shared_ptr<Batch>> batches;
};