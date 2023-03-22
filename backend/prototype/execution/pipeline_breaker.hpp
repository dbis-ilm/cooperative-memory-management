#pragma once

#include "operator.hpp"
#include "pipeline_starter.hpp"

class PipelineBreakerBase : public OperatorBase {
protected:
    BatchDescription batch_description;

public:
    PipelineBreakerBase(BatchDescription& batch_description);
    virtual ~PipelineBreakerBase();

    void consumeBatchDescription(BatchDescription& target);
    virtual void consumeBatches(std::vector<std::shared_ptr<Batch>>& target, uint32_t worker_id) = 0;

    friend class JoinFactory;
};

class PipelineStarterBreakerBase : public PipelineStarterBase, public PipelineBreakerBase {
public:
    PipelineStarterBreakerBase(BatchDescription& batch_description);
    virtual ~PipelineStarterBreakerBase();

    void push(std::shared_ptr<Batch>, uint32_t) override final {};
};

class DefaultBreaker : public PipelineBreakerBase {
protected:
    std::vector<std::vector<std::shared_ptr<Batch>>> batches;
    std::atomic_size_t valid_row_count;

public:
    DefaultBreaker(BatchDescription& batch_description, size_t num_workers);

    void push(std::shared_ptr<Batch> batch, uint32_t worker_id) override;
    void consumeBatches(std::vector<std::shared_ptr<Batch>>& target, uint32_t worker_id) override;
    size_t getValidRowCount() const;
};