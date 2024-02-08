#pragma once

#include "../scheduling/execution_context.hpp"
#include "../scheduling/job.hpp"
#include "operator.hpp"
#include "pipeline.hpp"

class PipelineStarterBase : public OperatorBase {
    friend class PipelineStarterBreakerBase;
    friend class PipelineJob;

protected:
    Pipeline* pipeline;

public:
    // Pipeline starters differ from normal operators in that they do not receive batches from preceeding operators through 'push', but are instead invoked to execute on a certain range of input tuples (e.g., from a base table). Hence 'push' is marked as final here, and pipeline starters should instead implement 'execute'.
    void push(std::shared_ptr<Batch>, uint32_t) override {};
    virtual void execute(size_t from, size_t to, uint32_t worker_id) = 0;
    virtual size_t getInputSize() const = 0;
    virtual double getExpectedTimePerUnit() const = 0;
    virtual size_t getMinMorselSize() const { return 1; }
    virtual void pipelinePreExecutionSteps(uint32_t) { }

    void setPipeline(Pipeline* pipeline) { this->pipeline = pipeline; }
    size_t getPipelineId() const { return pipeline->getId(); }
};