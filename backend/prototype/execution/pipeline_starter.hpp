#pragma once

#include "operator.hpp"

#include "pipeline.hpp"

class PipelineStarterBase : public OperatorBase {
    friend class PipelineStarterBreakerBase;
    friend class Dispatcher;

protected:
    Pipeline* pipeline;

private:
    std::atomic_int16_t finalization_counter { 0 };
    std::atomic<size_t> next_row[MAX_NUMA_NODES];
    size_t last_row[MAX_NUMA_NODES];

public:
    // Pipeline starters differ from normal operators in that they do not receive batches from preceeding operators through 'push', but are instead invoked to execute on a certain range of input tuples (e.g., from a base table). Hence 'push' is marked as final here, and pipeline starters should instead implement 'execute'.
    void push(std::shared_ptr<Batch>, uint32_t) override {};
    virtual void execute(size_t from, size_t to, uint32_t worker_id) = 0;
    virtual size_t getInputSize() const = 0; // TODO: update visibility of these functions?!
    virtual size_t getMorselSizeHint() const = 0;
    virtual size_t getMinMorselSize() const { return 1; }
    virtual void pipelinePreExecutionSteps(uint32_t) { }

    void initializeMorsels(); // called by dispatcher and used to set up the internal state for morsel selection, i.e., 'next_row' and 'last_row'
    bool executeNextMorsel(size_t morsel_size, size_t socket, uint32_t worker_id);

    void setPipeline(Pipeline* pipeline) { this->pipeline = pipeline; }
    size_t getPipelineId() const { return pipeline->getId(); }
    double getPriority() { return 1.0; } // TODO: get priority from QEP and allow adjusting it there
};