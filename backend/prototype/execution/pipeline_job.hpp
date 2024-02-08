#pragma once

#include <atomic>
#include <cstddef>

#include "../scheduling/execution_context.hpp"
#include "../scheduling/job.hpp"

class PipelineStarterBase;


class PipelineJob : public Job {
public:
    PipelineJob(const std::shared_ptr<PipelineStarterBase>& starter);
    size_t getSize() const override;
    bool executeNextMorsel(size_t morsel_size, const ExecutionContext context) override;
    void finalize(const ExecutionContext context) override;
    double getExpectedTimePerUnit() const override;
    size_t getMinMorselSize() const override;

private:
    std::shared_ptr<PipelineStarterBase> starter;
    std::atomic<size_t> next_row[MAX_NUMA_NODES];
    size_t last_row[MAX_NUMA_NODES];
};