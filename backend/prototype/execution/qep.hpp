#pragma once

#include <bitset>
#include <mutex>
#include <vector>

#include "../scheduling/execution_context.hpp"
#include "../storage/vmcache.hpp"
#include "../utils/stringify.hpp"
#include "pipeline.hpp"

#define MAX_PIPELINE_COUNT 64ul

class QEP {
public:
    QEP(std::vector<std::unique_ptr<ExecutablePipeline>>&& pipelines)
        : pipelines(std::move(pipelines))
        , completed_pipelines()
        , executing_pipelines()
        , finished(false)
    {
        if (this->pipelines.size() > MAX_PIPELINE_COUNT)
            throw std::runtime_error("More than " stringify(MAX_PIPELINE_COUNT) " pipelines are currently not supported in a single QEP!");
        if (this->pipelines.empty())
            throw std::runtime_error("Invalid QEP configuration!");
        for (size_t i = 0; i < this->pipelines.size(); i++) {
            if (this->pipelines[i]->getId() != i)
                throw std::runtime_error("Pipeline has invalid id!");
        }
    }

    void begin(const ExecutionContext context);
    void pipelineFinished(size_t id, const ExecutionContext context);

    bool isFinished() const { return finished; }
    void waitForExecution(const ExecutionContext context, const VMCache& vmcache, bool print_status = false) const;

    std::shared_ptr<PipelineBreakerBase> getResult() {
        return pipelines.back()->getBreaker();
    }

private:
    std::vector<std::unique_ptr<ExecutablePipeline>> pipelines;
    std::bitset<MAX_PIPELINE_COUNT> completed_pipelines;
    std::bitset<MAX_PIPELINE_COUNT> executing_pipelines;
    std::mutex sched_mutex;
    bool finished;
};