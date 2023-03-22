#include "qep.hpp"

#include <algorithm>
#include <iostream>
#include <thread>

void QEP::begin(const ExecutionContext context) {
    std::bitset<MAX_PIPELINE_COUNT> pipelines_to_execute;
    {
        std::lock_guard<std::mutex> guard(sched_mutex);
        for (size_t i = 0; i < pipelines.size(); i++) {
            if (pipelines[i]->getDependencies().empty()) {
                pipelines_to_execute.set(i);
                executing_pipelines.set(i);
            }
        }
    }
    for (size_t i = 0; i < MAX_PIPELINE_COUNT; i++) {
        if (pipelines_to_execute.test(i))
            pipelines[i]->startExecution(this, context);
    }
}

void QEP::pipelineFinished(size_t id, const ExecutionContext context) {
    std::bitset<MAX_PIPELINE_COUNT> pipelines_to_execute;
    {
        std::lock_guard<std::mutex> guard(sched_mutex);
        completed_pipelines.set(id);
        if (id != pipelines.size() - 1)
            pipelines[id] = nullptr;
        if (completed_pipelines.count() == pipelines.size()) {
            // done
            finished = true;
        } else {
            // schedule next pipeline(s)
            for (size_t i = 0; i < pipelines.size(); i++) {
                // 1. check if the pipeline has executed / is executing already
                if (completed_pipelines.test(i) || executing_pipelines.test(i))
                    continue;
                bool ready = true;
                // 2. check if the pipeline depends on any pipelines that have not executed yet
                for (const size_t dep : pipelines[i]->getDependencies()) {
                    if (!completed_pipelines.test(dep)) {
                        ready = false;
                        break;
                    }
                }
                // 3. execute pipeline if not executed yet and ready
                if (ready) {
                    pipelines_to_execute.set(i);
                    executing_pipelines.set(i);
                }
            }
        }
    }
    for (size_t i = 0; i < MAX_PIPELINE_COUNT; i++)
        if (pipelines_to_execute.test(i))
            pipelines[i]->startExecution(this, context);
}

void QEP::waitForExecution(const ExecutionContext context, const VMCache& vmcache, bool print_status) const {
    const auto status_frequency = std::chrono::seconds(1);
    auto last_status_time = std::chrono::steady_clock::now();
    while (!finished) {
        std::this_thread::sleep_for(std::chrono::microseconds(10));
        auto now = std::chrono::steady_clock::now();
        if (print_status && (now - last_status_time) > status_frequency) {
            context.getDispatcher().printJobStatus();
            last_status_time = now;
            vmcache.printMemoryUsage();
        }
    };
}