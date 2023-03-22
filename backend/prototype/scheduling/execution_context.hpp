#pragma once

#include "../core/db.hpp"
#include "job_manager.hpp"

class ExecutionContext {
public:
    ExecutionContext(JobManager& job_manager, uint32_t socket, uint32_t worker_id)
        : job_manager(job_manager)
        , socket(socket)
        , worker_id(worker_id) { }

    JobManager& getJobManager() const { return job_manager; }
    Dispatcher& getDispatcher() const { return job_manager.getDispatcher(); }
    size_t getWorkerCount() const { return job_manager.getWorkerCount(); }
    uint32_t getSocket() const { return socket; }
    uint32_t getWorkerId() const { return worker_id; }

private:
    JobManager& job_manager;
    const uint32_t socket;
    const uint32_t worker_id;
};