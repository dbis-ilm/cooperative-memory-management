#pragma once

#include "../core/db.hpp"
#include "job_manager.hpp"

class ExecutionContext {
public:
    ExecutionContext(JobManager& job_manager, DB& db, uint32_t socket, uint32_t worker_id, bool created_by_job_manager = true)
        : job_manager(job_manager)
        , db(db)
        , socket(socket)
        , worker_id(worker_id)
        , created_by_job_manager(created_by_job_manager) { }

    JobManager& getJobManager() const { return job_manager; }
    Dispatcher& getDispatcher() const { return job_manager.getDispatcher(); }
    DB& getDB() const { return db; }
    VMCache& getVMCache() const { return db.vmcache; }
    size_t getWorkerCount() const { return job_manager.getWorkerCount() + 1; } // 1 additional worker for the main thread
    uint32_t getSocket() const { return socket; }
    uint32_t getWorkerId() const { return worker_id; }
    bool isCreatedByJobManager() const { return created_by_job_manager; }

private:
    JobManager& job_manager;
    DB& db;
    const uint32_t socket;
    const uint32_t worker_id;
    const bool created_by_job_manager;
};