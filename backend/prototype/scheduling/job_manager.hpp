#pragma once

#include <thread>

#include "dispatcher.hpp"

class DB;
class ExecutionContext;

class JobManager {
public:
    JobManager(uint64_t num_threads, DB& db);

    void stop();
    Dispatcher& getDispatcher() { return *dispatcher; }
    size_t getWorkerCount() const { return threads.size(); }
    static uint64_t configureNumThreads(uint64_t num_threads);

private:
    static void workerThread(const int tid, const ExecutionContext context);

    std::unique_ptr<Dispatcher> dispatcher;
    std::vector<std::thread> threads;
};