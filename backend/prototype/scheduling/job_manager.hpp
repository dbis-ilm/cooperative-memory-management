#pragma once

#include <thread>

#include "dispatcher.hpp"

class ExecutionContext;

class JobManager {
public:
    JobManager(uint64_t num_threads);

    void stop();
    Dispatcher& getDispatcher() { return *dispatcher; }
    size_t getWorkerCount() const { return threads.size(); }

private:
    static void workerThread(const int tid, const ExecutionContext context);

    std::unique_ptr<Dispatcher> dispatcher;
    std::vector<std::thread> threads;
};