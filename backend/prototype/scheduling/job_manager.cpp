#include "job_manager.hpp"

#include <iostream>
#include <numa.h>

#include "execution_context.hpp"

#ifdef VTUNE_PROFILING
#include <ittnotify.h>
#endif

void JobManager::workerThread(const int tid, const ExecutionContext context) {
#ifdef VTUNE_PROFILING
    const static char* name_base = "Worker    ";
    char name[11];
    memcpy(name, name_base, 11);
    name[7] = '0' + static_cast<char>((tid / 100) % 10);
    name[8] = '0' + static_cast<char>((tid / 10) % 10);
    name[9] = '0' + static_cast<char>(tid % 10);
    __itt_thread_set_name(name);
#endif
    // set CPU affinity
    cpu_set_t mask;
    CPU_ZERO(&mask);
    CPU_SET(tid, &mask);
    if (sched_setaffinity(0, sizeof(cpu_set_t), &mask) == -1) {
        std::cerr << "Warning: Failed to set CPU affinity for worker thread " << tid << std::endl;
    }

    while (!context.getDispatcher().stop) {
        context.getDispatcher().runNext(context);
    }
}

JobManager::JobManager(uint64_t num_threads) {
    // start worker threads
    const uint64_t num_available_threads = static_cast<uint64_t>(numa_num_task_cpus());
    if (num_threads == 0 || num_threads > num_available_threads) {
        num_threads = num_available_threads;
    }
    int t = 0;
    uint32_t worker_id = 0;
    dispatcher = std::make_unique<Dispatcher>(num_threads);
    for (size_t i = 0; i < num_threads; i++) {
        while (numa_bitmask_isbitset(numa_all_cpus_ptr, t) != 1)
            t++;
        threads.emplace_back(workerThread, t, ExecutionContext(*this, numa_node_of_cpu(t), worker_id++));
        t++;
    }
    std::cout << "Using " << num_threads << " threads" << std::endl;
}

void JobManager::stop() {
    // stop worker threads
    dispatcher->stopAll();
    for (auto& thread : threads) {
        thread.join();
    }
}