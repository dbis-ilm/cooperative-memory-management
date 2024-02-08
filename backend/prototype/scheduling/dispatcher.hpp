#pragma once

#include <atomic>
#include <bitset>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <vector>

class ExecutionContext;
class Job;

const size_t JOB_SLOTS = 128; // TODO: need to figure out a way to avoid deadlocks in QEP::pipelineFinished() when all slots are being used -> just have a queue behind a mutex to keep track of pipelines that exceed the available slots?!
const size_t MAX_NUMA_NODES = 8;

const double T_MAX = 0.002; // target execution time per morsel [seconds]
const double ALPHA = 0.8; // weight for adjusting throughput estimates

#define FINALIZATION_MARKER 0xffff
#define SLOT_TAG_INACTIVE (1ull << 57)
#define SLOT_TAG_EMPTY (2ull << 57)
#define SLOT_PTR_MASK ((1ull << 57) - 1ull)
#define SLOT_TAG_MASK (~SLOT_PTR_MASK)
#define SLOT_PTR(slot) (Job*)((uint64_t)slot & SLOT_PTR_MASK)
#define SLOT_TAG(slot) ((uint64_t)slot & SLOT_TAG_MASK)

struct WorkerState {
    WorkerState()
        : active_slots(0)
        , priorities {}
        , pass_values {}
        , sum_priorities(0)
        , global_pass(0)
        , change_mask {}
        //, return_mask {}
        , current_slot(JOB_SLOTS + 1)
        , T {}
    { }

    // for thread-local (stride) scheduling
    std::bitset<JOB_SLOTS> active_slots;
    double priorities[JOB_SLOTS];
    double pass_values[JOB_SLOTS]; // pass_values[i] is incremented by priorities[i]^-1 every time a morsel/time slice from slot i is executed by the worker
    double sum_priorities;
    double global_pass;
    std::atomic_uint64_t change_mask[JOB_SLOTS / 64];
    //std::atomic_uint64_t return_mask[JOB_SLOTS / 64];
    std::atomic_uint16_t current_slot; // in the "Self-tuning query scheduling ..." paper, this is referred to as the "global state array"; it is used for pipeline finalization, i.e., when all morsels have already started execution but we need to wait for all of them to finish before scheduling the next pipeline

    // for adapting morsel sizes
    double T[JOB_SLOTS]; // throughput estimate for each job slot
};

struct Dispatcher {
    friend class JobManager;
    friend class QEP;

public:
    Dispatcher(uint64_t num_workers) : stop(false), worker_states(num_workers) {
        for (auto& slot : jobs)
            slot = (Job*)SLOT_TAG_EMPTY;
    }

    // for job management
    void scheduleJob(const std::shared_ptr<Job>& job, const ExecutionContext context);
    void printJobStatus() const;

private:
    void stopAll();
    void notifyAll();
    void finalizeSlot(size_t slot, const ExecutionContext context);

    // for worker threads
    void runNext(const ExecutionContext context, bool no_wait = false);

    std::mutex job_wait_mutex;
    std::condition_variable job_wait;
    std::atomic<bool> stop;
    std::atomic<Job*> jobs[JOB_SLOTS];
    std::vector<WorkerState> worker_states;
};