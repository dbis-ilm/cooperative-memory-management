#include "dispatcher.hpp"

#include <algorithm>
#include <cassert>
#include <iomanip>
#include <numa.h>

#include "../execution/qep.hpp"
#include "execution_context.hpp"
#include "job.hpp"

void Dispatcher::notifyAll() {
    std::unique_lock<std::mutex> lock(job_wait_mutex);
    job_wait.notify_all();
}

void Dispatcher::stopAll() {
    stop = true;
    notifyAll();
}

void Dispatcher::scheduleJob(const std::shared_ptr<Job>& job, const ExecutionContext context) {
    assert(job.get());
    const size_t job_size = job->getSize();
    const double expected_time = job->getExpectedTimePerUnit() * job_size;
    if (expected_time <= T_MAX || job_size <= job->getMinMorselSize()) {
        // if the job is small, execute it immediately within the calling thread
        // NOTE: we still may need to execute on multiple morsels as the job may have been split up across sockets
        while (job->executeNextMorsel(job_size, context)) { }
        job->finalize(context);
    } else {
        // schedule the job for execution using multiple worker threads
        size_t s = 0;
        while (true) {
            Job* slot = jobs[s].load();
            if ((uint64_t)slot == SLOT_TAG_EMPTY) {
                if (jobs[s].compare_exchange_weak(slot, job.get())) {
                    for (auto& worker_state : worker_states)
                        worker_state.change_mask[s / 64].fetch_or(1ull << (s % 64));
                    notifyAll();
                    break;
                }
            }
            s = (s + 1) % JOB_SLOTS;
        }
    }
}

void Dispatcher::finalizeSlot(size_t slot, const ExecutionContext context) {
    Job* job = SLOT_PTR(jobs[slot].load());
    jobs[slot].store((Job*)SLOT_TAG_EMPTY);
    job->finalize(context);
}

void Dispatcher::runNext(const ExecutionContext context, bool no_wait) {
    if (!context.isCreatedByJobManager())
        return;
    WorkerState& state = worker_states[context.getWorkerId()];
    // update thread-local active slots
    for (size_t i = 0; i < JOB_SLOTS / 64; i++) {
        uint64_t changes = state.change_mask[i].exchange(0);
        size_t offset = 0;
        while (changes > 0) {
            size_t trailing_zeroes = __builtin_ctzl(changes);
            offset += trailing_zeroes;
            assert(offset < 64);
            size_t s = i * 64 + offset;
            assert(s < JOB_SLOTS);
            // load the global slot, initialize priority and pass value
            Job* slot = jobs[s].load();
            if (SLOT_TAG(slot) == 0) { // ensure the slot is still active
                assert(slot);
                state.pass_values[s] = state.global_pass;
                state.priorities[s] = slot->getPriority();
                state.sum_priorities += state.priorities[s];
                state.T[s] = 1.0 / slot->getExpectedTimePerUnit(); // TODO: perhaps change this to find morsel size autonomously using a "startup phase" as described in the "Self-tuning query scheduling ..." paper
                state.active_slots.set(s);
            }
            offset += 1;
            // note: we shift two times separately here to avoid UB when trailing_zeroes == 63, as
            //  "[...] if the value of the right operand is negative or is greater or equal to the number of bits in the promoted left operand, the behavior is undefined." - https://en.cppreference.com/w/cpp/language/operator_arithmetic
            changes = changes >> trailing_zeroes;
            changes = changes >> 1;
        }
    }

    // select a job slot to execute the next morsel
    size_t min_pass_slot = JOB_SLOTS + 1;
    double min_pass = std::numeric_limits<double>::max();
    for (size_t s = 0; s < JOB_SLOTS; s++) {
        if (!state.active_slots.test(s))
            continue;
        const double pass_value = state.pass_values[s];
        if (pass_value < min_pass) {
            min_pass_slot = s;
            min_pass = pass_value;
        }
    }

    if (min_pass_slot != JOB_SLOTS + 1) {
        Job* slot = jobs[min_pass_slot].load();
        if (SLOT_TAG(slot) == 0) {
            auto begin = std::chrono::steady_clock::now();
            state.current_slot.store(min_pass_slot);
            Job* job = SLOT_PTR(slot);
            double throughput = state.T[min_pass_slot];
            size_t morsel_size = std::max(static_cast<size_t>(throughput * T_MAX), static_cast<size_t>(job->getMinMorselSize()));
            // TODO: implement a "shutdown phase" to try to achieve a "photo finish"?
            if (!job->executeNextMorsel(morsel_size, context)) {
                // no more morsels to process, finalize the job
                state.active_slots.set(min_pass_slot, false);
                state.sum_priorities -= state.priorities[min_pass_slot];
                auto prev_slot = state.current_slot.exchange(JOB_SLOTS + 1);
                int16_t finalizing_workers = std::numeric_limits<int16_t>::max();
                if (prev_slot == FINALIZATION_MARKER) {
                    // this worker is not the finalization coordinator, but needs to deregister
                    finalizing_workers = -1;
                } else if (jobs[min_pass_slot].compare_exchange_strong(slot, (Job*)((uint64_t)slot | SLOT_TAG_INACTIVE))) {
                    // this worker is now the "finalization coordinator", registering workers that are still running the job
                    finalizing_workers = 0;
                    for (auto& worker_state : worker_states) {
                        auto worker_slot = worker_state.current_slot.load();
                        if (worker_slot == min_pass_slot) {
                            if (worker_state.current_slot.compare_exchange_strong(worker_slot, FINALIZATION_MARKER))
                                finalizing_workers++;
                        }
                    }
                }
                if (finalizing_workers != std::numeric_limits<int16_t>::max() && job->finalization_counter.fetch_add(finalizing_workers) + finalizing_workers == 0) {
                    finalizeSlot(min_pass_slot, context);
                }
            } else {
                auto end = std::chrono::steady_clock::now();
                double et_secs = std::chrono::duration_cast<std::chrono::duration<double>>(end - begin).count();
                // update scheduling state
                state.pass_values[min_pass_slot] += et_secs / state.priorities[min_pass_slot];
                state.global_pass += et_secs / state.sum_priorities;
                // adjust througput estimate
                double measured_throughput = static_cast<double>(morsel_size) / et_secs;
                state.T[min_pass_slot] = std::min(std::max(ALPHA * measured_throughput + (1.0 - ALPHA) * throughput, throughput * 0.5), throughput * 1.5);
                // check for job finalization
                auto prev_slot = state.current_slot.exchange(JOB_SLOTS + 1);
                if (prev_slot == FINALIZATION_MARKER) {
                    state.active_slots.set(min_pass_slot, false);
                    state.sum_priorities -= state.priorities[min_pass_slot];
                    // slot is currently being finalized, we need to deregister
                    if (job->finalization_counter.fetch_sub(1) - 1 == 0) {
                        // we were the last worker, finalize the slot
                        finalizeSlot(min_pass_slot, context);
                    }
                }
            }
        } else {
            state.active_slots.set(min_pass_slot, false);
            state.sum_priorities -= state.priorities[min_pass_slot];
        }
    } else if (!stop && !no_wait) {
        bool need_more_maintenance = context.getVMCache().performIdleMaintenance(context.getWorkerId());
        if (!need_more_maintenance) {
            // currently there are no active jobs, wait until new ones are scheduled/until we should stop
            std::unique_lock<std::mutex> lock(job_wait_mutex);
            job_wait.wait_for(lock, std::chrono::milliseconds(1));
        }
    }
}

void Dispatcher::printJobStatus() const {
    std::cout << "Active jobs:" << std::endl;
    for (size_t i = 0; i < JOB_SLOTS; i++) {
        const Job* slot = jobs[i].load();
        if ((uint64_t)slot == SLOT_TAG_EMPTY)
            continue;
        const Job* job = SLOT_PTR(job);
        std::cout << "Slot " << i << ": " << std::setw(8) << job->getSize() << " tuples ";
        if (SLOT_TAG(job) == SLOT_TAG_INACTIVE)
            std::cout << " (inactive, " << job->finalization_counter.load() << " finalizing)";
        std::cout << std::endl;
    }
}