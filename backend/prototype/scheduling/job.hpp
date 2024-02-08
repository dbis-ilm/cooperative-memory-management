#pragma once

#include <atomic>

class Job {
    friend class Dispatcher;

public:
    virtual size_t getSize() const = 0; // total size of the job
    virtual double getExpectedTimePerUnit() const = 0; // hint for initializing adaptive morsel size (expected time in seconds it takes to process one unit of this job)
    virtual size_t getMinMorselSize() const { return 1; } // minimum morsel size
    virtual double getPriority() { return 1.0; }

    virtual bool executeNextMorsel(size_t morsel_size, const ExecutionContext context) = 0;
    virtual void finalize(const ExecutionContext context) = 0;

private:
    std::atomic_int16_t finalization_counter { 0 };
};

// Tasks are long-running non-parallel jobs
class Task : public Job {
public:
    Task() : executing(false) { }

    virtual void execute(const ExecutionContext context) = 0;

    size_t getSize() const override { return 1; }
    // return infinity here to make sure that we are not scheduled for immediate execution when calling Dispatcher::scheduleJob() (as this would block the calling thread indefinitely)
    double getExpectedTimePerUnit() const override { return std::numeric_limits<double>::infinity(); }
    size_t getMinMorselSize() const override { return 0; }
    double getPriority() override { return 10.0; }

    bool executeNextMorsel(size_t, const ExecutionContext context) override {
        bool exec = false;
        if (executing.compare_exchange_strong(exec, true))
            execute(context);
        return false;
    };
    void finalize(const ExecutionContext) override {};

private:
    std::atomic_bool executing;
};

class FunctionTask : public Task {
public:
    FunctionTask(std::function<void(const ExecutionContext)> func) : func(func) {}

    void execute(const ExecutionContext context) override { func(context); }

private:
    std::function<void(const ExecutionContext)> func;
};