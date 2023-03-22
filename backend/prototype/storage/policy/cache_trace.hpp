#include <fcntl.h>
#include <stdint.h>

#include "../../core/units.hpp"

#define THREAD_LOCAL_TRACE_SIZE 512

enum class CacheAction : uint8_t {
    Evict = 1,
    Fault = 2,
    Ref = 3 // note: for now we do not actually trace page references, but this may be implemented later on
};

struct CacheTraceEntry {
    double timestamp;
    uint64_t action_pid;

    CacheTraceEntry(double timestamp, CacheAction action, PageId pid) : timestamp(timestamp) {
        action_pid = pid | static_cast<uint64_t>(action) << 56ull;
    }

    PageId getPid() const { return action_pid & 0x00ffffffffffffffull; }

    CacheAction getAction() const { return static_cast<CacheAction>(action_pid >> 56ull); }
};

class CacheTracer {
public:
    CacheTracer(const size_t num_workers)
    : page_traces(num_workers)
    , begin(std::chrono::system_clock::now())
    , trace_offset(0)
    , trace_fd(open("cache.trc", O_CREAT | O_WRONLY | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)) {
        for (auto& trace : page_traces)
            trace.reserve(THREAD_LOCAL_TRACE_SIZE);
        if (trace_fd == -1)
            throw std::runtime_error("Failed to open cache trace file");
    }

    void trace(const CacheAction action, const PageId pid, uint32_t worker_id) {
        double timestamp = std::chrono::duration_cast<std::chrono::duration<double>>(std::chrono::system_clock::now() - begin).count();
        auto& trace = page_traces[worker_id];
        trace.emplace_back(timestamp, action, pid);
        if (trace.size() == THREAD_LOCAL_TRACE_SIZE) {
            const size_t sz = THREAD_LOCAL_TRACE_SIZE * sizeof(CacheTraceEntry);
            size_t offset = trace_offset.fetch_add(sz, std::memory_order_relaxed);
            pwrite(trace_fd, trace.data(), sz, offset);
            trace.clear();
        }
    }

private:
    std::vector<std::vector<CacheTraceEntry>> page_traces;
    std::chrono::time_point<std::chrono::system_clock> begin;
    std::atomic_size_t trace_offset;
    int trace_fd;
};