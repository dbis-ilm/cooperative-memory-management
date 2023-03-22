#include "pipeline_starter.hpp"

#include <numa.h>

#ifdef VTUNE_PROFILING
#include <ittnotify.h>

static __itt_domain* itt_domain = __itt_domain_create("prototype");
#define STRING_HANDLES(pid) \
    __itt_string_handle_create("pipeline " pid " morsel (socket 0)"), \
    __itt_string_handle_create("pipeline " pid " morsel (socket 1)"), \
    __itt_string_handle_create("pipeline " pid " morsel (socket 2)"), \
    __itt_string_handle_create("pipeline " pid " morsel (socket 3)"),
static __itt_string_handle* itt_handle_morsel[] = {
    STRING_HANDLES("0")
    STRING_HANDLES("1")
    STRING_HANDLES("2")
    STRING_HANDLES("3")
    STRING_HANDLES("4")
    STRING_HANDLES("5")
    STRING_HANDLES("6")
    STRING_HANDLES("7")
    STRING_HANDLES("8")
    STRING_HANDLES("9")
    STRING_HANDLES("10")
    STRING_HANDLES("11")
    STRING_HANDLES("12")
    STRING_HANDLES("13")
    STRING_HANDLES("14")
    STRING_HANDLES("15")
    STRING_HANDLES("16")
    STRING_HANDLES("17")
    STRING_HANDLES("18")
    STRING_HANDLES("19")
    STRING_HANDLES("19+")
};
#endif

void PipelineStarterBase::initializeMorsels() {
    const int numa_node_count = numa_num_configured_nodes();
    const int available_numa_node_count = numa_bitmask_weight(numa_all_nodes_ptr);
    assert(numa_node_count <= static_cast<int>(MAX_NUMA_NODES));
    const size_t row_count = getInputSize();
    const size_t rows_per_node = row_count / available_numa_node_count;
    int consecutive_node_id = 0;
    for (int i = 0; i < numa_node_count; i++) {
        if (numa_bitmask_isbitset(numa_all_nodes_ptr, i) != 1) {
            next_row[i] = 0;
            last_row[i] = 0;
        } else {
            next_row[i] = consecutive_node_id * rows_per_node;
            last_row[i] = (consecutive_node_id == available_numa_node_count - 1) ? row_count : (consecutive_node_id + 1) * rows_per_node;
            consecutive_node_id++;
        }
    }
}

bool PipelineStarterBase::executeNextMorsel(size_t morsel_size, size_t socket, uint32_t worker_id) {
    bool got_job = false;
    bool found_candidate = true;
    size_t m = 0;
    size_t socket_candidate;
    while (found_candidate) {
        found_candidate = false;
        uint8_t remaining_sockets = (0x1 << numa_num_configured_nodes()) - 1;
        socket_candidate = socket;
        while (remaining_sockets != 0) {
            remaining_sockets &= ~(0x1 << socket_candidate);
            m = next_row[socket_candidate].load();
            if (m >= last_row[socket_candidate]) { // this socket is done with the job
                socket_candidate = __builtin_ffs(remaining_sockets) - 1; // try stealing a morsel from another socket
                continue;
            }
            found_candidate = true;
            got_job = next_row[socket_candidate].compare_exchange_weak(m, m + morsel_size);
            break;
        }
        if (got_job)
            break;
    }

    if (got_job) {
        const size_t to = std::min(m + morsel_size, last_row[socket_candidate]);
#ifdef VTUNE_PROFILING
        if (getPipelineId() < 20) {
            __itt_task_begin(itt_domain, __itt_null, __itt_null, itt_handle_morsel[4 * getPipelineId() + socket]);
        } else {
            __itt_task_begin(itt_domain, __itt_null, __itt_null, itt_handle_morsel[4 * 20 + socket]);
        }
#endif
        execute(m, to, worker_id);
#ifdef VTUNE_PROFILING
        __itt_task_end(itt_domain);
#endif
        return true;
    }
    return false;
}