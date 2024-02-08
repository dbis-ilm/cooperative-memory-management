#pragma once

#include <cassert>
#include <cstring>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "../storage/policy/basic_partitioning_strategy.hpp"
#include "../storage/guard.hpp"
#include "../storage/vmcache.hpp"

#define ROOT_PID 0

class ExecutionContext;
struct TableBasepage;

class DB {
    friend class ColumnHelper;

public:
    DB(size_t memory_limit, const std::string& path, bool sandbox, bool no_dirty_writeback, bool flush_asynchronously, bool use_eviction_target, const size_t num_workers, bool use_exmap, bool stats_on_shutdown, std::unique_ptr<PartitioningStrategy>&& partitioning_strategy, size_t max_size_in_pages = 4ull * 1024ull * 1024ull);
    DB(size_t memory_limit, const std::string& path, bool sandbox, bool no_dirty_writeback, bool flush_asynchronously, bool use_eviction_target, const size_t num_workers, bool use_exmap, bool stats_on_shutdown = false, size_t max_size_in_pages = 4ull * 1024ull * 1024ull);

    uint64_t createSchema(const std::string& schema_name, uint32_t worker_id);
    uint64_t createTable(uint64_t schema_id, const std::string& table_name, size_t num_columns, uint32_t worker_id);
    // creates a B+-tree index on the first 'num_columns' columns of the table identified by 'tid';
    // it is assumed that these columns are 32-bit key columns
    // NOTE/TODO: in the current version, indices are not updated automatically if tuples are added to the table, only data already in the table at creation time will be indexed
    void createPrimaryKeyIndex(const std::string& table_name, size_t num_columns, const ExecutionContext context);
    template <typename T>
    void appendValues(size_t existing_rows, PageId column_base, typename std::vector<T>::iterator begin, typename std::vector<T>::iterator end, uint32_t worker_id);
    void appendFixedSizeValue(size_t existing_rows, PageId column_base, const void* value, size_t len, uint32_t worker_id);
    void appendFixedSizeValues(size_t existing_rows, PageId column_base, const void* values, size_t value_len, size_t num_values, uint32_t worker_id);
    size_t getNumTables(uint32_t worker_id);
    PageId getTableBasepageId(uint64_t tid, uint32_t worker_id);
    PageId getTableBasepageId(const std::string& table_name, uint32_t worker_id);

    VMCache vmcache;
    uint64_t default_schema_id;

private:
    PageId createTableInternal(size_t num_columns, uint32_t worker_id);
    std::mutex append_pids_mutex;
    std::unordered_map<PageId, PageId> append_pids;
};