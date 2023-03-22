#pragma once

#include <cassert>
#include <cstring>
#include <mutex>
#include <vector>

#include "../storage/policy/basic_partitioning_strategy.hpp"
#include "../storage/vmcache.hpp"

#define ROOT_PID 0

class ExecutionContext;

class DB {
public:
    DB(size_t memory_limit, int fd, bool create, const size_t num_workers, bool stats_on_shutdown, std::unique_ptr<PartitioningStrategy>&& partitioning_strategy, size_t max_size_in_pages = 4ull * 1024ull * 1024ull);
    DB(size_t memory_limit, int fd, bool create, const size_t num_workers, bool stats_on_shutdown = false, size_t max_size_in_pages = 4ull * 1024ull * 1024ull);

    uint64_t createSchema(const std::string& schema_name, uint32_t worker_id);
    uint64_t createTable(uint64_t schema_id, const std::string& table_name, size_t num_columns, uint32_t worker_id);
    // creates a B+-tree index on the first 'num_columns' columns of the table identified by 'tid';
    // it is assumed that these columns are 32-bit key columns
    // NOTE/TODO: in the current version, indices are not updated automatically if tuples are added to the table, only data already in the table at creation time will be indexed
    void createPrimaryKeyIndex(uint64_t tid, size_t num_columns, const ExecutionContext context);
    template <typename T>
    void appendValues(size_t existing_rows, PageId column_base, typename std::vector<T>::iterator begin, typename std::vector<T>::iterator end, uint32_t worker_id);
    void appendFixedSizeValue(size_t existing_rows, PageId column_base, void* value, size_t len, uint32_t worker_id);
    void appendFixedSizeValues(size_t existing_rows, PageId column_base, void* values, size_t value_len, size_t num_values, uint32_t worker_id);
    PageId getTableBasepageId(uint64_t tid, uint32_t worker_id);
    PageId getTableBasepageId(const std::string& table_name, uint32_t worker_id);

    VMCache vmcache;
    uint64_t default_schema_id;
    std::mutex write_lock;

private:
    PageId createTableInternal(size_t num_columns, uint32_t worker_id);
};