#pragma once

#include "../core/db.hpp"
#include "../storage/persistence/table.hpp"
#include "../utils/memcpy.hpp"
#include "pipeline_starter.hpp"
#include "paged_vector_iterator.hpp"

const size_t SCAN_MORSEL_SIZE = 32 * 1024;

class ScanOperator : public PipelineStarterBase {
public:
    // TODO: support filtering
    ScanOperator(DB& db, const std::string& table_name, const std::vector<uint64_t>& output_column_cids, const std::vector<NamedColumn>&& output_columns, const ExecutionContext context) : db(db), output_columns(output_columns), iterators(context.getWorkerCount()) {
        if (output_column_cids.size() != output_columns.size())
            throw std::runtime_error("Scan output specification does not match the specified column ids!");
        uint64_t basepage_pid = db.getTableBasepageId(table_name, context.getWorkerId());
        TableBasepage* basepage = reinterpret_cast<TableBasepage*>(db.vmcache.fixShared(basepage_pid, context.getWorkerId()));
        input_size = basepage->cardinality;
        for (const uint64_t cid : output_column_cids)
            basepages.push_back(basepage->column_basepages[cid]);
        for (auto& worker_iterators : iterators)
            worker_iterators.reserve(output_column_cids.size());
        db.vmcache.unfixShared(basepage_pid);
        row_size = 0;
        output_sizes.reserve(output_columns.size());
        for (const NamedColumn& col : output_columns) {
            output_sizes.push_back(col.column->getValueTypeSize());
            row_size += output_sizes.back();
        }
    }

    void execute(size_t from, size_t to, uint32_t worker_id) override {
        std::vector<GeneralPagedVectorIterator>& worker_iterators = iterators[worker_id];
        worker_iterators.clear();
        for (size_t i = 0; i < basepages.size(); i++) {
            worker_iterators.emplace_back(db.vmcache, basepages[i], from, output_sizes[i], worker_id);
        }
        std::shared_ptr<Batch> intermediates = std::make_shared<Batch>(db.vmcache, row_size, worker_id);
        for (size_t i = from; i < to; i++) {
            uint32_t row_id;
            char* loc = reinterpret_cast<char*>(intermediates->addRowIfPossible(row_id));
            if (loc == nullptr) {
                next_operator->push(intermediates, worker_id);
                if (intermediates.use_count() > 1)
                    intermediates = std::make_shared<Batch>(db.vmcache, row_size, worker_id);
                else
                    intermediates->clear();
                loc = reinterpret_cast<char*>(intermediates->addRowIfPossible(row_id));
            }
            for (size_t j = 0; j < basepages.size(); j++) {
                const size_t sz = output_sizes[j];
                const char* val_ptr = reinterpret_cast<const char*>(worker_iterators[j].getCurrentValue());
                fast_memcpy(loc, val_ptr, sz);
                ++worker_iterators[j];
                loc += sz;
            }
        }
        worker_iterators.clear();
        // push the last batch
        if (intermediates->getCurrentSize() > 0)
            next_operator->push(intermediates, worker_id);
    }

    size_t getInputSize() const override { return input_size; }
    size_t getMorselSizeHint() const override { return SCAN_MORSEL_SIZE; }
    size_t getMinMorselSize() const override { return PAGE_SIZE / sizeof(uint32_t); }

private:
    DB& db;
    size_t input_size;
    std::vector<PageId> basepages;
    std::vector<NamedColumn> output_columns;
    std::vector<size_t> output_sizes;
    size_t row_size;

    std::vector<std::vector<GeneralPagedVectorIterator>> iterators;
};

class FilteringScanOperator : public PipelineStarterBase {
public:
    // TODO: support different filter data types
    FilteringScanOperator(DB& db, const std::string& table_name, const std::vector<uint64_t>& filter_column_cids, const std::vector<Identifier>&& filter_values, const std::vector<uint64_t> output_column_cids, const std::vector<NamedColumn>&& output_columns, const ExecutionContext context) : db(db), filter_values(filter_values), output_columns(output_columns), iterators(context.getWorkerCount()) {
        if (filter_column_cids.size() != filter_values.size())
            throw std::runtime_error("Scan filter specification does not match the specified column ids!");
        if (output_column_cids.size() != output_columns.size())
            throw std::runtime_error("Scan output specification does not match the specified column ids!");
        uint64_t basepage_pid = db.getTableBasepageId(table_name, context.getWorkerId());
        TableBasepage* basepage = reinterpret_cast<TableBasepage*>(db.vmcache.fixShared(basepage_pid, context.getWorkerId()));
        input_size = basepage->cardinality;
        for (const uint64_t cid : filter_column_cids)
            basepages.push_back(basepage->column_basepages[cid]);
        first_output_basepage = basepages.size();
        for (const uint64_t cid : output_column_cids)
            basepages.push_back(basepage->column_basepages[cid]);
        for (auto& worker_iterators : iterators)
            worker_iterators.reserve(basepages.size());
        db.vmcache.unfixShared(basepage_pid);
        row_size = 0;
        output_sizes.reserve(basepages.size());
        for (size_t i = 0; i < filter_values.size(); i++) {
            output_sizes.push_back(sizeof(uint32_t)); // TODO: support different filter value sizes
        }
        for (const NamedColumn& col : output_columns) {
            output_sizes.push_back(col.column->getValueTypeSize());
            row_size += output_sizes.back();
        }
    }

    void execute(size_t from, size_t to, uint32_t worker_id) override {
        std::vector<GeneralPagedVectorIterator>& worker_iterators = iterators[worker_id];
        worker_iterators.clear();
        for (size_t i = 0; i < basepages.size(); i++) {
            worker_iterators.emplace_back(db.vmcache, basepages[i], from, output_sizes[i], worker_id);
        }
        std::shared_ptr<Batch> intermediates = std::make_shared<Batch>(db.vmcache, row_size, worker_id);
        for (size_t i = from; i < to; i++) {
            bool match = true;
            for (size_t j = 0; j < first_output_basepage; j++) {
                match &= *reinterpret_cast<Identifier*>(worker_iterators[j].getCurrentValue()) == filter_values[j];
                ++worker_iterators[j];
            }

            if (match) {
                uint32_t row_id;
                char* loc = reinterpret_cast<char*>(intermediates->addRowIfPossible(row_id));
                if (loc == nullptr) {
                    next_operator->push(intermediates, worker_id);
                    if (intermediates.use_count() > 1)
                        intermediates = std::make_shared<Batch>(db.vmcache, row_size, worker_id);
                    else
                        intermediates->clear();
                    loc = reinterpret_cast<char*>(intermediates->addRowIfPossible(row_id));
                }
                for (size_t j = first_output_basepage; j < basepages.size(); j++) {
                    const size_t sz = output_sizes[j];
                    const char* val_ptr = reinterpret_cast<const char*>(worker_iterators[j].getCurrentValue());
                    fast_memcpy(loc, val_ptr, sz);
                    loc += sz;
                }
            }

            for (size_t j = first_output_basepage; j < basepages.size(); j++) {
                ++worker_iterators[j];
            }
        }
        worker_iterators.clear();
        // push the last batch
        if (intermediates->getCurrentSize() > 0)
            next_operator->push(intermediates, worker_id);
    }

    size_t getInputSize() const override { return input_size; }
    size_t getMorselSizeHint() const override { return SCAN_MORSEL_SIZE; }
    size_t getMinMorselSize() const override { return PAGE_SIZE / sizeof(uint32_t); }

private:
    DB& db;
    size_t input_size;
    std::vector<Identifier> filter_values;
    std::vector<PageId> basepages;
    size_t first_output_basepage;
    std::vector<NamedColumn> output_columns;
    std::vector<size_t> output_sizes;
    size_t row_size;

    std::vector<std::vector<GeneralPagedVectorIterator>> iterators;
};