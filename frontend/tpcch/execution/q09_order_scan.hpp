#pragma once

#include "prototype/core/db.hpp"
#include "prototype/storage/persistence/table.hpp"
#include "prototype/utils/memcpy.hpp"
#include "prototype/execution/pipeline_starter.hpp"
#include "prototype/execution/paged_vector_iterator.hpp"

// specialised scan operator for "ORDER" that extracts the year from "O_ENTRY_D"
class Q09OrderScanOperator : public PipelineStarterBase {
public:
    Q09OrderScanOperator(DB& db, const std::vector<uint64_t>& output_column_cids, const std::vector<NamedColumn>&& output_columns, const ExecutionContext context) : db(db), output_columns(output_columns), iterators(context.getWorkerCount()) {
        if (output_column_cids.size() != output_columns.size())
            throw std::runtime_error("Scan output specification does not match the specified column ids!");
        uint64_t basepage_pid = db.getTableBasepageId("ORDER", context.getWorkerId());
        TableBasepage* basepage = reinterpret_cast<TableBasepage*>(db.vmcache.fixShared(basepage_pid, context.getWorkerId()));
        input_size = basepage->cardinality;
        for (const uint64_t cid : output_column_cids)
            basepages.push_back(basepage->column_basepages[cid]);
        for (auto& worker_iterators : iterators)
            worker_iterators.reserve(output_column_cids.size());
        db.vmcache.unfixShared(basepage_pid);
        row_size = 0;
        output_sizes.reserve(output_columns.size());
        size_t i = 0;
        bool l_year_in_output = false;
        for (const NamedColumn& col : output_columns) {
            output_sizes.push_back(col.column->getValueTypeSize());
            row_size += output_sizes.back();
            if (col.name == "L_YEAR") {
                l_year_basepage_id = i;
                l_year_in_output = true;
            }
            i++;
        }
        if (!l_year_in_output)
            throw std::runtime_error("Specialised Q09OrderScanOperator constructed without \"L_YEAR\" in the output specification!");
    }

    void execute(size_t from, size_t to, uint32_t worker_id) override {
        std::vector<GeneralPagedVectorIterator>& worker_iterators = iterators[worker_id];
        worker_iterators.clear();
        for (size_t i = 0; i < basepages.size(); i++) {
            worker_iterators.emplace_back(db.vmcache, basepages[i], from, i == l_year_basepage_id ? sizeof(DateTime) : output_sizes[i], worker_id);
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
                if (j == l_year_basepage_id) { // extract(year from O_ENTRY_D) as L_YEAR
                    *reinterpret_cast<uint32_t*>(loc) = reinterpret_cast<const DateTime*>(val_ptr)->getYear();
                } else {
                    fast_memcpy(loc, val_ptr, sz);
                }
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
    size_t l_year_basepage_id;

    std::vector<std::vector<GeneralPagedVectorIterator>> iterators;
};