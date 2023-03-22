#pragma once

#include "../schema.hpp"
#include "prototype/core/db.hpp"
#include "prototype/core/types.hpp"
#include "prototype/execution/pipeline_starter.hpp"
#include "prototype/execution/paged_vector_iterator.hpp"
#include "prototype/execution/scan.hpp"

class Q09StockScanOperator : public PipelineStarterBase {
public:
    Q09StockScanOperator(DB& db, const std::vector<NamedColumn>&& output_columns, const ExecutionContext context) : db(db), output_columns(output_columns), iterators(context.getWorkerCount()) {
        uint64_t basepage_pid = db.getTableBasepageId("STOCK", context.getWorkerId());
        TableBasepage* basepage = reinterpret_cast<TableBasepage*>(db.vmcache.fixShared(basepage_pid, context.getWorkerId()));
        input_size = basepage->cardinality;

        basepages.push_back(basepage->column_basepages[S_W_ID_CID]);
        basepages.push_back(basepage->column_basepages[S_I_ID_CID]);
        db.vmcache.unfixShared(basepage_pid);

        for (auto& worker_iterators : iterators)
            worker_iterators.reserve(basepages.size());
        row_size = 0;
        for (const NamedColumn& col : output_columns)
            row_size += col.column->getValueTypeSize();
    }

    void execute(size_t from, size_t to, uint32_t worker_id) override {
        std::vector<GeneralPagedVectorIterator>& worker_iterators = iterators[worker_id];
        worker_iterators.clear();
        for (size_t i = 0; i < basepages.size(); i++) {
            const size_t type_size = sizeof(uint32_t); // both S_W_ID and S_I_ID are identifier columns
            worker_iterators.emplace_back(db.vmcache, basepages[i], from, type_size, worker_id);
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
            uint32_t w_id = *reinterpret_cast<uint32_t*>(worker_iterators[0].getCurrentValue());
            uint32_t i_id = *reinterpret_cast<uint32_t*>(worker_iterators[1].getCurrentValue());
            *reinterpret_cast<uint32_t*>(loc) = (w_id * i_id) % 10000;
            for (size_t j = 1; j < output_columns.size(); j++) {
                loc += sizeof(uint32_t);
                *reinterpret_cast<uint32_t*>(loc) = (output_columns[j].name == "S_W_ID") ? w_id : i_id;
            }
            for (size_t j = 0; j < worker_iterators.size(); j++) // advance all iterators
                ++worker_iterators[j];
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
    size_t row_size;

    std::vector<std::vector<GeneralPagedVectorIterator>> iterators;
};