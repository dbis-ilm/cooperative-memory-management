#pragma once

#include <cassert>
#include <iomanip>
#include <iostream>

#include "prototype/core/db.hpp"
#include "prototype/execution/pipeline_breaker.hpp"
#include "prototype/execution/temporary_column.hpp"

const size_t AGGREGATION_KEY_SIZE = 25 + 4;

// NOTE: The implementation assumes that 'push' is only called from a single worker thread at a time; this is achieved in practice by using this operator to break a pipeline that starts with a 'SortOperator'
class Q09AggregationOperator : public PipelineBreakerBase {
public:
    Q09AggregationOperator(DB& db, BatchDescription& batch_description) : PipelineBreakerBase(batch_description), db(db) { }

    void push(std::shared_ptr<Batch> batch, uint32_t worker_id) override {
        assert(batch->getRowSize() == AGGREGATION_KEY_SIZE + sizeof(Decimal<2>));
        if (batches.empty()) {
            batches.push_back(std::make_shared<Batch>(db.vmcache, batch->getRowSize(), worker_id));
        }

        for (uint32_t i = 0; i < batch->getCurrentSize(); i++) {
            if (batch->isRowValid(i)) {
                char* input_row = reinterpret_cast<char*>(batch->getRow(i));
                if (batches.back()->getCurrentSize() == 0 || memcmp(batches.back()->getLastRow(), input_row, AGGREGATION_KEY_SIZE) != 0) {
                    uint32_t row_id;
                    char* loc = reinterpret_cast<char*>(batches.back()->addRowIfPossible(row_id));
                    if (loc == nullptr) {
                        batches.push_back(std::make_shared<Batch>(db.vmcache, batch->getRowSize(), worker_id));
                        loc = reinterpret_cast<char*>(batches.back()->addRowIfPossible(row_id));
                    }
                    memcpy(loc, input_row, batch->getRowSize());
                } else {
                    *reinterpret_cast<uint64_t*>(reinterpret_cast<char*>(batches.back()->getLastRow()) + AGGREGATION_KEY_SIZE) += *reinterpret_cast<uint64_t*>(input_row + AGGREGATION_KEY_SIZE);
                }
            }
        }
    }

    void consumeBatches(std::vector<std::shared_ptr<Batch>>& target, uint32_t) {
        target.reserve(batches.size());
        for (std::shared_ptr<Batch>& batch : batches) {
            target.push_back(batch);
            batch = nullptr;
        }
    }

private:
    DB& db;
    std::vector<std::shared_ptr<Batch>> batches;
};