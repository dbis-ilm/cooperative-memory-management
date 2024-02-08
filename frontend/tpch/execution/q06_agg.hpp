#pragma once

#include <cassert>
#include <iomanip>
#include <iostream>

#include "prototype/execution/pipeline_breaker.hpp"

class Q06AggregationOperator : public PipelineBreakerBase {
public:
    Q06AggregationOperator(DB& db, BatchDescription& batch_description) : PipelineBreakerBase(batch_description), db(db), result(0) { }

    void push(std::shared_ptr<Batch> batch, uint32_t) override {
        uint64_t revenue = 0;
        for (uint32_t i = 0; i < batch->getCurrentSize(); i++) {
            if (batch->isRowValid(i)) {
                uint64_t* loc = reinterpret_cast<uint64_t*>(batch->getRow(i));
                revenue += loc[0] * loc[1];
            }
        }
        auto res = result.load();
        while (!result.compare_exchange_weak(res, res + revenue)) {}
    }

    void consumeBatches(std::vector<std::shared_ptr<Batch>>& target, uint32_t worker_id) {
        // create result batch
        std::shared_ptr<Batch> result_batch = std::make_shared<Batch>(db.vmcache, sizeof(uint64_t), worker_id);
        uint32_t row_id = 0;
        uint64_t* result_dest = reinterpret_cast<uint64_t*>(result_batch->addRowIfPossible(row_id));
        assert(result_dest != nullptr);
        *result_dest = result;

        target.push_back(result_batch);
    }

private:
    DB& db;
    std::atomic_uint64_t result;
};