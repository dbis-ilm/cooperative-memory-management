#pragma once

#include <cassert>
#include <iomanip>
#include <iostream>

#include "prototype/core/db.hpp"
#include "prototype/execution/pipeline_breaker.hpp"

class Q09AggregationOperator : public PipelineBreakerBase {
public:
    Q09AggregationOperator(DB& db, BatchDescription& batch_description) : PipelineBreakerBase(batch_description), db(db), result(0) { }

    void push(std::shared_ptr<Batch> batch, uint32_t) override {
        int64_t sum_profit = 0;
        for (uint32_t i = 0; i < batch->getCurrentSize(); i++) {
            if (batch->isRowValid(i)) {
                int64_t* loc = reinterpret_cast<int64_t*>(batch->getRow(i));
                const int64_t l_extendedprice = loc[0];
                const int64_t l_discount = loc[1];
                const int64_t ps_supplycost = loc[2];
                const int32_t l_quantity = *reinterpret_cast<int32_t*>(reinterpret_cast<char*>(loc) + 24);
                sum_profit += l_extendedprice * (100 - l_discount) - ps_supplycost * l_quantity;
            }
        }
        auto res = result.load();
        while (!result.compare_exchange_weak(res, res + sum_profit)) {}
    }

    void consumeBatches(std::vector<std::shared_ptr<Batch>>& target, uint32_t worker_id) {
        // create result batch
        std::shared_ptr<Batch> result_batch = std::make_shared<Batch>(db.vmcache, sizeof(uint64_t), worker_id);
        uint32_t row_id = 0;
        uint64_t* result_dest = reinterpret_cast<uint64_t*>(result_batch->addRowIfPossible(row_id));
        assert(result_dest != nullptr);
        *result_dest = static_cast<uint64_t>(result);

        target.push_back(result_batch);
    }

private:
    DB& db;
    std::atomic_int64_t result;
};