#pragma once

#include <vector>

#include "print_result.hpp"
#include "../execution/batch.hpp"
#include "../execution/pipeline_breaker.hpp"

// helper utilities for validating result sets in testing

class BatchVector {
public:
    BatchVector(VMCache& vmcache, uint32_t row_size) : vmcache(vmcache), row_size(row_size) { }

    operator std::vector<std::shared_ptr<Batch>>&() { return batches; }

    void* addRow() {
        uint32_t row_id;
        void* result = batches.empty() ? nullptr : batches.back()->addRowIfPossible(row_id);
        if (result == nullptr) {
            batches.emplace_back(std::make_shared<Batch>(vmcache, row_size, 0));
            result = batches.back()->addRowIfPossible(row_id);
        }
        return result;
    }

private:
    VMCache& vmcache;
    const uint32_t row_size;
    std::vector<std::shared_ptr<Batch>> batches;
};

bool validateQueryResult(const std::shared_ptr<PipelineBreakerBase>& result, std::vector<std::shared_ptr<Batch>>& expected, bool match_order = false);