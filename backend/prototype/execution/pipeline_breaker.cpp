#include "pipeline_breaker.hpp"

PipelineBreakerBase::PipelineBreakerBase(BatchDescription& batch_description) {
    this->batch_description.swap(batch_description);
}

PipelineBreakerBase::~PipelineBreakerBase() { }

void PipelineBreakerBase::consumeBatchDescription(BatchDescription& target) {
    target.swap(batch_description);
}

PipelineStarterBreakerBase::PipelineStarterBreakerBase(BatchDescription& batch_description) : PipelineBreakerBase(batch_description) { }

PipelineStarterBreakerBase::~PipelineStarterBreakerBase() { }

DefaultBreaker::DefaultBreaker(BatchDescription& batch_description, size_t num_workers) : PipelineBreakerBase(batch_description), batches(num_workers), valid_row_count(0) { }

void DefaultBreaker::push(std::shared_ptr<Batch> batch, uint32_t worker_id) {
    if (batch->getRowSize() != batch_description.getRowSize())
        throw std::runtime_error("DefaultBreaker: Batch row size does not match batch_description");
    // just push batch to 'batches[worker_id]', tuples are not copied
    batches.at(worker_id).push_back(batch);
    valid_row_count += batch->getValidRowCount();
}

void DefaultBreaker::consumeBatches(std::vector<std::shared_ptr<Batch>>& target, uint32_t) {
    if (!target.empty()) {
        throw std::runtime_error("Target not empty");
    }

    size_t batch_count = 0;
    for (const std::vector<std::shared_ptr<Batch>>& worker_batches : batches) {
        batch_count += worker_batches.size();
    }
    target.reserve(batch_count);
    for (std::vector<std::shared_ptr<Batch>>& worker_batches : batches) {
        for (std::shared_ptr<Batch>& batch : worker_batches) {
            target.push_back(batch);
            batch = nullptr;
        }
    }
}

size_t DefaultBreaker::getValidRowCount() const {
    return valid_row_count.load();
}