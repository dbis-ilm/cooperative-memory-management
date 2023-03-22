#include "print_result.hpp"

#include <iomanip>

#include "../execution/pipeline_breaker.hpp"

void printResultRow(const char* row, const BatchDescription& description, std::ostream& out) {
    out << "| ";
    size_t offset = 0;
    for (auto& column : description.getColumns()) {
        out << *column.column->print(row + offset, 20) << " | ";
        offset += column.column->getValueTypeSize();
    }
    out << std::endl;
}

void printQueryResult(const std::vector<std::shared_ptr<Batch>>& batches, const BatchDescription& description, std::ostream& out) {
    int64_t row_limit = 10;
    out << "| ";
    for (auto& column : description.getColumns()) {
        out << std::setw(20) << std::setfill(' ') << column.name << " | ";
    }
    out << std::endl << "| ";
    for (size_t i = 0; i < description.getColumns().size(); i++) {
        out << std::setw(23) << std::setfill('-') << " | ";
    }
    out << std::endl;
    for (const std::shared_ptr<Batch>& batch : batches) {
        for (uint32_t row_id = 0; row_id < batch->getCurrentSize(); row_id++) {
            if (batch->isRowValid(row_id)) {
                if (row_limit > 0) {
                    const char* row = reinterpret_cast<const char*>(batch->getRow(row_id));
                    printResultRow(row, description, out);
                }
                row_limit--;
            }
        }
    }
    if (row_limit < 0)
        out << "| " << -row_limit << " additional rows ..." << std::endl;
}

void printQueryResult(const std::shared_ptr<PipelineBreakerBase>& breaker, uint32_t worker_id, std::ostream& out) {
    std::vector<std::shared_ptr<Batch>> batches;
    BatchDescription description;
    breaker->consumeBatches(batches, worker_id);
    breaker->consumeBatchDescription(description);
    // output results
    printQueryResult(batches, description, out);
}