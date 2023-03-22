#pragma once

#include <memory>

#include "../execution/batch.hpp"

class PipelineBreakerBase;

void printResultRow(const char* row, const BatchDescription& description, std::ostream& out = std::cout);
void printQueryResult(const std::vector<std::shared_ptr<Batch>>& batches, const BatchDescription& description, std::ostream& out = std::cout);
void printQueryResult(const std::shared_ptr<PipelineBreakerBase>& breaker, uint32_t worker_id, std::ostream& out = std::cout);