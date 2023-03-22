#include "validation.hpp"

bool validateQueryResult(const std::shared_ptr<PipelineBreakerBase>& result, std::vector<std::shared_ptr<Batch>>& expected, bool match_order) {
    std::vector<std::shared_ptr<Batch>> batches;
    BatchDescription description;
    result->consumeBatches(batches, 0);
    result->consumeBatchDescription(description);

    // check that all row sizes match
    const uint32_t row_size = batches.empty() ? 0 : batches.front()->getRowSize();
    for (auto& batch : batches) {
        if (batch->getRowSize() != row_size) {
            std::cout << "Inconsistent row size in the result" << std::endl;
            return false;
        }
    }
    for (auto& expected_batch : expected) {
        if (expected_batch->getRowSize() != row_size) {
            std::cout << "Expected row size does not match result row size" << std::endl;
            return false;
        }
    }

    // check that tuples match
    bool match = true;
    size_t c_row_id = 0;
    if (match_order) {
        auto ex_it = expected.begin();
        size_t ex_row_id = 0;
        for (auto& batch : batches) {
            for (uint32_t row_id = 0; row_id < batch->getCurrentSize(); row_id++) {
                if (!batch->isRowValid(row_id))
                    continue;
                c_row_id++;
                void* row = batch->getRow(row_id);

                // get next valid row in 'expected'
                while (!ex_it->get()->isRowValid(ex_row_id)) {
                    ex_row_id++;
                    if (ex_row_id >= ex_it->get()->getCurrentSize()) {
                        ex_it++;
                        ex_row_id = 0;
                    }
                    if (ex_it == expected.end()) {
                        std::cout << "Result set contains more rows than expected" << std::endl;
                        std::cout << "Full result:" << std::endl;
                        printQueryResult(batches, description);
                        return false;
                    }
                }

                // check if rows match
                void* ex_row = ex_it->get()->getRow(ex_row_id);
                if (memcmp(row, ex_row, row_size) != 0) {
                    std::cout << "Did not find match for result set row " << c_row_id << ":" << std::endl;
                    printResultRow(reinterpret_cast<const char*>(row), description);
                    std::cout << "Full result:" << std::endl;
                    printQueryResult(batches, description);
                    return false;
                }
                ex_it->get()->markInvalid(ex_row_id);
            }
        }
    } else {
        for (auto& batch : batches) {
            for (uint32_t row_id = 0; row_id < batch->getCurrentSize(); row_id++) {
                if (!batch->isRowValid(row_id))
                    continue;
                c_row_id++;
                void* row = batch->getRow(row_id);
                bool found_match = false;
                for (auto& expected_batch : expected) {
                    for (uint32_t ex_row_id = 0; ex_row_id < expected_batch->getCurrentSize(); ex_row_id++) {
                        if (!expected_batch->isRowValid(ex_row_id))
                            continue;
                        void* ex_row = expected_batch->getRow(ex_row_id);
                        if (memcmp(row, ex_row, row_size) == 0) {
                            expected_batch->markInvalid(ex_row_id);
                            found_match = true;
                            break;
                        }
                    }
                    if (found_match)
                        break;
                }
                if (!found_match) {
                    std::cout << "Did not find match for result set row " << c_row_id << ":" << std::endl;
                    printResultRow(reinterpret_cast<const char*>(row), description);
                    std::cout << "Full result:" << std::endl;
                    printQueryResult(batches, description);
                    match = false;
                }
            }
        }
    }

    size_t missing_expected_rows = 0;
    for (auto& expected_batch : expected) {
        missing_expected_rows += expected_batch->getValidRowCount();
    }
    if (missing_expected_rows > 0) {
        std::cout << missing_expected_rows << " expected rows are missing from the result set:" << std::endl;
        match = false;
        for (auto& expected_batch : expected) {
            for (uint32_t ex_row_id = 0; ex_row_id < expected_batch->getCurrentSize(); ex_row_id++) {
                if (!expected_batch->isRowValid(ex_row_id))
                    continue;
                printResultRow(reinterpret_cast<const char*>(expected_batch->getRow(ex_row_id)), description);
            }
        }
    }

    return match;
}