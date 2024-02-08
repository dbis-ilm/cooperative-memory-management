#pragma once

#include "../schema.hpp"
#include "prototype/core/db.hpp"
#include "prototype/execution/pipeline_starter.hpp"
#include "prototype/execution/paged_vector_iterator.hpp"
#include "prototype/execution/scan.hpp"

// this specialised scan scans the PART table and performs filtering on P_SIZE < 5
class Q09PartScanOperator : public ScanBaseOperator<Q09PartScanOperator> {
private:
    std::vector<NamedColumn> extendScanColumnsByPSize(std::vector<NamedColumn>&& output_columns) {
        output_columns.push_back(p_size);
        return output_columns;
    }

public:
    friend class ScanBaseOperator;

    Q09PartScanOperator(DB& db, std::vector<NamedColumn>&& output_columns, const ExecutionContext context) : ScanBaseOperator(db, "PART", extendScanColumnsByPSize(std::move(output_columns)), context) {
        row_size = 0;
        for (size_t i = 0; i < scan_columns.size() - 1; ++i)
            row_size += scan_columns[i].column->getValueTypeSize();
    }

private:
    bool filter(std::vector<GeneralPagedVectorIterator>& iterators) const {
        return *reinterpret_cast<const Integer*>(iterators.back().getCurrentValue()) < 5;
    }

    void project(char* loc, std::vector<GeneralPagedVectorIterator>& iterators) const {
        for (size_t j = 0; j < iterators.size() - 1; ++j) {
            const size_t sz = value_sizes[j];
            const char* val_ptr = reinterpret_cast<const char*>(iterators[j].getCurrentValue());
            fast_memcpy(loc, val_ptr, sz);
            loc += sz;
        }
    }

    size_t getRowSize() const {
        return row_size;
    }

    size_t row_size;
};