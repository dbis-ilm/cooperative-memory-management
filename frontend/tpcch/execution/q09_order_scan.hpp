#pragma once

#include "../tpcch.hpp"
#include "prototype/core/db.hpp"
#include "prototype/storage/persistence/table.hpp"
#include "prototype/utils/memcpy.hpp"
#include "prototype/execution/scan.hpp"

namespace tpcch {

// specialised scan operator for "ORDER" that extracts the year from "O_ENTRY_D"
class Q09OrderScanOperator : public ScanBaseOperator<Q09OrderScanOperator> {
    friend class ScanBaseOperator;

private:
    static const size_t O_ENTRY_D_IDX = 3;

public:
    Q09OrderScanOperator(DB& db, const ExecutionContext context) : ScanBaseOperator(db, "ORDER", std::vector<NamedColumn>({ tpcch::O_W_ID, tpcch::O_D_ID, tpcch::O_ID, tpcch::O_ENTRY_D }), context) {
        row_size = 0;
        for (size_t i = 0; i < value_sizes.size(); ++i) {
            row_size += i != O_ENTRY_D_IDX ? value_sizes[i] : sizeof(Integer);
        }
    }

private:
    bool filter(std::vector<GeneralPagedVectorIterator>&) const {
        return true;
    }

    void project(char* loc, std::vector<GeneralPagedVectorIterator>& iterators) const {
        for (size_t j = 0; j < iterators.size(); ++j) {
            const size_t sz = value_sizes[j];
            const char* val_ptr = reinterpret_cast<const char*>(iterators[j].getCurrentValue());
            if (j == O_ENTRY_D_IDX) { // extract(year from O_ENTRY_D) as L_YEAR
                *reinterpret_cast<Integer*>(loc) = reinterpret_cast<const DateTime*>(val_ptr)->getYear();
            } else {
                fast_memcpy(loc, val_ptr, sz);
            }
            loc += sz;
        }
    }

    size_t getRowSize() const {
        return row_size;
    }

    size_t row_size;
    size_t l_year_basepage_id;
};

} // namespace tpcch