#pragma once

#include "../tpcch.hpp"
#include "prototype/core/db.hpp"
#include "prototype/execution/paged_vector_iterator.hpp"
#include "prototype/execution/pipeline_starter.hpp"
#include "prototype/execution/scan.hpp"

namespace tpcch {
/*
    select	sum(ol_amount) as revenue
    from	orderline
    where	ol_delivery_d >= '1999-01-01 00:00:00.000000'
        and ol_delivery_d < '2020-01-01 00:00:00.000000'
        and ol_quantity between 1 and 100000
*/

// this specialised scan performs the filter operation in the query above, and returns ol_amount as its result
class Q06ScanOperator : public ScanBaseOperator<Q06ScanOperator> {
public:
    friend class ScanBaseOperator;

    Q06ScanOperator(DB& db, const ExecutionContext context) : ScanBaseOperator(db, "ORDERLINE", std::vector<NamedColumn>({ OL_DELIVERY_D, OL_QUANTITY, OL_AMOUNT }), context) {
        row_size = OL_AMOUNT.column->getValueTypeSize();
    }

private:
    bool filter(std::vector<GeneralPagedVectorIterator>& iterators) const {
        const uint64_t min_date = encode_date_time(1999, 1, 1, 0, 0, 0); // inclusive 1999-01-01 00:00:00.000000
        const uint64_t max_date = encode_date_time(3000, 1, 1, 0, 0, 0); // exclusive 3000-01-01 00:00:00.000000
        // note: contrary to the query definition using a max date in year 3000 here, as the CH benchmark delivers all ORDERLINES at the current system date and we would otherwise skip all rows
        const int32_t min_quantity = 1; // inclusive
        const int32_t max_quantity = 100000; // inclusive

        auto delivery_d_value = *reinterpret_cast<const uint64_t*>(iterators[0].getCurrentValue());
        auto quantity_value = *reinterpret_cast<const int32_t*>(iterators[1].getCurrentValue());

        return delivery_d_value >= min_date && delivery_d_value < max_date && quantity_value >= min_quantity && quantity_value <= max_quantity;
    }

    void project(char* loc, std::vector<GeneralPagedVectorIterator>& iterators) const {
        // ol_amount
        reinterpret_cast<uint64_t*>(loc)[0] = *reinterpret_cast<const uint64_t*>(iterators[2].getCurrentValue());
    }

    size_t getRowSize() const {
        return row_size;
    }

    size_t row_size;
};

} // namespace tpcch