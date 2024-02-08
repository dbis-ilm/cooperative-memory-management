#pragma once

#include "../schema.hpp"
#include "prototype/core/db.hpp"
#include "prototype/execution/paged_vector_iterator.hpp"
#include "prototype/execution/pipeline_starter.hpp"
#include "prototype/execution/scan.hpp"

//select
//    sum(l_extendedprice * l_discount) as revenue
//from
//    lineitem
//where
//    l_shipdate >= date '1994-01-01'
//    and l_shipdate < date '1995-01-01'
//    and l_discount between 0.06 - 0.01 and 0.06 + 0.01
//    and l_quantity < 24

// this specialised scan performs the filter operation in the query above, and returns l_extendedprice and l_discount as its result
class Q06ScanOperator : public ScanBaseOperator<Q06ScanOperator> {
public:
    friend class ScanBaseOperator;

    Q06ScanOperator(DB& db, const ExecutionContext context) : ScanBaseOperator(db, "LINEITEM", std::vector<NamedColumn>({ l_shipdate, l_discount, l_quantity, l_extendedprice }), context) {
        row_size = l_extendedprice.column->getValueTypeSize() + l_discount.column->getValueTypeSize();
    }

private:
    bool filter(std::vector<GeneralPagedVectorIterator>& iterators) const {
        const uint32_t min_date = 1 | (1 << 5) | (1994 << 9); // inclusive
        const uint32_t max_date = 1 | (1 << 5) | (1995 << 9); // exclusive
        const uint64_t min_discount = 5; // inclusive
        const uint64_t max_discount = 7; // inclusive
        const uint64_t max_quantity = 2400; // exclusive

        auto shipdate_value = *reinterpret_cast<const uint32_t*>(iterators[0].getCurrentValue());
        auto discount_value = *reinterpret_cast<const uint64_t*>(iterators[1].getCurrentValue());
        auto quantity_value = *reinterpret_cast<const uint64_t*>(iterators[2].getCurrentValue());

        return shipdate_value >= min_date && shipdate_value < max_date && discount_value >= min_discount && discount_value <= max_discount && quantity_value < max_quantity;
    }

    void project(char* loc, std::vector<GeneralPagedVectorIterator>& iterators) const {
        // l_extendedprice
        reinterpret_cast<uint64_t*>(loc)[0] = *reinterpret_cast<const uint64_t*>(iterators[3].getCurrentValue());
        // l_discount
        reinterpret_cast<uint64_t*>(loc)[1] = *reinterpret_cast<const uint64_t*>(iterators[1].getCurrentValue());
    }

    size_t getRowSize() const {
        return row_size;
    }

    size_t row_size;
};