#pragma once

#include "../tpcch.hpp"
#include "prototype/core/db.hpp"
#include "prototype/core/types.hpp"
#include "prototype/execution/scan.hpp"

namespace tpcch {

// scans STOCK for S_W_ID and S_I_ID, additionally outputs S_SUPPKEY ((S_W_ID * S_I_ID) % 10000) as the first output column
class Q09StockScanOperator : public ScanBaseOperator<Q09StockScanOperator> {
    friend class ScanBaseOperator;

public:
    Q09StockScanOperator(DB& db, const ExecutionContext context) : ScanBaseOperator(db, "STOCK", std::vector<NamedColumn>({ S_W_ID, S_I_ID }), context) { }

private:
    bool filter(std::vector<GeneralPagedVectorIterator>&) const {
        return true;
    }

    void project(char* loc, std::vector<GeneralPagedVectorIterator>& iterators) const {
        Identifier w_id = *reinterpret_cast<const Identifier*>(iterators[0].getCurrentValue());
        Identifier i_id = *reinterpret_cast<const Identifier*>(iterators[1].getCurrentValue());
        *reinterpret_cast<Identifier*>(loc) = (w_id * i_id) % 10000;
        loc += sizeof(Identifier);
        *reinterpret_cast<Identifier*>(loc) = w_id;
        loc += sizeof(Identifier);
        *reinterpret_cast<Identifier*>(loc) = i_id;
    }

    size_t getRowSize() const {
        return 3 * sizeof(Identifier);
    }
};

} // namespace tpcch