#pragma once

#include "../tpcch.hpp"
#include "prototype/core/db.hpp"
#include "prototype/core/types.hpp"
#include "prototype/execution/scan.hpp"

namespace tpcch {

// scans ITEM for I_ID, filters on I_DATA
class Q09ItemScanOperator : public ScanBaseOperator<Q09ItemScanOperator> {
    friend class ScanBaseOperator;

public:
    Q09ItemScanOperator(DB& db, const ExecutionContext context) : ScanBaseOperator(db, "ITEM", std::vector<NamedColumn>({ I_ID, I_DATA }), context) { }

private:
    bool filter(std::vector<GeneralPagedVectorIterator>& iterators) {
        const char* i_data = reinterpret_cast<const char*>(iterators[1].getCurrentValue());
        size_t end = 49;
        while (i_data[end] == 0 && end > 0)
            end--;
        return i_data[end - 1] == 'B' && i_data[end] == 'B';
    }

    void project(char* loc, std::vector<GeneralPagedVectorIterator>& iterators) const {
        *reinterpret_cast<Identifier*>(loc) = *reinterpret_cast<const Identifier*>(iterators[0].getCurrentValue());
    }

    size_t getRowSize() const {
        return sizeof(Identifier);
    }
};

} // namespace tpcch