#pragma once

#include "../schema.hpp"
#include "prototype/core/db.hpp"
#include "prototype/core/types.hpp"
#include "prototype/storage/persistence/btree.hpp"
#include "prototype/storage/persistence/table.hpp"
#include "prototype/utils/memcpy.hpp"
#include "prototype/execution/index_scan.hpp"
#include "prototype/execution/paged_vector_iterator.hpp"

// Index scan on CUSTOMER, additional filtering by C_LAST
class CustomerSelectIndexScanOperator : public IndexScanOperator<3> {
public:
    CustomerSelectIndexScanOperator(DB& db, const std::string& c_last, NamedColumn c_last_spec, CompositeKey<3> from_search_value, CompositeKey<3> to_search_value, const std::vector<NamedColumn>&& output_columns, const ExecutionContext context)
    : IndexScanOperator<3>(db, "CUSTOMER", from_search_value, to_search_value, std::vector<NamedColumn>(output_columns), context)
    , c_last(c_last)
    , c_last_spec(c_last_spec) {
        uint64_t basepage_pid = db.getTableBasepageId("CUSTOMER", context.getWorkerId());
        TableBasepage* basepage = reinterpret_cast<TableBasepage*>(db.vmcache.fixShared(basepage_pid, context.getWorkerId()));
        c_last_basepage = basepage->column_basepages[C_LAST_CID];
        db.vmcache.unfixShared(basepage_pid);
    }

    void execute(__attribute__((unused)) size_t from, __attribute__((unused)) size_t to, uint32_t worker_id) override {
        assert(from == 0);
        assert(to == 1);
        BTree<CompositeKey<3>, size_t> index(db.vmcache, index_root_page, worker_id);
        BTree<RowId, bool> visibility(db.vmcache, visibility_root_page, worker_id);
        auto it = index.lookup(from_search_value);
        std::vector<GeneralPagedVectorIterator> worker_iterators;
        worker_iterators.reserve(basepages.size() + 1);
        for (size_t i = 0; i < basepages.size(); i++) {
            worker_iterators.emplace_back(db.vmcache, basepages[i], GeneralPagedVectorIterator::UNLOAD, output_sizes[i], worker_id);
        }
        worker_iterators.emplace_back(db.vmcache, c_last_basepage, GeneralPagedVectorIterator::UNLOAD, c_last_spec.column->getValueTypeSize(), worker_id);
        IntermediateHelper intermediates(db.vmcache, row_size, next_operator, worker_id);
        while (it != index.end()) {
            auto val = *it;
            if (val.first > to_search_value)
                break;
            ++it;
            it.release();
            // check if row is visible
            if (!visibility.lookupValue(val.second).value_or(false))
                continue;
            // filter by 'c_last'
            worker_iterators.back().reposition(val.second);
            bool skip_row = memcmp(worker_iterators.back().getCurrentValue(), c_last.c_str(), std::min(c_last.size() + 1, c_last_spec.column->getValueTypeSize())) == 0;
            worker_iterators.back().release();
            if (skip_row)
                continue;
            // output result row
            for (size_t i = 0; i < basepages.size(); i++) {
                worker_iterators[i].reposition(val.second);
            }
            char* loc = intermediates.addRow();
            for (size_t j = 0; j < basepages.size(); j++) {
                const size_t sz = output_sizes[j];
                const char* val_ptr = reinterpret_cast<const char*>(worker_iterators[j].getCurrentValue());
                fast_memcpy(loc, val_ptr, sz);
                loc += sz;
                worker_iterators[j].release();
            }
        }
        worker_iterators.clear();
    }

    size_t getInputSize() const override { return 1; }
    double getExpectedTimePerUnit() const override { return 0.01; }

private:
    std::string c_last;
    NamedColumn c_last_spec;
    PageId c_last_basepage;
};