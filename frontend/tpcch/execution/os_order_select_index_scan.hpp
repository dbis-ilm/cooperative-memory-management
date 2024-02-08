#pragma once

#include "../schema.hpp"
#include "prototype/core/db.hpp"
#include "prototype/core/types.hpp"
#include "prototype/storage/persistence/btree.hpp"
#include "prototype/storage/persistence/table.hpp"
#include "prototype/utils/memcpy.hpp"
#include "prototype/execution/index_scan.hpp"
#include "prototype/execution/paged_vector_iterator.hpp"

// Index scan on ORDER, returns one result row with maximum O_ID
class OSOrderSelectIndexScanOperator : public IndexScanOperator<4> {
public:
    OSOrderSelectIndexScanOperator(DB& db, CompositeKey<4> from_search_value, CompositeKey<4> to_search_value, const std::vector<NamedColumn>&& output_columns, const ExecutionContext context)
    : IndexScanOperator<4>(db, "ORDER", from_search_value, to_search_value, std::vector<NamedColumn>(output_columns), context) {
        uint64_t basepage_pid = db.getTableBasepageId("ORDER", context.getWorkerId());
        TableBasepage* basepage = reinterpret_cast<TableBasepage*>(db.vmcache.fixShared(basepage_pid, context.getWorkerId()));
        index_root_page = basepage->additional_index_basepage;
        db.vmcache.unfixShared(basepage_pid);
    }

    void execute(__attribute__((unused)) size_t from, __attribute__((unused)) size_t to, uint32_t worker_id) override {
        assert(from == 0);
        assert(to == 1);
        BTree<CompositeKey<4>, size_t> index(db.vmcache, index_root_page, worker_id);
        BTree<RowId, bool> visibility(db.vmcache, visibility_root_page, worker_id);
        auto it = --index.lookup(to_search_value);
        std::vector<GeneralPagedVectorIterator> worker_iterators;
        worker_iterators.reserve(basepages.size());
        for (size_t i = 0; i < basepages.size(); i++) {
            worker_iterators.emplace_back(db.vmcache, basepages[i], GeneralPagedVectorIterator::UNLOAD, output_sizes[i], worker_id);
        }
        IntermediateHelper intermediates(db.vmcache, row_size, next_operator, worker_id);
        char* const loc = intermediates.addRow();
        while (it != index.end()) {
            auto val = *it;
            if (val.first < from_search_value) {
                it.release();
                break;
            }
            --it;
            it.release();
            // check if row is visible
            if (!visibility.lookupValue(val.second).value_or(false))
                continue;
            // output result row
            for (size_t i = 0; i < basepages.size(); i++) {
                worker_iterators[i].reposition(val.second);
            }
            char* tmp_loc = loc;
            for (size_t j = 0; j < basepages.size(); j++) {
                const size_t sz = output_sizes[j];
                const char* val_ptr = reinterpret_cast<const char*>(worker_iterators[j].getCurrentValue());
                fast_memcpy(tmp_loc, val_ptr, sz);
                tmp_loc += sz;
                worker_iterators[j].release();
            }
            break;
        }
        worker_iterators.clear();
    }

    size_t getInputSize() const override { return 1; }
    double getExpectedTimePerUnit() const override { return 0.01; }
};