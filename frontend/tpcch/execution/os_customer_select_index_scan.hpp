#pragma once

#include "../schema.hpp"
#include "prototype/core/db.hpp"
#include "prototype/core/types.hpp"
#include "prototype/storage/persistence/btree.hpp"
#include "prototype/storage/persistence/table.hpp"
#include "prototype/utils/memcpy.hpp"
#include "prototype/execution/index_scan.hpp"
#include "prototype/execution/paged_vector_iterator.hpp"

class OSCustomerSelectIndexScanOperator : public IndexScanOperator<3> {
public:
    OSCustomerSelectIndexScanOperator(DB& db, const std::string& c_last, NamedColumn c_last_spec, CompositeKey<3> from_search_value, CompositeKey<3> to_search_value, const std::vector<uint64_t> output_column_cids, const std::vector<NamedColumn>&& output_columns, const ExecutionContext context)
    : IndexScanOperator<3>(db, "CUSTOMER", from_search_value, to_search_value, output_column_cids, std::vector<NamedColumn>(output_columns), context)
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
        auto it = index.lookup(from_search_value);
        std::vector<GeneralPagedVectorIterator> worker_iterators;
        worker_iterators.reserve(basepages.size() + 1);
        for (size_t i = 0; i < basepages.size(); i++) {
            worker_iterators.emplace_back(db.vmcache, basepages[i], 0, output_sizes[i], worker_id);
        }
        worker_iterators.emplace_back(db.vmcache, c_last_basepage, 0, c_last_spec.column->getValueTypeSize(), worker_id);
        std::shared_ptr<Batch> intermediates = std::make_shared<Batch>(db.vmcache, row_size, worker_id);
        while (it != index.end()) {
            auto val = *it;
            ++it;
            if (val.first > to_search_value)
                break;
            // filter by 'c_last'
            worker_iterators.back().reposition(val.second);
            if (memcmp(worker_iterators.back().getCurrentValue(), c_last.c_str(), std::min(c_last.size() + 1, c_last_spec.column->getValueTypeSize())) == 0) {
                continue;
            }
            for (size_t i = 0; i < basepages.size(); i++) {
                worker_iterators[i].reposition(val.second);
            }
            uint32_t row_id;
            char* loc = reinterpret_cast<char*>(intermediates->addRowIfPossible(row_id));
            if (loc == nullptr) {
                next_operator->push(intermediates, worker_id);
                if (intermediates.use_count() > 1)
                    intermediates = std::make_shared<Batch>(db.vmcache, row_size, worker_id);
                else
                    intermediates->clear();
                loc = reinterpret_cast<char*>(intermediates->addRowIfPossible(row_id));
            }
            for (size_t j = 0; j < basepages.size(); j++) {
                const size_t sz = output_sizes[j];
                const char* val_ptr = reinterpret_cast<const char*>(worker_iterators[j].getCurrentValue());
                fast_memcpy(loc, val_ptr, sz);
                loc += sz;
            }
        }
        worker_iterators.clear();
        // push the last batch
        if (intermediates->getCurrentSize() > 0)
            next_operator->push(intermediates, worker_id);
    }

    size_t getInputSize() const override { return 1; }
    size_t getMorselSizeHint() const override { return 1; }

private:
    std::string c_last;
    NamedColumn c_last_spec;
    PageId c_last_basepage;
};