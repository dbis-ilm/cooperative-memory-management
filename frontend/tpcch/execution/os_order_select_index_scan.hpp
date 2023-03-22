#pragma once

#include "../schema.hpp"
#include "prototype/core/db.hpp"
#include "prototype/core/types.hpp"
#include "prototype/storage/persistence/btree.hpp"
#include "prototype/storage/persistence/table.hpp"
#include "prototype/utils/memcpy.hpp"
#include "prototype/execution/index_scan.hpp"
#include "prototype/execution/paged_vector_iterator.hpp"

class OSOrderSelectIndexScanOperator : public IndexScanOperator<3> {
public:
    OSOrderSelectIndexScanOperator(DB& db, Identifier c_id, CompositeKey<3> from_search_value, CompositeKey<3> to_search_value, const std::vector<uint64_t> output_column_cids, const std::vector<NamedColumn>&& output_columns, const ExecutionContext context)
    : IndexScanOperator<3>(db, "ORDER", from_search_value, to_search_value, output_column_cids, std::vector<NamedColumn>(output_columns), context)
    , c_id(c_id) {
        uint64_t basepage_pid = db.getTableBasepageId("ORDER", context.getWorkerId());
        TableBasepage* basepage = reinterpret_cast<TableBasepage*>(db.vmcache.fixShared(basepage_pid, context.getWorkerId()));
        c_id_basepage = basepage->column_basepages[O_C_ID_CID];
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
        worker_iterators.emplace_back(db.vmcache, c_id_basepage, 0, sizeof(Identifier), worker_id);
        std::shared_ptr<Batch> intermediates = std::make_shared<Batch>(db.vmcache, row_size, worker_id);
        uint32_t row_id;
        char* const loc = reinterpret_cast<char*>(intermediates->addRowIfPossible(row_id));
        Identifier max_o_id = 0;
        while (it != index.end()) {
            auto val = *it;
            ++it;
            if (val.first > to_search_value)
                break;
            // filter by 'c_id'
            worker_iterators.back().reposition(val.second);
            Identifier row_c_id = *reinterpret_cast<Identifier*>(worker_iterators.back().getCurrentValue());
            if (row_c_id != c_id)
                continue;
            for (size_t i = 0; i < basepages.size(); i++) {
                worker_iterators[i].reposition(val.second);
            }
            const Identifier o_id = val.first.keys[2];
            if (o_id <= max_o_id)
                continue;
            max_o_id = o_id;
            char* tmp_loc = loc;
            for (size_t j = 0; j < basepages.size(); j++) {
                const size_t sz = output_sizes[j];
                const char* val_ptr = reinterpret_cast<const char*>(worker_iterators[j].getCurrentValue());
                fast_memcpy(tmp_loc, val_ptr, sz);
                tmp_loc += sz;
            }
        }
        worker_iterators.clear();
        assert(max_o_id != 0);
        // push the result row
        next_operator->push(intermediates, worker_id);
    }

    size_t getInputSize() const override { return 1; }
    size_t getMorselSizeHint() const override { return 1; }

private:
    Identifier c_id;
    PageId c_id_basepage;
};