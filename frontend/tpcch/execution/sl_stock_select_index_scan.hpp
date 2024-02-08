#pragma once

#include "../schema.hpp"
#include "prototype/core/db.hpp"
#include "prototype/core/types.hpp"
#include "prototype/storage/persistence/btree.hpp"
#include "prototype/storage/persistence/table.hpp"
#include "prototype/utils/memcpy.hpp"
#include "prototype/execution/index_scan.hpp"
#include "prototype/execution/paged_vector_iterator.hpp"

// Index scan on STOCK, additional filtering by QUANTITY < 'max_quantity', returns COUNT(*)
class SLStockSelectIndexScanOperator : public IndexScanOperator<2> {
public:
    SLStockSelectIndexScanOperator(DB& db, const std::vector<Identifier>& i_ids, Identifier w_id, int32_t max_quantity, const ExecutionContext context)
    : IndexScanOperator<2>(db, "STOCK", CompositeKey<2>(), CompositeKey<2>(), std::vector<NamedColumn>(), context)
    , i_ids(i_ids)
    , w_id(w_id)
    , max_quantity(max_quantity) {
        uint64_t basepage_pid = db.getTableBasepageId("STOCK", context.getWorkerId());
        TableBasepage* basepage = reinterpret_cast<TableBasepage*>(db.vmcache.fixShared(basepage_pid, context.getWorkerId()));
        quantity_basepage = basepage->column_basepages[S_QUANTITY_CID];
        db.vmcache.unfixShared(basepage_pid);
    }

    void execute(__attribute__((unused)) size_t from, __attribute__((unused)) size_t to, uint32_t worker_id) override {
        assert(from == 0);
        assert(to == 1);
        BTree<CompositeKey<2>, size_t> index(db.vmcache, index_root_page, worker_id);
        BTree<RowId, bool> visibility(db.vmcache, visibility_root_page, worker_id);
        GeneralPagedVectorIterator quantity_iterator(db.vmcache, quantity_basepage, GeneralPagedVectorIterator::UNLOAD, sizeof(int32_t), worker_id);
        IntermediateHelper intermediates(db.vmcache, sizeof(int32_t), next_operator, worker_id);
        uint32_t* const result_loc = reinterpret_cast<uint32_t*>(intermediates.addRow());
        *(result_loc) = 0;
        for (Identifier i_id : i_ids) {
            auto it = index.lookupExact(CompositeKey<2> { i_id, w_id });
            if (it != index.end()) {
                auto val = *it;
                it.release();
                // check if row is visible
                if (!visibility.lookupValue(val.second).value_or(false))
                    continue;
                // filter by 'quantity'
                quantity_iterator.reposition(val.second);
                int32_t row_quantity = *reinterpret_cast<const int32_t*>(quantity_iterator.getCurrentValue());
                quantity_iterator.release();
                if (row_quantity >= max_quantity)
                    continue;
                (*result_loc)++;
            }
        }
    }

    size_t getInputSize() const override { return 1; }
    double getExpectedTimePerUnit() const override { return 0.01; }

private:
    std::vector<Identifier> i_ids;
    Identifier w_id;
    int32_t max_quantity;
    PageId quantity_basepage;
};