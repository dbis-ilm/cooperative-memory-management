#pragma once

#include "../core/db.hpp"
#include "../core/types.hpp"
#include "../storage/guard.hpp"
#include "../storage/persistence/btree.hpp"
#include "../storage/persistence/table.hpp"
#include "../utils/memcpy.hpp"
#include "pipeline_starter.hpp"
#include "paged_vector_iterator.hpp"
#include "table_column.hpp"

// performs an index lookup on the primary key index of a table, currently only supports 32-bit indices (possibly composite)
template <size_t n_keys>
class IndexScanOperator : public PipelineStarterBase {
public:
    IndexScanOperator(DB& db, const std::string& table_name, CompositeKey<n_keys> search_value, const std::vector<NamedColumn>&& output_columns, const ExecutionContext context, size_t result_limit=0) : IndexScanOperator(
        db,
        table_name,
        search_value,
        search_value,
        std::vector<NamedColumn>(output_columns),
        context,
        result_limit
    ) { }

    IndexScanOperator(DB& db, const std::string& table_name, CompositeKey<n_keys> from_search_value, CompositeKey<n_keys> to_search_value, const std::vector<NamedColumn>&& output_columns, const ExecutionContext context, size_t result_limit = 0) : db(db), from_search_value(from_search_value), to_search_value(to_search_value), output_columns(output_columns), result_limit(result_limit) {
        uint64_t basepage_pid = db.getTableBasepageId(table_name, context.getWorkerId());
        SharedGuard<TableBasepage> basepage(db.vmcache, basepage_pid, context.getWorkerId());
        visibility_root_page = basepage->visibility_basepage;
        index_root_page = basepage->primary_key_index_basepage;
        if (index_root_page == INVALID_PAGE_ID)
            throw std::runtime_error("Table does not have a primary key index!");
        for (auto col : output_columns) {
            auto table_col = std::dynamic_pointer_cast<TableColumnBase>(col.column);
            if (!table_col)
                throw std::runtime_error("Scan output columns must be table columns!");
            basepages.push_back(basepage->column_basepages[table_col->getCid()]);
        }
        row_size = 0;
        output_sizes.reserve(output_columns.size());
        for (const NamedColumn& col : output_columns) {
            output_sizes.push_back(col.column->getValueTypeSize());
            row_size += output_sizes.back();
        }
    }

    void execute(__attribute__((unused)) size_t from, __attribute__((unused)) size_t to, uint32_t worker_id) override {
        assert(from == 0);
        assert(to == 1);
        BTree<CompositeKey<n_keys>, size_t> index(db.vmcache, index_root_page, worker_id);
        BTree<RowId, bool> visibility(db.vmcache, visibility_root_page, worker_id);
        auto it = index.lookup(from_search_value);
        std::vector<GeneralPagedVectorIterator> worker_iterators;
        worker_iterators.reserve(basepages.size());
        for (size_t i = 0; i < basepages.size(); i++) {
            worker_iterators.emplace_back(db.vmcache, basepages[i], GeneralPagedVectorIterator::UNLOAD, output_sizes[i], worker_id);
        }
        IntermediateHelper intermediates(db.vmcache, row_size, next_operator, worker_id);
        size_t num_results = 0;
        while (it != index.end()) {
            auto val = *it;
            if (val.first > to_search_value) {
                it.release();
                break;
            }
            ++it;
            // NOTE: this is inefficient, but required to avoid deadlocks with concurrent inserts (which first latch visibility exclusively and then may perform a primary key insert - if we do not release the shared latch on the primary key here, the insert operation will keep holding the visibility latch while waiting for the primary key page to become available; this causes a deadlock when we attempt to lookup visibility below)
            // TODO: can we do better? perhaps with optimistic latches?
            it.release();
            // check if row is visible
            if (!visibility.lookupValue(val.second).value_or(false))
                continue;
            // output result row
            char* loc = intermediates.addRow();
            for (size_t j = 0; j < basepages.size(); j++) {
                const size_t sz = output_sizes[j];
                worker_iterators[j].reposition(val.second);
                const char* val_ptr = reinterpret_cast<const char*>(worker_iterators[j].getCurrentValue());
                fast_memcpy(loc, val_ptr, sz);
                loc += sz;
                worker_iterators[j].release();
            }
            if (++num_results == result_limit)
                break;
        }
        worker_iterators.clear();
    }

    size_t getInputSize() const override { return 1; }
    double getExpectedTimePerUnit() const override { return 0.001; }

protected:
    DB& db;
    CompositeKey<n_keys> from_search_value;
    CompositeKey<n_keys> to_search_value; // note: inclusive
    PageId visibility_root_page;
    PageId index_root_page;
    std::vector<PageId> basepages;
    std::vector<NamedColumn> output_columns;
    std::vector<size_t> output_sizes;
    const size_t result_limit;
    size_t row_size;
};