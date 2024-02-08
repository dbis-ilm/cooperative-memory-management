#pragma once

#include "../core/db.hpp"
#include "../core/types.hpp"
#include "../storage/guard.hpp"
#include "../storage/persistence/btree.hpp"
#include "../storage/persistence/table.hpp"
#include "../utils/memcpy.hpp"
#include "pipeline_starter.hpp"
#include "paged_vector_iterator.hpp"

// performs an index lookup on the primary key index of a table and performs updates on the specified columns, outputs the updated values; currently only supports 32-bit indices (possibly composite)
template <size_t n_keys>
class IndexUpdateOperator : public PipelineStarterBase {
public:
    IndexUpdateOperator(DB& db, const std::string& table_name, CompositeKey<n_keys> search_value, const std::vector<NamedColumn>&& update_columns, const std::vector<std::function<void(void*)>> updates, const ExecutionContext context) : IndexUpdateOperator(
        db,
        table_name,
        search_value,
        search_value,
        std::vector<NamedColumn>(update_columns),
        updates,
        context
    ) { }

    IndexUpdateOperator(DB& db, const std::string& table_name, CompositeKey<n_keys> from_search_value, CompositeKey<n_keys> to_search_value, const std::vector<NamedColumn>&& update_columns, const std::vector<std::function<void(void*)>> updates, const ExecutionContext context) : db(db), from_search_value(from_search_value), to_search_value(to_search_value), update_columns(update_columns), updates(updates) {
        table_basepage_pid = db.getTableBasepageId(table_name, context.getWorkerId());
        SharedGuard<TableBasepage> basepage(db.vmcache, table_basepage_pid, context.getWorkerId());
        index_root_pid = basepage->primary_key_index_basepage;
        visibility_root_pid = basepage->visibility_basepage;
        if (index_root_pid == INVALID_PAGE_ID)
            throw std::runtime_error("Table does not have a primary key index!");
        for (auto col : update_columns) {
            auto table_col = std::dynamic_pointer_cast<TableColumnBase>(col.column);
            if (!table_col)
                throw std::runtime_error("Index update columns must be table columns!");
            column_basepage_pids.push_back(basepage->column_basepages[table_col->getCid()]);
        }
        row_size = 0;
        output_sizes.reserve(update_columns.size());
        for (const NamedColumn& col : update_columns) {
            output_sizes.push_back(col.column->getValueTypeSize());
            row_size += output_sizes.back();
        }
    }

    void execute(__attribute__((unused)) size_t from, __attribute__((unused)) size_t to, uint32_t worker_id) override {
        assert(from == 0);
        assert(to == 1);
        BTree<CompositeKey<n_keys>, size_t> index(db.vmcache, index_root_pid, worker_id);
        BTree<RowId, bool> visibility(db.vmcache, visibility_root_pid, worker_id);
        auto it = index.lookup(from_search_value);
        std::vector<GeneralPagedVectorIterator> worker_iterators;
        worker_iterators.reserve(column_basepage_pids.size());
        for (size_t i = 0; i < column_basepage_pids.size(); i++) {
            worker_iterators.emplace_back(db.vmcache, column_basepage_pids[i], GeneralPagedVectorIterator::UNLOAD, output_sizes[i], worker_id);
        }
        IntermediateHelper intermediates(db.vmcache, row_size, next_operator, worker_id);
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
            // check if row is visible and latch visibility leaf page
            auto update_guard = visibility.latchForUpdate(val.second);
            if (!update_guard.has_value() || !update_guard->prev_value)
                continue;
            char* loc = intermediates.addRow();
            for (size_t j = 0; j < column_basepage_pids.size(); j++) {
                const size_t sz = output_sizes[j];
                worker_iterators[j].reposition(val.second, true);
                void* val_ptr = worker_iterators[j].getCurrentValueForUpdate();
                // perform update
                updates[j](val_ptr);
                // copy to output
                fast_memcpy(loc, reinterpret_cast<const char*>(val_ptr), sz);
                loc += sz;
                worker_iterators[j].release();
            }
        }
        worker_iterators.clear();
    }

    size_t getInputSize() const override { return 1; }
    double getExpectedTimePerUnit() const override { return 0.001; }

protected:
    DB& db;
    CompositeKey<n_keys> from_search_value;
    CompositeKey<n_keys> to_search_value; // note: inclusive
    PageId table_basepage_pid;
    PageId index_root_pid;
    PageId visibility_root_pid;
    std::vector<PageId> column_basepage_pids;
    std::vector<NamedColumn> update_columns;
    std::vector<std::function<void(void*)>> updates;
    std::vector<size_t> output_sizes;
    size_t row_size;
};