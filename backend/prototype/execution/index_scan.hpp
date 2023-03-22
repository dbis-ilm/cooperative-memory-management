#pragma once

#include "../core/db.hpp"
#include "../core/types.hpp"
#include "../storage/persistence/btree.hpp"
#include "../storage/persistence/table.hpp"
#include "../utils/memcpy.hpp"
#include "pipeline_starter.hpp"
#include "paged_vector_iterator.hpp"

// performs an index lookup on the primary key index of a table, currently only supports 32-bit indices (possibly composite)
template <size_t n_keys>
class IndexScanOperator : public PipelineStarterBase {
public:
    IndexScanOperator(DB& db, const std::string& table_name, CompositeKey<n_keys> search_value, const std::vector<uint64_t> output_column_cids, const std::vector<NamedColumn>&& output_columns, const ExecutionContext context) : IndexScanOperator(
        db,
        table_name,
        search_value,
        search_value,
        output_column_cids,
        std::vector<NamedColumn>(output_columns),
        context
    ) { }

    IndexScanOperator(DB& db, const std::string& table_name, CompositeKey<n_keys> from_search_value, CompositeKey<n_keys> to_search_value, const std::vector<uint64_t> output_column_cids, const std::vector<NamedColumn>&& output_columns, const ExecutionContext context) : db(db), from_search_value(from_search_value), to_search_value(to_search_value), output_columns(output_columns) {
        if (output_column_cids.size() != output_columns.size())
            throw std::runtime_error("Scan output specification does not match the specified column ids!");
        uint64_t basepage_pid = db.getTableBasepageId(table_name, context.getWorkerId());
        TableBasepage* basepage = reinterpret_cast<TableBasepage*>(db.vmcache.fixShared(basepage_pid, context.getWorkerId()));
        index_root_page = basepage->primary_key_index_basepage;
        if (index_root_page == INVALID_PAGE_ID)
            throw std::runtime_error("Table does not have a primary key index!");
        for (const uint64_t cid : output_column_cids)
            basepages.push_back(basepage->column_basepages[cid]);
        db.vmcache.unfixShared(basepage_pid);
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
        auto it = index.lookup(from_search_value);
        std::vector<GeneralPagedVectorIterator> worker_iterators;
        worker_iterators.reserve(basepages.size());
        for (size_t i = 0; i < basepages.size(); i++) {
            worker_iterators.emplace_back(db.vmcache, basepages[i], 0, output_sizes[i], worker_id);
        }
        std::shared_ptr<Batch> intermediates = std::make_shared<Batch>(db.vmcache, row_size, worker_id);
        while (it != index.end()) {
            auto val = *it;
            if (val.first > to_search_value)
                break;
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
            ++it;
        }
        worker_iterators.clear();
        // push the last batch
        if (intermediates->getCurrentSize() > 0)
            next_operator->push(intermediates, worker_id);
    }

    size_t getInputSize() const override { return 1; }
    size_t getMorselSizeHint() const override { return 1; }

protected:
    DB& db;
    CompositeKey<n_keys> from_search_value;
    CompositeKey<n_keys> to_search_value; // note: inclusive
    PageId index_root_page;
    std::vector<PageId> basepages;
    std::vector<NamedColumn> output_columns;
    std::vector<size_t> output_sizes;
    size_t row_size;
};