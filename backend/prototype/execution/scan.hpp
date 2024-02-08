#pragma once

#include "../core/db.hpp"
#include "../storage/guard.hpp"
#include "../storage/persistence/btree.hpp"
#include "../storage/persistence/table.hpp"
#include "../utils/memcpy.hpp"
#include "pipeline_starter.hpp"
#include "paged_vector_iterator.hpp"
#include "table_column.hpp"

const size_t SCAN_MORSEL_SIZE = 32 * 1024;

// ScanBaseOperator is a CRTP base class for various specialised scans, allowing sub classes to define custom filters and projections on scanned data.
// The advantage over dynamic polymorphism with, e.g., a virtual bool filter() method, is that static polymporphism using CRTP avoids the runtime overhead of virtual method calls.
template <class Derived>
class ScanBaseOperator : public PipelineStarterBase {
public:
    ScanBaseOperator(DB& db, const std::string& table_name, const std::vector<NamedColumn>&& scan_columns, const ExecutionContext context) : db(db), scan_columns(scan_columns), iterators(context.getWorkerCount()) {
        uint64_t basepage_pid = db.getTableBasepageId(table_name, context.getWorkerId());
        SharedGuard<TableBasepage> basepage(db.vmcache, basepage_pid, context.getWorkerId());
        visibility_basepage = basepage->visibility_basepage;
        auto key_range = BTree<RowId, bool>(db.vmcache, visibility_basepage, context.getWorkerId()).keyRange();
        if (key_range.first >= key_range.second) { // table currently empty
            input_size = 1; // set input size to 1 so that we execute on at least one thread to push an empty output batch
        } else {
            input_size = key_range.second - key_range.first;
        }
        for (auto col : scan_columns) {
            auto table_col = std::dynamic_pointer_cast<TableColumnBase>(col.column);
            if (!table_col)
                throw std::runtime_error("Scan columns must be table columns!");
            basepages.push_back(basepage->column_basepages[table_col->getCid()]);
        }
        for (auto& worker_iterators : iterators)
            worker_iterators.reserve(scan_columns.size());
        value_sizes.reserve(scan_columns.size());
        for (const NamedColumn& col : scan_columns) {
            value_sizes.push_back(col.column->getValueTypeSize());
        }
    }

    virtual ~ScanBaseOperator() { }

    void execute(size_t from, size_t to, uint32_t worker_id) override {
        Derived* derived = static_cast<Derived*>(this);
        std::vector<GeneralPagedVectorIterator>& worker_iterators = iterators[worker_id];
        worker_iterators.clear();
        BTree<RowId, bool> visibility(db.vmcache, visibility_basepage, worker_id);
        auto it = visibility.lookup(from);
        auto end = visibility.lookup(to);
        // safeguard in case rows were added between constructing the scan operator and executing it
        if (to == input_size)
            end = visibility.end();
        IntermediateHelper intermediates(db.vmcache, derived->getRowSize(), next_operator, worker_id);
        if (it == end) { // empty input, abort scan
            return;
        }
        for (size_t i = 0; i < basepages.size(); i++) {
            worker_iterators.emplace_back(db.vmcache, basepages[i], (*it).first, value_sizes[i], worker_id);
        }
        for (; it != end; ++it) {
            if (!(*it).second)
                continue;
            RowId rid = (*it).first;
            for (size_t j = 0; j < basepages.size(); j++) {
                worker_iterators[j].reposition(rid);
            }
            if (derived->filter(worker_iterators)) {
                char* loc = intermediates.addRow();
                derived->project(loc, worker_iterators);
            }
            // TODO: improve with optimistic visibility scans/lookups
            for (size_t j = 0; j < basepages.size(); j++) {
                worker_iterators[j].release();
            }
        }
        worker_iterators.clear();
    }

    size_t getInputSize() const override { return input_size; }
    double getExpectedTimePerUnit() const override { return 0.02 / SCAN_MORSEL_SIZE; }
    size_t getMinMorselSize() const override { return PAGE_SIZE / sizeof(uint32_t); }

protected:
    DB& db;
    size_t input_size;
    PageId visibility_basepage;
    std::vector<PageId> basepages;
    std::vector<NamedColumn> scan_columns;
    std::vector<size_t> value_sizes;

    std::vector<std::vector<GeneralPagedVectorIterator>> iterators;
};

class ScanOperator : public ScanBaseOperator<ScanOperator> {
public:
    friend class ScanBaseOperator;

    ScanOperator(DB& db, const std::string& table_name, const std::vector<NamedColumn>&& output_columns, const ExecutionContext context) : ScanBaseOperator(db, table_name, std::move(output_columns), context) {
        row_size = 0;
        for (auto sz : value_sizes)
            row_size += sz;
    }

private:
    bool filter(std::vector<GeneralPagedVectorIterator>&) const {
        return true;
    }

    void project(char* loc, std::vector<GeneralPagedVectorIterator>& iterators) const {
        for (size_t j = 0; j < iterators.size(); ++j) {
            const size_t sz = value_sizes[j];
            const char* val_ptr = reinterpret_cast<const char*>(iterators[j].getCurrentValue());
            fast_memcpy(loc, val_ptr, sz);
            loc += sz;
        }
    }

    size_t getRowSize() const {
        return row_size;
    }

    size_t row_size;
};

class FilteringBase {
public:
    FilteringBase(const std::vector<NamedColumn>&& filter_columns, const std::vector<NamedColumn>&& scan_columns, const std::vector<Identifier>&& filter_values) : filter_values(std::move(filter_values)), full_scan_columns(std::move(scan_columns)) {
        if (filter_columns.size() != filter_values.size())
            throw std::runtime_error("Scan filter specification does not match the specified column ids!");
        num_output_columns = full_scan_columns.size();

        auto end = full_scan_columns.end();
        for (auto& filter_col : filter_columns) {
            auto it = std::find(full_scan_columns.begin(), end, filter_col);
            if (it != end) { // filter column is already in the original 'full_scan_columns'
                filter_positions.push_back(it - full_scan_columns.begin());
            } else {
                filter_positions.push_back(full_scan_columns.size());
                full_scan_columns.push_back(filter_col);
            }
        }
    }

    bool filter(std::vector<GeneralPagedVectorIterator>& iterators) const {
        for (size_t i = 0; i < filter_positions.size(); ++i) {
            auto pos = filter_positions[i];
            if (*reinterpret_cast<const Identifier*>(iterators[pos].getCurrentValue()) != filter_values[i])
                return false;
        }
        return true;
    }

    size_t num_output_columns;
    std::vector<Identifier> filter_values;
    std::vector<NamedColumn> full_scan_columns;
    std::vector<size_t> filter_positions;
};

class FilteringScanOperator : private FilteringBase, public ScanBaseOperator<FilteringScanOperator> {
public:
    friend class ScanBaseOperator;

    // TODO: support different filter data types
    FilteringScanOperator(DB& db, const std::string& table_name, const std::vector<NamedColumn>&& filter_columns, const std::vector<Identifier>&& filter_values, const std::vector<NamedColumn>&& output_columns, const ExecutionContext context) : FilteringBase(std::move(filter_columns), std::move(output_columns), std::move(filter_values)), ScanBaseOperator(db, table_name, std::move(full_scan_columns), context) {
        row_size = 0;
        for (size_t i = 0; i < num_output_columns; ++i)
            row_size += value_sizes[i];
    }

private:
    void project(char* loc, std::vector<GeneralPagedVectorIterator>& iterators) const {
        for (size_t j = 0; j < num_output_columns; ++j) {
            const size_t sz = value_sizes[j];
            const char* val_ptr = reinterpret_cast<const char*>(iterators[j].getCurrentValue());
            fast_memcpy(loc, val_ptr, sz);
            loc += sz;
        }
    }

    size_t getRowSize() const {
        return row_size;
    }

    size_t row_size;
};