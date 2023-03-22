#pragma once

#include <algorithm>
#include <cstring>
#include <iterator>
#include <memory>
#include <stdint.h>
#include <vector>

#include "../storage/vmcache.hpp"
#include "temporary_column.hpp"

struct NamedColumn {
    std::string name;
    std::shared_ptr<TemporaryColumnBase> column;

    NamedColumn(const std::string& name, std::shared_ptr<TemporaryColumnBase> column) : name(name), column(column) { }
};

struct ColumnInfo {
    size_t offset;
    std::shared_ptr<TemporaryColumnBase> column;

    ColumnInfo(size_t offset, std::shared_ptr<TemporaryColumnBase> column) : offset(offset), column(column) { }
    ColumnInfo() : offset(0), column(nullptr) { }
};

class BatchDescription {
private:
    std::vector<NamedColumn> columns;

public:
    BatchDescription() { }
    BatchDescription(std::vector<NamedColumn>&& columns) : columns(columns) { }

    void swap(BatchDescription& other);

    // adds a new column and ensures that the name is unique
    void addColumn(const std::string& pipeline_column_name, std::shared_ptr<TemporaryColumnBase> column);

    const std::vector<NamedColumn>& getColumns() const { return columns; }
    ColumnInfo find(const std::string& column_name) const;
    bool tryFind(const std::string& column_name, ColumnInfo& dest) const;
    size_t getRowSize() const;
};

struct Row {
    uint32_t size;
    void* data;

    Row(uint32_t size, void* data) : size(size), data(data) { }
};

class Batch {
public:
    struct Iterator {
        friend class Batch;
        using iterator_category = std::random_access_iterator_tag;
        using difference_type = int32_t;
        using value_type = Row;
        using pointer = void;
        using reference = Row;

        explicit Iterator(Batch& batch, uint32_t row_id) : batch(&batch), row_id(row_id) {
            // TODO: add support for non-dense batches (i.e., batches that contain values marked as invalid)
            assert(batch.dense()); // batch iterators do not currently support non-dense batches!
        }

        Iterator(const Iterator& other) // copy
        : batch(other.batch)
        , row_id(other.row_id) { }

        Iterator& operator=(const Iterator& other) { // copy
            if (this == &other)
                return *this;
            batch = other.batch;
            row_id = other.row_id;
            return *this;
        }

        reference operator*() const {
            return Row(batch->getRowSize(), batch->getRow(row_id));
        }

        friend bool operator==(const Iterator& a, const Iterator& b) { return &a.batch == &b.batch && a.row_id == b.row_id; }
        friend bool operator!=(const Iterator& a, const Iterator& b) { return &a.batch != &b.batch || a.row_id != b.row_id; }
        friend difference_type operator-(const Iterator& a, const Iterator& b) { return static_cast<difference_type>(b.row_id) - static_cast<difference_type>(a.row_id); }
        friend Iterator operator+(const Iterator& a, difference_type b) { return Iterator(*a.batch, a.row_id + b); }
        friend Iterator operator+(difference_type a, const Iterator& b) { return Iterator(*b.batch, b.row_id + a); }
        friend Iterator operator-(const Iterator& a, difference_type b) { return Iterator(*a.batch, a.row_id - b); }
        friend bool operator<(const Iterator& a, const Iterator& b) { return a.row_id < b.row_id; }
        friend bool operator<=(const Iterator& a, const Iterator& b) { return a.row_id <= b.row_id; }
        friend bool operator>(const Iterator& a, const Iterator& b) { return a.row_id > b.row_id; }
        friend bool operator>=(const Iterator& a, const Iterator& b) { return a.row_id >= b.row_id; }

        Iterator operator++(int){ Iterator copy(*this); ++*this; return copy; }
        Iterator& operator++() { row_id++; return *this; }
        Iterator operator--(int){ Iterator copy(*this); --*this; return copy; }
        Iterator& operator--() { row_id--; return *this; }

        explicit operator bool() const { return row_id < batch->getCurrentSize() && batch->isRowValid(row_id); }

    private:
        Batch* batch;
        uint32_t row_id;
    };

    // TODO: dynamic batch sizes depending on row_size, instead of always using 4 kiB?
    Batch(VMCache& vmcache, uint32_t row_size, uint32_t worker_id);
    ~Batch();

    Batch(const Batch& other) = delete;
    Batch(Batch&& other) = delete;
    Batch& operator=(const Batch& other) = delete;
    Batch& operator=(Batch&& other) = delete;

    Iterator begin() {
        if (empty())
            return end();
        return Iterator(*this, first_valid_row_id);
    }

    Iterator end() {
        return Iterator(*this, current_size);
    }

    inline void* addRowIfPossible(uint32_t& row_id) {
        if (current_size >= max_size)
            return nullptr;

        row_id = current_size;
        data[row_id / 8] |= 0x1ull << (row_id % 8);
        valid_row_count++;
        current_size++;
        return getRow(row_id);
    }

    inline bool isRowValid(const uint32_t row_id) const {
        assert(row_id < current_size);
        return ((data[row_id / 8] >> (row_id % 8)) & 0x1) > 0;
    }

    inline void* getRow(const uint32_t row_id) {
        assert(row_id < current_size);
        return data + (max_size >> 3) + row_id * row_size;
    }

    inline const void* getRow(const uint32_t row_id) const {
        assert(row_id < current_size);
        return data + (max_size >> 3) + row_id * row_size;
    }

    inline void* getLastRow() {
        return getRow(current_size - 1);
    }

    inline void markInvalid(Iterator it) {
        assert(it.batch == this);
        markInvalid(it.row_id);
    }

    inline void markInvalid(const uint32_t row_id) {
        assert(row_id < current_size);
        assert(isRowValid(row_id));
        data[row_id / 8] &= ~(0x1 << row_id % 8);
        valid_row_count--;
        // update 'first_valid_row_id'
        if (row_id == first_valid_row_id && valid_row_count != 0) {
            while (!isRowValid(first_valid_row_id))
                first_valid_row_id++;
        }
    }

    uint32_t getRowSize() const { return row_size; }
    uint32_t getCurrentSize() const { return current_size; }
    size_t getValidRowCount() const { return valid_row_count; }
    bool empty() const { return valid_row_count == 0; }
    bool dense() const { return valid_row_count + first_valid_row_id == current_size; }

    void clear() {
        std::memset(data, 0, max_size >> 3);
        valid_row_count = 0;
        first_valid_row_id = 0;
        current_size = 0;
    }

protected:
    uint32_t valid_row_count;
    uint32_t first_valid_row_id;
    uint32_t row_size; // in bytes
    uint32_t current_size; // in rows
    uint32_t max_size; // in rows
    uint32_t worker_id;
    VMCache& vmcache;
    uint8_t* data; // stores a bitvector for row validity and the raw tuple data
};