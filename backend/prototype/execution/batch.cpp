#include "batch.hpp"

Batch::Batch(VMCache& vmcache, uint32_t row_size, uint32_t worker_id)
: valid_row_count(0)
, first_valid_row_id(0)
, row_size(row_size)
, current_size(0)
, max_size(PAGE_SIZE * 8 / (row_size * 8 + 1))
, worker_id(worker_id)
, vmcache(vmcache) {
    char* data_raw = vmcache.allocateTemporaryPage(worker_id);
    data = reinterpret_cast<uint8_t*>(data_raw);
    clear(); // pre-fault the page
}

Batch::~Batch() {
    vmcache.dropTemporaryPage(reinterpret_cast<char*>(data), worker_id);
}

void BatchDescription::swap(BatchDescription& other) {
    columns.swap(other.columns);
}

void BatchDescription::addColumn(const std::string& pipeline_column_name, std::shared_ptr<ColumnBase> column) {
    for (auto& column : columns) {
        if (column.name == pipeline_column_name)
            throw std::runtime_error("Pipeline column name or alias '" + pipeline_column_name + "' already exists");
    }
    columns.emplace_back(pipeline_column_name, column);
}

ColumnInfo BatchDescription::find(const std::string& pipeline_column_name) const {
    size_t offset = 0;
    for (auto& column : columns) {
        if (column.name == pipeline_column_name)
            return ColumnInfo(offset, column.column);
        offset += column.column->getValueTypeSize();
    }
    throw std::runtime_error("Pipeline column name or alias '" + pipeline_column_name + "' not found");
}

bool BatchDescription::tryFind(const std::string& column_name, ColumnInfo& dest) const {
    size_t offset = 0;
    for (auto& column : columns) {
        if (column.name == column_name) {
            dest = ColumnInfo(offset, column.column);
            return true;
        }
        offset += column.column->getValueTypeSize();
    }
    return false;
}

size_t BatchDescription::getRowSize() const {
    size_t result = 0;
    for (auto& column : columns) {
        result += column.column->getValueTypeSize();
    }
    return result;
}