#pragma once

#include <memory>

#include "../core/column_base.hpp"
#include "unencoded_column_value_printer.hpp"
#include "typed_column.hpp"

class TableColumnBase : public virtual ColumnBase {
public:
    TableColumnBase(uint64_t cid) : cid(cid) { }

    uint64_t getCid() const {
        return cid;
    }

    virtual ~TableColumnBase() { }

protected:
    uint64_t cid;
};

template<class ValueType>
class UnencodedTableColumn : public TableColumnBase, public UnencodedTypedColumn<ValueType> {
public:
    UnencodedTableColumn(uint64_t cid) : TableColumnBase(cid) { }
};