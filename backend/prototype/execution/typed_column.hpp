#pragma once

#include "../core/column_base.hpp"
#include "unencoded_column_value_printer.hpp"

template<class ValueType>
class UnencodedTypedColumn : public virtual ColumnBase {
public:
    virtual ~UnencodedTypedColumn() { }

    int cmp(const void* a, const void* b) const override;

    size_t getValueTypeSize() const override {
        return sizeof(ValueType);
    }

    std::unique_ptr<ColumnValuePrinter> print(const char* value, size_t width) const override {
        return std::make_unique<UnencodedColumnValuePrinter<ValueType>>(value, width);
    }
};